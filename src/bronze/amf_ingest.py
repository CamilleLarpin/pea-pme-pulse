from __future__ import annotations

import csv
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


# ============================================================================
# Configuration
# ============================================================================

DATA_SOURCE = "amf"
BASE_URL = (
    "https://info-financiere.gouv.fr/api/explore/v2.1/catalog/"
    "datasets/flux-amf-new-prod/exports/jsonl"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OuffoIngestor/2.0)"
}

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


@dataclass(frozen=True)
class Config:
    project_id: str
    bucket_name: str
    location: str
    dataset_id: str
    table_id: str
    staging_table_id: str
    csv_path: str
    gcs_prefix: str
    chunk_size: int
    request_timeout: int
    write_disposition_staging: str

    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    @property
    def full_staging_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.staging_table_id}"


def load_config() -> Config:
    return Config(
        project_id=os.environ["GCP_PROJECT_ID"],
        bucket_name=os.environ["GCS_BUCKET_NAME"],
        location=os.environ.get("GCP_LOCATION", "EU"),
        dataset_id=os.environ.get("BQ_DATASET_ID", "bronze"),
        table_id=DATA_SOURCE,
        staging_table_id=os.environ.get("BQ_STAGING_TABLE_ID", f"{DATA_SOURCE}_staging"),
        csv_path="referentiel/companies_draft.csv",
        gcs_prefix=DATA_SOURCE,
        chunk_size=200,
        request_timeout=120,
        write_disposition_staging="WRITE_TRUNCATE",
    )


BQ_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("record_id", "STRING"),
    bigquery.SchemaField("societe", "STRING"),
    bigquery.SchemaField("isin", "STRING"),
    bigquery.SchemaField("publication_ts", "TIMESTAMP"),
    bigquery.SchemaField("pdf_url", "STRING"),
    bigquery.SchemaField("titre", "STRING"),
    bigquery.SchemaField("sous_type", "STRING"),
    bigquery.SchemaField("type_information", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
]


# ============================================================================
# Utilities
# ============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_requests_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_api_datetime(value: object) -> str | None:
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    # Normalizing well known format to ISO 8601 UTC-friendly.
    # If value is correct, leave it as it is.
    try:
        # formats like "2025-01-31T10:15:00+00:00", "2025-01-31T10:15:00Z", etc.
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return isoformat_utc(dt)
    except ValueError:
        pass

    # Simple format "YYYY-MM-DD"
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return isoformat_utc(dt)
    except ValueError:
        logger.warning("Unable to parse publication date: {}", raw)
        return None


def validate_isin(value: str) -> bool:
    return bool(ISIN_REGEX.fullmatch(value.strip().upper()))


# ============================================================================
# CSV / Targets
# ============================================================================

def load_targets(csv_path: str) -> set[str]:
    targets: set[str] = set()
    invalid_count = 0

    with open(csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        if "isin" not in (reader.fieldnames or []):
            raise ValueError("CSV must contain an 'isin' column.")

        for row in reader:
            raw_isin = (row.get("isin") or "").strip().upper()
            if not raw_isin:
                continue

            if not validate_isin(raw_isin):
                invalid_count += 1
                logger.warning("Invalid ISIN ignored: {}", raw_isin)
                continue

            targets.add(raw_isin)

    if invalid_count:
        logger.warning("{} invalid ISIN(s) ignored from CSV", invalid_count)

    return targets


# ============================================================================
# API
# ============================================================================

def build_where_clause(isins: list[str]) -> str:
    safe_isins = []
    for isin in isins:
        if not validate_isin(isin):
            raise ValueError(f"Invalid ISIN in request chunk: {isin}")
        safe_isins.append(f"'{isin}'")

    quoted_isins = ",".join(safe_isins)
    return f"identificationsociete_iso_cd_isi IN ({quoted_isins})"


def fetch_export_jsonl(
    session: requests.Session,
    where_clause: str,
    timeout: int,
) -> requests.Response:
    params = {
        "select": ",".join([
            "recordid",
            "identificationsociete_iso_nom_soc",
            "identificationsociete_iso_cd_isi",
            "uin_dat_amf",
            "informationdeposee_inf_tit_inf",
            "sous_type_d_information",
            "type_d_information",
            "url_de_recuperation",
        ]),
        "where": where_clause,
    }

    response = session.get(
        BASE_URL,
        params=params,
        timeout=timeout,
        stream=True,
    )

    if response.status_code == 429:
        try:
            error_data = response.json()
            logger.warning("Quota reached: {}", error_data.get("error"))
            logger.warning("Reset: {}", error_data.get("reset_time"))
            logger.warning(
                "Limit: {} {}",
                error_data.get("call_limit"),
                error_data.get("limit_time_unit"),
            )
        except json.JSONDecodeError:
            logger.warning("Quota reached (429), but response is not JSON.")

    response.raise_for_status()
    return response


# ============================================================================
# Data shaping
# ============================================================================

def build_clean_record(
    record: dict[str, object],
    *,
    run_id: str,
    ingestion_ts: str,
) -> dict[str, object]:
    return {
        "record_id": record.get("recordid"),
        "societe": record.get("identificationsociete_iso_nom_soc"),
        "isin": record.get("identificationsociete_iso_cd_isi"),
        "publication_ts": parse_api_datetime(record.get("uin_dat_amf")),
        "pdf_url": record.get("url_de_recuperation"),
        "titre": record.get("informationdeposee_inf_tit_inf"),
        "sous_type": record.get("sous_type_d_information"),
        "type_information": record.get("type_d_information"),
        "source": DATA_SOURCE,
        "run_id": run_id,
        "ingestion_ts": ingestion_ts,
    }


def extract_data(
    *,
    config: Config,
    run_id: str,
    raw_output_path: Path,
    clean_output_path: Path,
) -> tuple[int, int]:
    targets = load_targets(config.csv_path)
    isins = sorted(targets)

    if not isins:
        raise ValueError("No valid ISIN found in the CSV.")

    logger.info("{} valid ISIN(s) to follow.", len(isins))

    session = build_requests_session()
    ingestion_ts = isoformat_utc(utc_now())
    raw_count = 0
    clean_count = 0
    invalid_json_count = 0
    duplicate_record_ids_seen: set[str] = set()

    with open(raw_output_path, "w", encoding="utf-8") as raw_file, \
         open(clean_output_path, "w", encoding="utf-8") as clean_file:

        for chunk_index, isin_chunk in enumerate(chunked(isins, config.chunk_size), start=1):
            where_clause = build_where_clause(isin_chunk)
            logger.info(
                "Fetching chunk {}/{} ({} ISINs)",
                chunk_index,
                (len(isins) + config.chunk_size - 1) // config.chunk_size,
                len(isin_chunk),
            )

            response = fetch_export_jsonl(
                session=session,
                where_clause=where_clause,
                timeout=config.request_timeout,
            )

            for raw_line in response.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue

                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError:
                    invalid_json_count += 1
                    logger.exception("Invalid JSON line skipped.")
                    continue

                isin = str(record.get("identificationsociete_iso_cd_isi") or "").strip().upper()
                if not isin or isin not in targets:
                    continue

                record_id = str(record.get("recordid") or "").strip()
                if record_id and record_id in duplicate_record_ids_seen:
                    logger.debug("Duplicate record_id skipped in raw stream: {}", record_id)
                    continue

                if record_id:
                    duplicate_record_ids_seen.add(record_id)

                raw_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                raw_count += 1

                clean_record = build_clean_record(
                    record,
                    run_id=run_id,
                    ingestion_ts=ingestion_ts,
                )
                clean_file.write(json.dumps(clean_record, ensure_ascii=False) + "\n")
                clean_count += 1

            response.close()

    if invalid_json_count:
        logger.warning("{} invalid JSON line(s) skipped", invalid_json_count)

    logger.info("Extraction complete.")
    logger.info("- Raw records written   : {}", raw_count)
    logger.info("- Clean records written : {}", clean_count)

    return raw_count, clean_count


# ============================================================================
# GCS
# ============================================================================

def ensure_bucket_exists(
    client: storage.Client,
    bucket_name: str,
    location: str,
) -> storage.Bucket:
    try:
        bucket = client.create_bucket(bucket_name, location=location)
        logger.info("Bucket created: {}", bucket_name)
        return bucket
    except Conflict:
        logger.info("Bucket already exists: {}", bucket_name)
        return client.bucket(bucket_name)


def upload_to_gcs(
    client: storage.Client,
    bucket_name: str,
    local_file: Path,
    destination_blob: str,
    location: str,
) -> str:
    bucket = ensure_bucket_exists(client, bucket_name, location=location)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(str(local_file))
    return f"gs://{bucket_name}/{destination_blob}"


def dump_gcs(
    *,
    config: Config,
    run_id: str,
    raw_output_path: Path,
    clean_output_path: Path,
) -> tuple[str, str]:
    storage_client = storage.Client(project=config.project_id)

    raw_blob = f"{config.gcs_prefix}/raw/run_id={run_id}/{config.gcs_prefix}_raw.jsonl"
    clean_blob = f"{config.gcs_prefix}/clean/run_id={run_id}/{config.gcs_prefix}_clean.jsonl"

    if not raw_output_path.exists():
        raise FileNotFoundError(f"Missing local raw file: {raw_output_path}")
    if not clean_output_path.exists():
        raise FileNotFoundError(f"Missing local clean file: {clean_output_path}")

    raw_uri = upload_to_gcs(
        client=storage_client,
        bucket_name=config.bucket_name,
        local_file=raw_output_path,
        destination_blob=raw_blob,
        location=config.location,
    )
    logger.info("Upload OK: {}", raw_uri)

    clean_uri = upload_to_gcs(
        client=storage_client,
        bucket_name=config.bucket_name,
        local_file=clean_output_path,
        destination_blob=clean_blob,
        location=config.location,
    )
    logger.info("Upload OK: {}", clean_uri)

    return raw_uri, clean_uri


# ============================================================================
# BigQuery
# ============================================================================

def ensure_dataset_exists(client: bigquery.Client, project_id: str, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location

    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset OK: {}.{}", project_id, dataset_id)
    except NotFound:
        client.create_dataset(dataset_ref)
        logger.info("Dataset created: {}.{}", project_id, dataset_id)


def ensure_table_exists(client: bigquery.Client, full_table_id: str) -> None:
    try:
        client.get_table(full_table_id)
        logger.info("Table OK: {}", full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="publication_ts",
        )
        table.clustering_fields = ["isin", "record_id"]
        client.create_table(table)
        logger.info("Table created: {}", full_table_id)


def truncate_staging_table(client: bigquery.Client, full_staging_table_id: str) -> None:
    query = f"TRUNCATE TABLE `{full_staging_table_id}`"
    job = client.query(query)
    job.result()
    logger.info("Staging table truncated: {}", full_staging_table_id)


def gcs_to_bq(
    client: bigquery.Client,
    gcs_uri: str,
    full_table_id: str,
    write_disposition: str,
    schema: list[bigquery.SchemaField],
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        schema=schema,
    )

    job = client.load_table_from_uri(
        gcs_uri,
        full_table_id,
        job_config=job_config,
    )
    job.result()

    logger.info("Load job completed for {} into {}", gcs_uri, full_table_id)


def merge_staging_into_target(
    client: bigquery.Client,
    staging_table_id: str,
    target_table_id: str,
) -> None:
    query = f"""
    MERGE `{target_table_id}` AS T
    USING `{staging_table_id}` AS S
    ON T.record_id = S.record_id

    WHEN NOT MATCHED THEN
      INSERT (
        record_id,
        societe,
        isin,
        publication_ts,
        pdf_url,
        titre,
        sous_type,
        type_information,
        source,
        run_id,
        ingestion_ts
      )
      VALUES (
        S.record_id,
        S.societe,
        S.isin,
        S.publication_ts,
        S.pdf_url,
        S.titre,
        S.sous_type,
        S.type_information,
        S.source,
        S.run_id,
        S.ingestion_ts
      )
    """

    job = client.query(query)
    job.result()
    logger.info("MERGE completed from {} into {}", staging_table_id, target_table_id)


def inject_bq(config: Config, clean_uri: str) -> None:
    bq_client = bigquery.Client(project=config.project_id)

    ensure_dataset_exists(bq_client, config.project_id, config.dataset_id, config.location)
    ensure_table_exists(bq_client, config.full_table_id)
    ensure_table_exists(bq_client, config.full_staging_table_id)

    truncate_staging_table(bq_client, config.full_staging_table_id)

    gcs_to_bq(
        client=bq_client,
        gcs_uri=clean_uri,
        full_table_id=config.full_staging_table_id,
        write_disposition=config.write_disposition_staging,
        schema=BQ_SCHEMA,
    )

    merge_staging_into_target(
        client=bq_client,
        staging_table_id=config.full_staging_table_id,
        target_table_id=config.full_table_id,
    )


# ============================================================================
# Main
# ============================================================================

def cleanup_local_files(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
                logger.info("Local file removed: {}", path)
        except OSError:
            logger.exception("Failed to remove local file: {}", path)


def run_pipeline() -> None:
    config = load_config()
    run_id = utc_now().strftime("%Y-%m-%dT%H-%M-%SZ")

    logger.info("Starting ingestion for source={} run_id={}", DATA_SOURCE, run_id)

    with tempfile.TemporaryDirectory(prefix="amf_ingest_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        raw_output_path = tmp_path / "amf_raw.jsonl"
        clean_output_path = tmp_path / "amf_clean.jsonl"

        extract_data(
            config=config,
            run_id=run_id,
            raw_output_path=raw_output_path,
            clean_output_path=clean_output_path,
        )

        _, clean_uri = dump_gcs(
            config=config,
            run_id=run_id,
            raw_output_path=raw_output_path,
            clean_output_path=clean_output_path,
        )

        inject_bq(config, clean_uri)

        cleanup_local_files(raw_output_path, clean_output_path)

    logger.info("Ingestion finished successfully for run_id={}", run_id)
