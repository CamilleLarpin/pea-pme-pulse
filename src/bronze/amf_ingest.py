from __future__ import annotations

import csv
import json
import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================================
# Configuration
# ============================================================================

DATA_SOURCE = "amf"
BASE_URL = (
    "https://info-financiere.gouv.fr/api/explore/v2.1/catalog/"
    "datasets/flux-amf-new-prod/exports/jsonl"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; OuffoIngestor/2.0)"}

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


@dataclass(frozen=True)
class Config:
    project_id: str
    bucket_name: str
    location: str
    dataset_id: str
    table_id: str
    csv_path: str
    gcs_prefix: str
    chunk_size: int
    request_timeout: int

    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


@dataclass(frozen=True)
class RunContext:
    run_id: str
    tmp_dir: str
    raw_output_path: str
    clean_output_path: str


@dataclass(frozen=True)
class ExtractionArtifacts:
    run_context: RunContext
    raw_count: int
    clean_count: int


@dataclass(frozen=True)
class GcsArtifacts:
    extraction: ExtractionArtifacts
    raw_uri: str
    clean_uri: str


def load_config() -> Config:
    return Config(
        project_id=os.environ["GCP_PROJECT_ID"],
        bucket_name=os.environ["GCS_BUCKET_NAME"],
        location=os.environ.get("GCP_LOCATION", "EU"),
        dataset_id=os.environ.get("BQ_DATASET_ID", "bronze"),
        table_id=DATA_SOURCE,
        csv_path="referentiel/boursorama_peapme_final.csv",
        gcs_prefix=DATA_SOURCE,
        chunk_size=200,
        request_timeout=120,
    )


BQ_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("record_id", "STRING"),
    bigquery.SchemaField("societe", "STRING"),
    bigquery.SchemaField("isin", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("publication_ts", "TIMESTAMP"),
    bigquery.SchemaField("pdf_url", "STRING"),
    bigquery.SchemaField("titre", "STRING"),
    bigquery.SchemaField("sous_type", "STRING"),
    bigquery.SchemaField("type_information", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
    bigquery.SchemaField("pdf_download_status", "STRING"),
    bigquery.SchemaField("pdf_gcs_uri", "STRING"),
    bigquery.SchemaField("pdf_error_message", "STRING"),
    bigquery.SchemaField("pdf_http_status", "INT64"),
    bigquery.SchemaField("pdf_content_type", "STRING"),
    bigquery.SchemaField("pdf_file_size_bytes", "INT64"),
    bigquery.SchemaField("pdf_last_check_ts", "TIMESTAMP"),
    bigquery.SchemaField("pdf_download_run_id", "STRING"),
]

# ============================================================================
# Utilities
# ============================================================================


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


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

    try:
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return isoformat_utc(dt)
    except ValueError:
        pass

    try:
        dt = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
        return isoformat_utc(dt)
    except ValueError:
        logger.warning("Unable to parse publication date: {}", raw)
        return None


def validate_isin(value: str) -> bool:
    return bool(ISIN_REGEX.fullmatch(value.strip().upper()))


def prepare_run_context(prefix: str = f"{DATA_SOURCE}_ingest_") -> RunContext:
    run_id = utc_now().strftime("%Y-%m-%dT%H-%M-%SZ")
    tmp_dir = Path(tempfile.mkdtemp(prefix=prefix))

    return RunContext(
        run_id=run_id,
        tmp_dir=str(tmp_dir),
        raw_output_path=str(tmp_dir / f"{DATA_SOURCE}_raw.jsonl"),
        clean_output_path=str(tmp_dir / f"{DATA_SOURCE}_clean.jsonl"),
    )


# ============================================================================
# CSV / Targets
# ============================================================================


def load_targets(csv_path: str) -> dict[str, str | None]:
    """
    Retourne un mapping :
        {isin: ticker_bourso}

    CSV attendu :
        - colonne 'isin'
        - colonne 'ticker_bourso'
    """
    targets: dict[str, str | None] = {}
    invalid_count = 0

    with open(csv_path, encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = reader.fieldnames or []

        if "isin" not in fieldnames:
            raise ValueError("CSV must contain an 'isin' column.")

        if "ticker_bourso" not in fieldnames:
            raise ValueError("CSV must contain a 'ticker_bourso' column.")

        for row in reader:
            raw_isin = (row.get("isin") or "").strip().upper()
            if not raw_isin:
                continue

            if not validate_isin(raw_isin):
                invalid_count += 1
                logger.warning("Invalid ISIN ignored: {}", raw_isin)
                continue

            raw_ticker = (row.get("ticker_bourso") or "").strip()
            ticker = raw_ticker or None

            # En cas de doublon d'ISIN dans le CSV :
            # on garde le premier ticker non vide rencontré
            if raw_isin in targets:
                if targets[raw_isin] is None and ticker is not None:
                    targets[raw_isin] = ticker
                continue

            targets[raw_isin] = ticker

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
        "select": ",".join(
            [
                "recordid",
                "identificationsociete_iso_nom_soc",
                "identificationsociete_iso_cd_isi",
                "uin_dat_amf",
                "informationdeposee_inf_tit_inf",
                "sous_type_d_information",
                "type_d_information",
                "url_de_recuperation",
            ]
        ),
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
    ticker: str | None,
    run_id: str,
    ingestion_ts: str,
) -> dict[str, object]:
    return {
        "record_id": record.get("recordid"),
        "societe": record.get("identificationsociete_iso_nom_soc"),
        "isin": record.get("identificationsociete_iso_cd_isi"),
        "ticker": ticker,
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
    run_context: RunContext,
) -> ExtractionArtifacts:
    targets = load_targets(config.csv_path)
    isins = sorted(targets.keys())

    if not isins:
        raise ValueError("No valid ISIN found in the CSV.")

    logger.info("{} valid ISIN(s) to follow.", len(isins))

    session = build_requests_session()
    ingestion_ts = isoformat_utc(utc_now())
    raw_count = 0
    clean_count = 0
    invalid_json_count = 0
    duplicate_record_ids_seen: set[str] = set()

    raw_output_path = Path(run_context.raw_output_path)
    clean_output_path = Path(run_context.clean_output_path)

    with (
        open(raw_output_path, "w", encoding="utf-8") as raw_file,
        open(clean_output_path, "w", encoding="utf-8") as clean_file,
    ):
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

            try:
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

                    ticker = targets[isin]

                    clean_record = build_clean_record(
                        record,
                        ticker=ticker,
                        run_id=run_context.run_id,
                        ingestion_ts=ingestion_ts,
                    )
                    clean_file.write(json.dumps(clean_record, ensure_ascii=False) + "\n")
                    clean_count += 1
            finally:
                response.close()

    if invalid_json_count:
        logger.warning("{} invalid JSON line(s) skipped", invalid_json_count)

    logger.info("Extraction complete.")
    logger.info("- Raw records written   : {}", raw_count)
    logger.info("- Clean records written : {}", clean_count)

    return ExtractionArtifacts(
        run_context=run_context,
        raw_count=raw_count,
        clean_count=clean_count,
    )


# ============================================================================
# GCS
# ============================================================================


def ensure_bucket_exists(
    client: storage.Client,
    bucket_name: str,
    location: str,
) -> storage.Bucket:
    try:
        bucket = client.get_bucket(bucket_name)
        logger.info("Bucket already exists: {}", bucket_name)
        return bucket
    except NotFound:
        bucket = client.create_bucket(bucket_name, location=location)
        logger.info("Bucket created: {}", bucket_name)
        return bucket


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
    extraction: ExtractionArtifacts,
) -> GcsArtifacts:
    storage_client = storage.Client(project=config.project_id)

    run_context = extraction.run_context
    run_id = run_context.run_id
    raw_output_path = Path(run_context.raw_output_path)
    clean_output_path = Path(run_context.clean_output_path)

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

    return GcsArtifacts(
        extraction=extraction,
        raw_uri=raw_uri,
        clean_uri=clean_uri,
    )


def extract_and_dump_gcs(*, config: Config) -> GcsArtifacts:
    run_context = prepare_run_context()

    logger.info(
        "Starting extract+dump for source={} run_id={}",
        DATA_SOURCE,
        run_context.run_id,
    )

    try:
        extraction = extract_data(
            config=config,
            run_context=run_context,
        )

        gcs_artifacts = dump_gcs(
            config=config,
            extraction=extraction,
        )

        logger.info(
            "Extract+dump completed for run_id={} | raw_count={} | clean_count={} | clean_uri={}",
            run_context.run_id,
            extraction.raw_count,
            extraction.clean_count,
            gcs_artifacts.clean_uri,
        )

        return gcs_artifacts
    finally:
        cleanup_run_context(run_context)


# ============================================================================
# BigQuery
# ============================================================================


def ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
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
        table = client.get_table(full_table_id)
        logger.info("Table OK: {}", full_table_id)

        existing_fields = {field.name for field in table.schema}
        if "ticker" not in existing_fields:
            table.schema = list(table.schema) + [bigquery.SchemaField("ticker", "STRING")]
            client.update_table(table, ["schema"])
            logger.info("Column added to existing table: {}.ticker", full_table_id)

    except NotFound:
        table = bigquery.Table(full_table_id, schema=BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="publication_ts",
        )
        table.clustering_fields = ["isin", "record_id"]
        client.create_table(table)
        logger.info("Table created: {}", full_table_id)


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


def inject_bq(
    *,
    config: Config,
    gcs_artifacts: GcsArtifacts,
) -> None:
    client = bigquery.Client(project=config.project_id)

    ensure_dataset_exists(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        location=config.location,
    )
    ensure_table_exists(client, config.full_table_id)

    temp_table_id = f"{config.full_table_id}__temp_{utc_now().strftime('%Y%m%d%H%M%S')}"

    logger.info("Using temp table: {}", temp_table_id)

    gcs_to_bq(
        client=client,
        gcs_uri=gcs_artifacts.clean_uri,
        full_table_id=temp_table_id,
        write_disposition="WRITE_TRUNCATE",
        schema=BQ_SCHEMA,
    )

    query = f"""
    MERGE `{config.full_table_id}` AS T
    USING `{temp_table_id}` AS S
    ON T.record_id = S.record_id

    WHEN MATCHED THEN
      UPDATE SET
        T.societe = S.societe,
        T.isin = S.isin,
        T.ticker = S.ticker,
        T.publication_ts = S.publication_ts,
        T.pdf_url = S.pdf_url,
        T.titre = S.titre,
        T.sous_type = S.sous_type,
        T.type_information = S.type_information,
        T.source = S.source,
        T.run_id = S.run_id,
        T.ingestion_ts = S.ingestion_ts

    WHEN NOT MATCHED THEN
      INSERT (
        record_id,
        societe,
        isin,
        ticker,
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
        S.ticker,
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

    client.query(query).result()
    logger.info("MERGE completed")

    client.delete_table(temp_table_id, not_found_ok=True)
    logger.info("Temp table deleted: {}", temp_table_id)


# ============================================================================
# Cleanup
# ============================================================================


def cleanup_local_files(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
                logger.info("Local file removed: {}", path)
        except OSError:
            logger.exception("Failed to remove local file: {}", path)


def cleanup_run_context(run_context: RunContext) -> None:
    raw_output_path = Path(run_context.raw_output_path)
    clean_output_path = Path(run_context.clean_output_path)
    tmp_dir = Path(run_context.tmp_dir)

    cleanup_local_files(raw_output_path, clean_output_path)

    try:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
            logger.info("Temporary directory removed: {}", tmp_dir)
    except OSError:
        logger.exception("Failed to remove temporary directory: {}", tmp_dir)


# ============================================================================
# Main
# ============================================================================


def run_pipeline() -> None:
    config = load_config()
    gcs_artifacts = extract_and_dump_gcs(config=config)

    inject_bq(
        config=config,
        gcs_artifacts=gcs_artifacts,
    )

    logger.info(
        "Ingestion finished successfully for run_id={} | raw_uri={} | clean_uri={}",
        gcs_artifacts.extraction.run_context.run_id,
        gcs_artifacts.raw_uri,
        gcs_artifacts.clean_uri,
    )


if __name__ == "__main__":
    run_pipeline()