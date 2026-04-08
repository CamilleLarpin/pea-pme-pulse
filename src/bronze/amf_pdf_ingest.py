from __future__ import annotations

import json
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATA_SOURCE = "amf"


# ============================================================================
# Configuration
# ============================================================================


@dataclass(frozen=True)
class PdfIngestConfig:
    project_id: str
    bucket_name: str
    location: str
    dataset_id: str
    table_id: str
    staging_table_id: str
    gcs_pdf_prefix: str
    request_timeout: int
    max_documents: int | None
    write_disposition_staging: str
    max_workers: int
    batch_size: int
    log_every: int

    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    @property
    def full_staging_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.staging_table_id}"


def load_pdf_ingest_config() -> PdfIngestConfig:
    return PdfIngestConfig(
        project_id=os.environ["GCP_PROJECT_ID"],
        bucket_name=os.environ["GCS_BUCKET_NAME"],
        location=os.environ.get("GCP_LOCATION", "EU"),
        dataset_id=os.environ.get("BQ_DATASET_ID", "bronze"),
        table_id=DATA_SOURCE,
        staging_table_id=os.environ.get("BQ_PDF_STAGING_TABLE_ID", "amf_pdf_staging"),
        gcs_pdf_prefix=os.environ.get("GCS_AMF_PDF_PREFIX", "amf/pdfs"),
        request_timeout=int(os.environ.get("PDF_REQUEST_TIMEOUT", "120")),
        max_documents=(
            int(os.environ["PDF_MAX_DOCUMENTS"]) if "PDF_MAX_DOCUMENTS" in os.environ else None
        ),
        write_disposition_staging="WRITE_TRUNCATE",
        max_workers=int(os.environ.get("PDF_MAX_WORKERS", "8")),
        batch_size=int(os.environ.get("PDF_BATCH_SIZE", "200")),
        log_every=int(os.environ.get("PDF_LOG_EVERY", "25")),
    )


# ============================================================================
# Schema
# ============================================================================


PDF_STAGING_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
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


def to_iso_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return None


def chunked(items: list, size: int):
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
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "OuffoPdfIngestor/1.0"})
    return session


_thread_local = threading.local()


def get_thread_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_requests_session()
        _thread_local.session = session
    return session


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class PdfCandidate:
    record_id: str
    isin: str
    publication_ts: str | None
    pdf_url: str
    source: str
    source_run_id: str | None


@dataclass(frozen=True)
class PdfDownloadResult:
    record_id: str
    pdf_download_status: str
    pdf_gcs_uri: str | None
    pdf_error_message: str | None
    pdf_http_status: int | None
    pdf_content_type: str | None
    pdf_file_size_bytes: int | None
    pdf_last_check_ts: str
    pdf_download_run_id: str


# ============================================================================
# BigQuery helpers
# ============================================================================


def ensure_dataset_exists(
    *,
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


def ensure_pdf_staging_table_exists(
    *,
    client: bigquery.Client,
    full_table_id: str,
) -> None:
    try:
        client.get_table(full_table_id)
        logger.info("Staging table OK: {}", full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=PDF_STAGING_SCHEMA)
        client.create_table(table)
        logger.info("Staging table created: {}", full_table_id)


def fetch_pdf_candidates(
    *,
    client: bigquery.Client,
    config: PdfIngestConfig,
) -> list[PdfCandidate]:
    limit_clause = f"\n    limit {config.max_documents}" if config.max_documents is not None else ""

    query = f"""
    SELECT
      record_id,
      isin,
      publication_ts,
      pdf_url,
      source,
      run_id AS source_run_id
    FROM `{config.full_table_id}`
    WHERE pdf_url IS NOT NULL
      AND isin IS NOT NULL
      AND (
        pdf_download_status IS NULL

        OR pdf_download_status IN ('failed', 'empty_file')

        OR (
          pdf_download_status = 'http_error'
          AND (
            pdf_http_status IS NULL
            OR pdf_http_status NOT IN (404, 410)
          )
        )
      )
    ORDER BY publication_ts DESC NULLS LAST{limit_clause}
    """

    logger.info(
        "Fetching PDF candidates from {} with limit={}",
        config.full_table_id,
        config.max_documents if config.max_documents is not None else "FULL",
    )

    rows = client.query(query).result()

    candidates = [
        PdfCandidate(
            record_id=row["record_id"],
            isin=row["isin"],
            publication_ts=to_iso_or_none(row["publication_ts"]),
            pdf_url=row["pdf_url"],
            source=row["source"],
            source_run_id=row["source_run_id"],
        )
        for row in rows
    ]

    logger.info("{} PDF candidate(s) fetched", len(candidates))
    return candidates


def load_jsonl_to_bq(
    *,
    client: bigquery.Client,
    local_jsonl_path: Path,
    full_table_id: str,
    schema: list[bigquery.SchemaField],
    write_disposition: str,
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        schema=schema,
    )

    with open(local_jsonl_path, "rb") as file:
        job = client.load_table_from_file(
            file,
            full_table_id,
            job_config=job_config,
        )
        job.result()

    logger.info("Loaded staging JSONL into {}", full_table_id)


def merge_pdf_staging_into_amf(
    *,
    client: bigquery.Client,
    staging_table_id: str,
    target_table_id: str,
) -> None:
    query = f"""
    MERGE `{target_table_id}` AS T
    USING `{staging_table_id}` AS S
    ON T.record_id = S.record_id

    WHEN MATCHED THEN
      UPDATE SET
        T.pdf_download_status = S.pdf_download_status,
        T.pdf_gcs_uri = S.pdf_gcs_uri,
        T.pdf_error_message = S.pdf_error_message,
        T.pdf_http_status = S.pdf_http_status,
        T.pdf_content_type = S.pdf_content_type,
        T.pdf_file_size_bytes = S.pdf_file_size_bytes,
        T.pdf_last_check_ts = S.pdf_last_check_ts,
        T.pdf_download_run_id = S.pdf_download_run_id
    """

    job = client.query(query)
    job.result()
    logger.info("MERGE completed from {} into {}", staging_table_id, target_table_id)


# ============================================================================
# GCS
# ============================================================================


def upload_pdf_to_gcs(
    *,
    storage_client: storage.Client,
    bucket_name: str,
    local_path: Path,
    destination_blob: str,
) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(str(local_path), content_type="application/pdf")
    return f"gs://{bucket_name}/{destination_blob}"


# ============================================================================
# PDF processing
# ============================================================================


def download_pdf_to_local(
    *,
    session: requests.Session,
    pdf_url: str,
    output_path: Path,
    timeout: int,
) -> tuple[int | None, str | None, int | None]:
    response = session.get(pdf_url, timeout=timeout, stream=True)

    try:
        http_status = response.status_code
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        normalized_content_type = (content_type or "").lower()

        if "pdf" not in normalized_content_type and "octet-stream" not in normalized_content_type:
            raise ValueError(f"Unexpected content type: {content_type or 'missing'}")

        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        file_size_bytes = output_path.stat().st_size if output_path.exists() else 0
        if file_size_bytes <= 0:
            raise ValueError("Downloaded file is empty")

        return http_status, content_type, file_size_bytes

    finally:
        response.close()


def process_single_pdf(
    *,
    storage_client: storage.Client,
    config: PdfIngestConfig,
    candidate: PdfCandidate,
    pdf_download_run_id: str,
) -> PdfDownloadResult:
    checked_at = isoformat_utc(utc_now())
    session = get_thread_session()

    try:
        with tempfile.TemporaryDirectory(prefix="amf_pdf_") as tmp_dir:
            local_path = Path(tmp_dir) / f"{candidate.record_id}.pdf"
            destination_blob = f"{config.gcs_pdf_prefix}/{candidate.record_id}.pdf"

            http_status, content_type, file_size_bytes = download_pdf_to_local(
                session=session,
                pdf_url=candidate.pdf_url,
                output_path=local_path,
                timeout=config.request_timeout,
            )

            gcs_pdf_uri = upload_pdf_to_gcs(
                storage_client=storage_client,
                bucket_name=config.bucket_name,
                local_path=local_path,
                destination_blob=destination_blob,
            )

            return PdfDownloadResult(
                record_id=candidate.record_id,
                pdf_download_status="success",
                pdf_gcs_uri=gcs_pdf_uri,
                pdf_error_message=None,
                pdf_http_status=http_status,
                pdf_content_type=content_type,
                pdf_file_size_bytes=file_size_bytes,
                pdf_last_check_ts=checked_at,
                pdf_download_run_id=pdf_download_run_id,
            )

    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        return PdfDownloadResult(
            record_id=candidate.record_id,
            pdf_download_status="http_error",
            pdf_gcs_uri=None,
            pdf_error_message=str(exc),
            pdf_http_status=response.status_code if response is not None else None,
            pdf_content_type=response.headers.get("Content-Type") if response is not None else None,
            pdf_file_size_bytes=None,
            pdf_last_check_ts=checked_at,
            pdf_download_run_id=pdf_download_run_id,
        )

    except ValueError as exc:
        status = "invalid_content_type"
        if "empty" in str(exc).lower():
            status = "empty_file"

        return PdfDownloadResult(
            record_id=candidate.record_id,
            pdf_download_status=status,
            pdf_gcs_uri=None,
            pdf_error_message=str(exc),
            pdf_http_status=None,
            pdf_content_type=None,
            pdf_file_size_bytes=None,
            pdf_last_check_ts=checked_at,
            pdf_download_run_id=pdf_download_run_id,
        )

    except Exception as exc:
        return PdfDownloadResult(
            record_id=candidate.record_id,
            pdf_download_status="failed",
            pdf_gcs_uri=None,
            pdf_error_message=str(exc),
            pdf_http_status=None,
            pdf_content_type=None,
            pdf_file_size_bytes=None,
            pdf_last_check_ts=checked_at,
            pdf_download_run_id=pdf_download_run_id,
        )


def process_pdf_batch_concurrently(
    *,
    storage_client: storage.Client,
    config: PdfIngestConfig,
    candidates: list[PdfCandidate],
    pdf_download_run_id: str,
    batch_index: int,
    total_batches: int,
    total_candidates: int,
    processed_before_batch: int,
) -> list[PdfDownloadResult]:
    if not candidates:
        return []

    logger.info(
        "Starting batch {}/{} | batch_size={} | processed_before_batch={}/{} | max_workers={}",
        batch_index,
        total_batches,
        len(candidates),
        processed_before_batch,
        total_candidates,
        config.max_workers,
    )

    results: list[PdfDownloadResult] = []
    completed_in_batch = 0

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {
            executor.submit(
                process_single_pdf,
                storage_client=storage_client,
                config=config,
                candidate=candidate,
                pdf_download_run_id=pdf_download_run_id,
            ): candidate.record_id
            for candidate in candidates
        }

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed_in_batch += 1

            logger.info(
                "record_id={} status={} gcs_pdf_uri={}",
                result.record_id,
                result.pdf_download_status,
                result.pdf_gcs_uri,
            )

            if completed_in_batch % config.log_every == 0 or completed_in_batch == len(candidates):
                success_so_far = sum(r.pdf_download_status == "success" for r in results)
                logger.info(
                    (
                        "Batch progress {}/{} | completed_in_batch={}/{} | "
                        "global_progress={}/{} | success_in_batch={}"
                    ),
                    batch_index,
                    total_batches,
                    completed_in_batch,
                    len(candidates),
                    processed_before_batch + completed_in_batch,
                    total_candidates,
                    success_so_far,
                )

    return results


# ============================================================================
# Local staging file
# ============================================================================


def write_pdf_results_jsonl(
    *,
    results: list[PdfDownloadResult],
    output_path: Path,
) -> None:
    with open(output_path, "w", encoding="utf-8") as file:
        for result in results:
            file.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")

    logger.info("Wrote {} PDF result row(s) to {}", len(results), output_path)


# ============================================================================
# Batch persist
# ============================================================================


def persist_batch_results(
    *,
    client: bigquery.Client,
    config: PdfIngestConfig,
    results: list[PdfDownloadResult],
) -> None:
    if not results:
        logger.info("No batch results to persist.")
        return

    with tempfile.TemporaryDirectory(prefix="amf_pdf_results_") as tmp_dir:
        staging_jsonl_path = Path(tmp_dir) / "amf_pdf_results.jsonl"

        write_pdf_results_jsonl(
            results=results,
            output_path=staging_jsonl_path,
        )

        load_jsonl_to_bq(
            client=client,
            local_jsonl_path=staging_jsonl_path,
            full_table_id=config.full_staging_table_id,
            schema=PDF_STAGING_SCHEMA,
            write_disposition=config.write_disposition_staging,
        )

    merge_pdf_staging_into_amf(
        client=client,
        staging_table_id=config.full_staging_table_id,
        target_table_id=config.full_table_id,
    )


# ============================================================================
# Main
# ============================================================================


def run_pdf_ingestion(*, config: PdfIngestConfig | None = None) -> None:
    config = config or load_pdf_ingest_config()
    pdf_download_run_id = utc_now().strftime("%Y-%m-%dT%H-%M-%SZ")

    logger.info(
        "Starting AMF PDF ingestion | run_id={} | limit={} | max_workers={} | batch_size={}",
        pdf_download_run_id,
        config.max_documents if config.max_documents is not None else "FULL",
        config.max_workers,
        config.batch_size,
    )

    bq_client = bigquery.Client(project=config.project_id)
    storage_client = storage.Client(project=config.project_id)

    ensure_dataset_exists(
        client=bq_client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        location=config.location,
    )
    ensure_pdf_staging_table_exists(
        client=bq_client,
        full_table_id=config.full_staging_table_id,
    )

    candidates = fetch_pdf_candidates(
        client=bq_client,
        config=config,
    )

    if not candidates:
        logger.info("No PDF candidates to process.")
        return

    candidate_batches = list(chunked(candidates, config.batch_size))
    total_batches = len(candidate_batches)
    total_candidates = len(candidates)

    total_results = 0
    total_success = 0
    total_errors = 0

    for batch_index, batch_candidates in enumerate(candidate_batches, start=1):
        processed_before_batch = total_results

        batch_results = process_pdf_batch_concurrently(
            storage_client=storage_client,
            config=config,
            candidates=batch_candidates,
            pdf_download_run_id=pdf_download_run_id,
            batch_index=batch_index,
            total_batches=total_batches,
            total_candidates=total_candidates,
            processed_before_batch=processed_before_batch,
        )

        persist_batch_results(
            client=bq_client,
            config=config,
            results=batch_results,
        )

        batch_success = sum(result.pdf_download_status == "success" for result in batch_results)
        batch_errors = len(batch_results) - batch_success

        total_results += len(batch_results)
        total_success += batch_success
        total_errors += batch_errors

        logger.info(
            (
                "Batch persisted {}/{} | batch_total={} | batch_success={} | batch_errors={} | "
                "global_total={} | global_success={} | global_errors={}"
            ),
            batch_index,
            total_batches,
            len(batch_results),
            batch_success,
            batch_errors,
            total_results,
            total_success,
            total_errors,
        )

    logger.info(
        "AMF PDF ingestion finished | run_id={} | total={} | success={} | errors={}",
        pdf_download_run_id,
        total_results,
        total_success,
        total_errors,
    )


if __name__ == "__main__":
    run_pdf_ingestion()
