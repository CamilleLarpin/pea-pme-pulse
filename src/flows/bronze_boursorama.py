"""Prefect flow — Bronze Boursorama PEA-PME company listing ingestion.

Scrapes the full Boursorama PEA-PME listing, enriches each row with its ISIN,
then uploads the result as a CSV to GCS. The BQ external table bronze.boursorama
points to that GCS path and auto-reflects the new data on next query.

Schedule: monthly (1st of month, 08:00 Paris) — referentiel changes rarely.
"""

import os
import sys
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name


@task(name="boursorama-scrape", retries=2, retry_delay_seconds=60)
def boursorama_scrape() -> list[dict]:
    from bronze.boursorama import fetch_companies

    return fetch_companies()


@task(name="boursorama-upload-gcs", retries=2, retry_delay_seconds=30)
def boursorama_upload_gcs(rows: list[dict]) -> str:
    from bronze.boursorama import upload_to_gcs

    return upload_to_gcs(rows)


@flow(name="bronze-boursorama")
def bronze_boursorama_flow() -> list[dict]:
    logger = get_run_logger()
    rows = boursorama_scrape()
    uri = boursorama_upload_gcs(rows)
    matched = sum(1 for r in rows if r.get("isin"))
    logger.info(
        "bronze-boursorama complete — %d companies scraped, %d with ISIN, uploaded to %s",
        len(rows),
        matched,
        uri,
    )
    return rows


if __name__ == "__main__":
    bronze_boursorama_flow()
