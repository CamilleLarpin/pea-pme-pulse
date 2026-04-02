"""Prefect flow — Bronze yfinance OHLCV ingestion (daily)."""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Prefect Managed runner passes GCP credentials as JSON env var — write to temp file
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_gcp_creds_json)
    _tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

from prefect import flow, get_run_logger, task


@task(name="yfinance-ohlcv-ingest", retries=2, retry_delay_seconds=60)
def yfinance_ohlcv_ingest() -> dict:
    """
    Ingère les données OHLCV depuis Yahoo Finance vers bronze.yfinance_ohlcv.

    Délègue entièrement à bronze.yahoo_ohlcv_bronze.run() qui gère :
    - Résolution ISIN → ticker Yahoo (avec ticker_overrides.json)
    - Idempotence incrémentale par date (ne fetch que les nouvelles lignes)
    - Écriture dans BigQuery en mode APPEND
    """
    from bronze.yahoo_ohlcv_bronze import run
    return run()


@flow(name="bronze-yfinance-ohlcv")
def yfinance_ohlcv_flow() -> None:
    logger = get_run_logger()
    logger.info("Démarrage flow bronze-yfinance-ohlcv")
    stats = yfinance_ohlcv_ingest()
    logger.info(
        "Flow terminé — %d succès, %d skippés, %d échecs (total %d)",
        stats["success"], stats["skipped"], stats["failed"], stats["total"],
    )


if __name__ == "__main__":
    yfinance_ohlcv_flow()
