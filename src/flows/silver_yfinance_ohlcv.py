"""Prefect flow — Silver yfinance OHLCV indicators computation (daily).

Reads bronze.yfinance_ohlcv from BigQuery, computes technical indicators
(RSI_14, MACD, BB, SMA_50/200, EMA_20) via compute_silver.run(), and writes
results to yfinance_silver.yahoo_ohlcv.

Designed to run after bronze-yfinance-ohlcv completes each weekday morning.
"""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# GCP credentials — same pattern as Bronze flows
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name  # noqa: E402

from prefect import flow, get_run_logger, task  # noqa: E402


@task(name="yfinance-silver-compute", retries=1, retry_delay_seconds=60)
def yfinance_silver_compute() -> None:
    """
    Calcule les indicateurs techniques Silver depuis bronze.yfinance_ohlcv.

    Délègue entièrement à silver.compute_silver.run() qui gère :
    - Lecture de bronze.yfinance_ohlcv par ISIN depuis BQ
    - Calcul RSI_14, MACD, MACD_signal, BB_upper/lower, SMA_50, SMA_200, EMA_20
    - Écriture dans yfinance_silver.yahoo_ohlcv (WRITE_TRUNCATE premier ISIN,
      WRITE_APPEND suivants)
    """
    from silver.compute_silver import run

    run()


@flow(name="silver-yfinance-ohlcv")
def silver_yfinance_ohlcv_flow() -> None:
    logger = get_run_logger()
    logger.info("Démarrage flow silver-yfinance-ohlcv")
    yfinance_silver_compute()
    logger.info("Flow silver-yfinance-ohlcv terminé")


if __name__ == "__main__":
    silver_yfinance_ohlcv_flow()
