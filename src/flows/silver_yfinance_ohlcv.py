"""Prefect flow — Silver yfinance OHLCV pipeline (daily).

Step 1 — dbt run yahoo_ohlcv_clean:
    Reads bronze.yfinance_ohlcv, deduplicates, filters invalid prices,
    and writes clean OHLCV to silver.yahoo_ohlcv_clean.

Step 2 — compute_silver:
    Reads silver.yahoo_ohlcv_clean, computes technical indicators
    (RSI_14, MACD, BB, SMA_50/200, EMA_20) and writes to silver.yahoo_ohlcv.

Designed to run after bronze-yfinance-ohlcv completes each weekday at 19h30 Paris.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# GCP credentials — same pattern as other flows
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name  # noqa: E402

from prefect import flow, get_run_logger, task  # noqa: E402

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
GCP_PROJECT = "bootcamp-project-pea-pme"


@task(name="dbt-run-yahoo-ohlcv-clean", retries=1, retry_delay_seconds=60)
def dbt_run_yahoo_ohlcv_clean() -> None:
    """
    Matérialise silver.yahoo_ohlcv_clean via dbt.

    Déduplique bronze.yfinance_ohlcv par (isin, Date), filtre les cours
    invalides et les séances incomplètes, et ajoute last_trading_date.
    """
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    cmd = [
        "dbt", "run",
        "--select", "yahoo_ohlcv_clean",
        "--project-dir", str(DBT_PROJECT_DIR),
    ]

    if keyfile:
        # Prefect managed — build a service-account profile at runtime
        profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: service-account
      project: {GCP_PROJECT}
      dataset: silver
      threads: 4
      timeout_seconds: 300
      location: EU
      keyfile: {keyfile}
"""
        with tempfile.TemporaryDirectory() as profiles_dir:
            (Path(profiles_dir) / "profiles.yml").write_text(profiles)
            result = subprocess.run(
                cmd + ["--profiles-dir", profiles_dir],
                capture_output=True,
                text=True,
            )
            logger.info("dbt stdout:\n%s", result.stdout)
            if result.returncode != 0:
                logger.error("dbt stderr:\n%s", result.stderr)
                raise RuntimeError(f"dbt run failed (exit {result.returncode})")
    else:
        # Local dev — use ~/.dbt/profiles.yml
        logger.info("GOOGLE_APPLICATION_CREDENTIALS not set — using local dbt profile")
        result = subprocess.run(cmd, capture_output=True, text=True)
        logger.info("dbt stdout:\n%s", result.stdout)
        if result.returncode != 0:
            logger.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt run failed (exit {result.returncode})")

    logger.info("dbt-run-yahoo-ohlcv-clean complete")


@task(name="yfinance-silver-compute", retries=1, retry_delay_seconds=60)
def yfinance_silver_compute() -> None:
    """
    Calcule les indicateurs techniques Silver depuis silver.yahoo_ohlcv_clean.

    Délègue à silver.compute_silver.run() qui gère :
    - Lecture de silver.yahoo_ohlcv_clean par ISIN depuis BQ
    - Calcul RSI_14, MACD, MACD_signal, BB_upper/lower, SMA_50, SMA_200, EMA_20
    - Écriture dans silver.yahoo_ohlcv (WRITE_TRUNCATE premier ISIN,
      WRITE_APPEND suivants)
    """
    from silver.compute_silver import run

    run()


@flow(name="silver-yfinance-ohlcv")
def silver_yfinance_ohlcv_flow() -> None:
    logger = get_run_logger()
    logger.info("Démarrage flow silver-yfinance-ohlcv")
    dbt_run_yahoo_ohlcv_clean()
    yfinance_silver_compute()
    logger.info("Flow silver-yfinance-ohlcv terminé")


if __name__ == "__main__":
    silver_yfinance_ohlcv_flow()
