"""Prefect flow — yfinance pipeline orchestrator (Bronze → Silver → Gold).

Runs daily weekdays at 19h30 Paris:
  1. Bronze — yfinance OHLCV ingestion → bronze.yfinance_ohlcv
  2. dbt run yahoo_ohlcv_clean → silver.yahoo_ohlcv_clean
  3. compute_silver — technical indicators → silver.yahoo_ohlcv
  4. dbt run stocks_score → gold.stocks_score
  5. dbt run company_scores → gold.company_scores (composite score rafraîchi)
"""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

from prefect import flow, get_run_logger  # noqa: E402

from flows.bronze_yfinance_ohlcv import yfinance_ohlcv_flow  # noqa: E402
from flows.silver_yfinance_ohlcv import (  # noqa: E402
    dbt_deps,
    dbt_run_company_scores,
    dbt_run_stocks_score,
    dbt_run_yahoo_ohlcv_clean,
    yfinance_silver_compute,
)


@flow(name="yfinance-ohlcv-pipeline")
def yfinance_ohlcv_pipeline_flow(full_refresh: bool = False) -> None:
    logger = get_run_logger()
    logger.info("Démarrage yfinance-ohlcv-pipeline (full_refresh=%s)", full_refresh)
    yfinance_ohlcv_flow()
    dbt_deps()
    dbt_run_yahoo_ohlcv_clean(full_refresh)
    yfinance_silver_compute()
    dbt_run_stocks_score()
    dbt_run_company_scores()
    logger.info("yfinance-ohlcv-pipeline terminé")


if __name__ == "__main__":
    yfinance_ohlcv_pipeline_flow()
