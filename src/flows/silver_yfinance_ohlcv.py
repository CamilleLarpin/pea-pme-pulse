"""Prefect flow — Silver yfinance OHLCV pipeline (daily).

Step 1 — dbt run yahoo_ohlcv_clean:
    Reads bronze.yfinance_ohlcv, deduplicates, filters invalid prices,
    and writes clean OHLCV to silver.yahoo_ohlcv_clean.

Step 2 — compute_silver:
    Reads silver.yahoo_ohlcv_clean, computes technical indicators
    (RSI_14, MACD, BB, SMA_50/200, EMA_20) and writes to silver.yahoo_ohlcv.

Step 3 — dbt run stocks_score:
    Reads silver.yahoo_ohlcv, computes 5 technical signals and score_technique [0-10],
    and writes to gold.stocks_score.

Designed to run after bronze-yfinance-ohlcv completes each weekday at 19h30 Paris.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from prefect import flow, get_run_logger, task

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
GCP_PROJECT = "bootcamp-project-pea-pme"


@task(name="dbt-deps")
def dbt_deps() -> None:
    """Install dbt packages (dbt_packages/ is not persisted in the cloned repo)."""
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "deps", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    logger.info("dbt deps stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("dbt deps stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt deps failed (exit {result.returncode})")
    logger.info("dbt-deps complete")


@task(name="dbt-run-yahoo-ohlcv-clean", retries=1, retry_delay_seconds=60)
def dbt_run_yahoo_ohlcv_clean(full_refresh: bool = False) -> None:
    """
    Matérialise silver.yahoo_ohlcv_clean via dbt (insert_overwrite, monthly partition).

    Déduplique bronze.yfinance_ohlcv par (isin, Date) et filtre les cours invalides.
    En mode incremental, réécrit la partition du mois courant (+ mois précédent les 7
    premiers jours du mois). Passer full_refresh=True pour recréer la table complète.
    """
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    cmd = [
        "dbt",
        "run",
        "--select",
        "yahoo_ohlcv_clean",
        "--project-dir",
        str(DBT_PROJECT_DIR),
    ]
    if full_refresh:
        cmd.append("--full-refresh")

    method = "service-account" if keyfile else "oauth"
    extra = f"      keyfile: {keyfile}\n" if keyfile else ""
    profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: {method}
      project: {GCP_PROJECT}
      dataset: silver
      threads: 4
      location: EU
{extra}"""
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


@task(name="dbt-run-stocks-score", retries=1, retry_delay_seconds=60)
def dbt_run_stocks_score() -> None:
    """
    Matérialise gold.stocks_score via dbt.

    Lit silver.yahoo_ohlcv, calcule 5 signaux techniques et score_technique [0-10],
    et écrit le résultat dans gold.stocks_score (table complète, grain: isin × date).
    """
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    cmd = [
        "dbt",
        "run",
        "--select",
        "stocks_score",
        "--project-dir",
        str(DBT_PROJECT_DIR),
    ]

    method = "service-account" if keyfile else "oauth"
    extra = f"      keyfile: {keyfile}\n" if keyfile else ""
    profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: {method}
      project: {GCP_PROJECT}
      dataset: gold
      threads: 4
      location: EU
{extra}"""
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

    logger.info("dbt-run-stocks-score complete")


@flow(name="silver-yfinance-ohlcv")
def silver_yfinance_ohlcv_flow(full_refresh: bool = False) -> None:
    logger = get_run_logger()
    logger.info("Démarrage flow silver-yfinance-ohlcv (full_refresh=%s)", full_refresh)
    dbt_deps()
    dbt_run_yahoo_ohlcv_clean(full_refresh)
    yfinance_silver_compute()
    dbt_run_stocks_score()
    logger.info("Flow silver-yfinance-ohlcv terminé")


if __name__ == "__main__":
    silver_yfinance_ohlcv_flow()
