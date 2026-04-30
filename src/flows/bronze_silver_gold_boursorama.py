"""Prefect flow — Boursorama pipeline orchestrator (Bronze → Silver → Gold).

Runs monthly:
  1. Scrape Boursorama PEA-PME listing → upload CSV to GCS (bronze.boursorama refreshes)
  2. dbt run silver.companies — incremental snapshot
  3. dbt run gold.companies — enriched company list with current news score
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).parent.parent))

_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

from flows.bronze_boursorama import bronze_boursorama_flow  # noqa: E402

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
GCP_PROJECT = "arched-run-488313-h2"


def _build_profiles(method: str, keyfile: str | None) -> str:
    extra = f"      keyfile: {keyfile}\n" if keyfile else ""
    return f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: {method}
      project: {GCP_PROJECT}
      dataset: silver
      threads: 4
      timeout_seconds: 300
      location: EU
{extra}"""


def _run_dbt(select: str) -> None:
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    method = "service-account" if keyfile else "oauth"
    profiles = _build_profiles(method, keyfile)

    cmd = [
        "dbt",
        "run",
        "--select",
        select,
        "--project-dir",
        str(DBT_PROJECT_DIR),
    ]

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


@task(name="dbt-run-silver-companies", retries=1, retry_delay_seconds=60)
def dbt_run_silver_companies() -> None:
    _run_dbt("silver.companies")
    get_run_logger().info("dbt-run-silver-companies complete")


@task(name="dbt-run-gold-company-scores", retries=1, retry_delay_seconds=60)
def dbt_run_gold_companies() -> None:
    _run_dbt("gold.company_scores")
    get_run_logger().info("dbt-run-gold-companies complete")


@flow(name="bronze-silver-gold-boursorama")
def bronze_silver_gold_boursorama_flow() -> None:
    logger = get_run_logger()
    rows = bronze_boursorama_flow()
    dbt_run_silver_companies()
    dbt_run_gold_companies()
    logger.info(
        "bronze-silver-gold-boursorama complete — %d companies processed",
        len(rows),
    )


if __name__ == "__main__":
    bronze_silver_gold_boursorama_flow()
