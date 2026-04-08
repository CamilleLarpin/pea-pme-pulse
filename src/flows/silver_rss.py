"""Prefect flow — Bronze RSS → Silver dbt orchestration.

Runs both Bronze RSS flows as subflows, then triggers dbt Silver refresh.
Silver only runs if both Bronze flows succeed.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Must be before project imports — ensures src/ is on sys.path in the Prefect
# managed environment where pip editable-install .pth files aren't picked up
# by an already-running Python process.
sys.path.insert(0, str(Path(__file__).parent.parent))

from prefect import flow, get_run_logger, task  # noqa: E402
from prefect.deployments import run_deployment  # noqa: E402

from flows.bronze_google_news_rss import google_news_rss_flow  # noqa: E402
from flows.bronze_yahoo_rss import yahoo_rss_flow  # noqa: E402

# GCP credentials — same pattern as Bronze flows; Bronze imports below are no-ops if already set
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
GCP_PROJECT = "bootcamp-project-pea-pme"


@task(name="dbt-run-silver", retries=1, retry_delay_seconds=60)
def dbt_run_silver() -> None:
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    cmd = [
        "dbt",
        "run",
        "--select",
        "silver.rss_articles",
        "--project-dir",
        str(DBT_PROJECT_DIR),
    ]

    if keyfile:
        method = "service-account"
        extra = f"      keyfile: {keyfile}\n"
    else:
        method = "oauth"
        extra = ""

    profiles = f"""pea_pme_pulse:
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

    logger.info("dbt-run-silver complete")


@flow(name="bronze-silver-rss")
def bronze_silver_rss_flow() -> None:
    logger = get_run_logger()
    yahoo_rss_flow()
    google_news_rss_flow()
    dbt_run_silver()
    logger.info("bronze-silver-rss complete — triggering silver-gold-rss")
    run_deployment("silver-gold-rss/silver-gold-rss", timeout=0)
    logger.info("silver-gold-rss triggered")


if __name__ == "__main__":
    bronze_silver_rss_flow()
