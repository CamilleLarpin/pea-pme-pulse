"""Prefect flow — RSS pipeline orchestrator (Bronze → Silver → Gold).

One schedule, one run in the UI: Yahoo + Google News ingestion → dbt Silver → Groq Gold scoring.
Each brick is a direct subflow call — if any step fails, the parent run turns red immediately.
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

from flows.bronze_google_news_rss import google_news_rss_flow  # noqa: E402
from flows.bronze_yahoo_rss import yahoo_rss_flow  # noqa: E402
from flows.gold_sentiment import silver_gold_rss_flow  # noqa: E402

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
GCP_PROJECT = "bootcamp-project-pea-pme"


@task(name="dbt-deps")
def dbt_deps() -> None:
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "deps", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    logger.info("dbt stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt deps failed (exit {result.returncode})")


@task(name="dbt-run-silver", retries=1, retry_delay_seconds=60)
def dbt_run_silver() -> None:
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if keyfile:
        auth = f"      method: service-account\n      keyfile: {keyfile}"
    else:
        auth = "      method: oauth"

    profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
{auth}
      project: {GCP_PROJECT}
      dataset: silver
      threads: 4
      timeout_seconds: 300
      location: EU
"""
    with tempfile.TemporaryDirectory() as profiles_dir:
        (Path(profiles_dir) / "profiles.yml").write_text(profiles)
        result = subprocess.run(
            ["dbt", "run",
             "--select", "silver.rss_articles",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", profiles_dir],
            capture_output=True,
            text=True,
        )
    logger.info("dbt stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt run failed (exit {result.returncode})")


@flow(name="bronze-silver-rss")
def bronze_silver_rss_flow() -> None:
    # GCP credentials from env var — local dev only; VM uses ADC
    gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(gcp_creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name

    yahoo_rss_flow()
    google_news_rss_flow()
    dbt_deps()
    dbt_run_silver()
    silver_gold_rss_flow()


if __name__ == "__main__":
    bronze_silver_rss_flow()
