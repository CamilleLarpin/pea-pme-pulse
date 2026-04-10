"""
Prefect flow — AMF Financial Signal Silver Layer
=================================================
Steps:
  1. Run amf_financial_signal_extract.py → populates work.amf_financial_signal_staging
  2. Run dbt (amf_financial_signal) → merges staging into silver.amf_financial_signal

Environment variables required:
  GCP_PROJECT_ID, GROQ_API_KEY
  (all others have defaults — see FinancialSignalConfig)

Optional:
  FINANCIAL_SIGNAL_MAX_DOCUMENTS  — cap the number of documents processed (useful for testing)
  LLM_PROMPT_VERSION              — pin to a specific prompt version (e.g. "v5")
  AMF_ACTIVE_PROMPT_VERSION       — passed to dbt as var to filter silver by prompt version
  DBT_PROJECT_DIR                 — path to dbt project root (default: current directory)
  DBT_PROFILES_DIR                — path to dbt profiles dir (default: ~/.dbt)
  DBT_TARGET                      — dbt target (default: "prod")
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from silver.amf_financial_signal_extract import run_financial_signal_extract

# ============================================================================
# Config
# ============================================================================

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


# ============================================================================
# Tasks
# ============================================================================


@task(
    name="amf-extract-financial-signals",
    description="Run the AMF PDF extraction pipeline → work.amf_financial_signal_staging",
    retries=1,
    retry_delay_seconds=60,
)
def extract_financial_signals() -> dict[str, int]:
    """
    Calls run_financial_signal_extract() from the extraction module directly.
    Returns a summary dict with total_success and total_errors counts.
    """
    logger = get_run_logger()

    logger.info("Starting AMF financial signal extraction...")
    run_financial_signal_extract()
    logger.info("AMF financial signal extraction completed.")

    # run_financial_signal_extract logs its own summary — we return a sentinel
    return {"status": "completed"}


@task(
    name="dbt-run-amf-financial-signal",
    description="Run dbt model amf_financial_signal → merges into silver.amf_financial_signal",
    retries=1,
    retry_delay_seconds=30,
)
def dbt_run_amf_financial_signal(
    full_refresh: bool = False,
) -> None:
    logger = get_run_logger()

    active_prompt_version = os.environ.get("AMF_ACTIVE_PROMPT_VERSION")

    cmd = [
        "dbt", "run",
        "--select", "amf_financial_signal",
        "--project-dir", str(DBT_PROJECT_DIR)
    ]

    if full_refresh:
        cmd.append("--full-refresh")

    if active_prompt_version:
        cmd += ["--vars", f"{{amf_active_prompt_version: '{active_prompt_version}'}}"]

    logger.info(f"Running dbt command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    # Always log stdout/stderr for visibility in Prefect UI
    if result.stdout:
        logger.info(f"dbt stdout:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"dbt stderr:\n{result.stderr}")

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt run failed with return code {result.returncode}.\n"
            f"stderr: {result.stderr}"
        )

    logger.info("dbt run amf_financial_signal completed successfully.")


@task(
    name="dbt-test-amf-financial-signal",
    description="Run dbt tests on amf_financial_signal",
    retries=0,
)
def dbt_test_amf_financial_signal() -> None:
    logger = get_run_logger()

    cmd = [
        "dbt", "test",
        "--select", "amf_financial_signal",
        "--project-dir", str(DBT_PROJECT_DIR),
    ]

    logger.info(f"Running dbt test command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(f"dbt test stdout:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"dbt test stderr:\n{result.stderr}")

    if result.returncode != 0:
        # Tests failures are warnings, not hard failures — log but don't raise
        logger.warning(
            f"dbt tests reported failures (return code {result.returncode}). Check Prefect logs for details.",
        )
    else:
        logger.info("All dbt tests passed.")


# ============================================================================
# Flow
# ============================================================================


@flow(
    name="amf-financial-signal-silver",
    description=(
        "End-to-end AMF financial signal silver pipeline: "
        "PDF extraction → work.amf_financial_signal_staging → silver.amf_financial_signal"
    ),
)
def amf_financial_signal_silver_flow(
    full_refresh: bool = False,
    run_tests: bool = True,
) -> None:
    """
    Parameters
    ----------
    full_refresh : bool
        If True, passes --full-refresh to dbt, rebuilding the silver table from scratch.
        Useful after a prompt version bump or schema change.
    run_tests : bool
        If True, runs dbt tests after the model run. Failures are logged as warnings,
        not as flow failures, to avoid blocking downstream flows.
    """
    logger = get_run_logger()
    logger.info(
        f"Starting AMF financial signal silver flow | full_refresh={full_refresh} | run_tests={run_tests}",
    )

    # Step 1 — Extract from AMF PDFs → staging
    extract_financial_signals()

    # Step 2 — dbt merge staging → silver
    dbt_run_amf_financial_signal(full_refresh=full_refresh)

    # Step 3 (optional) — dbt tests
    if run_tests:
        dbt_test_amf_financial_signal()

    logger.info("AMF financial signal silver flow completed.")


# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":
    amf_financial_signal_silver_flow()