"""
Prefect flow — AMF Financial Signal Pipeline (Bronze → Silver → Gold)
======================================================================
Orchestrates the three layer flows as subflows in sequence.
"""

from __future__ import annotations

import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.bronze_amf_flux import amf_flux_flow
from flows.silver_amf import amf_financial_signal_silver_flow
from flows.utils.dbt import _dbt_deps, _log_dbt_output, _run_dbt_cmd

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


@task(name="dbt-run-financials-score", retries=1, retry_delay_seconds=30)
def dbt_run_financials_score_task(full_refresh: bool = False) -> None:
    logger = get_run_logger()
    _dbt_deps()

    extra_args = ["--full-refresh"] if full_refresh else []
    result = _run_dbt_cmd("run", "financials_score", extra_args=extra_args)
    _log_dbt_output(logger, result)

    if result.returncode != 0:
        raise RuntimeError(f"dbt run financials_score failed (code {result.returncode}).")
    logger.info("dbt run financials_score completed successfully.")


@task(name="dbt-test-financials-score", retries=0)
def dbt_test_financials_score_task() -> None:
    logger = get_run_logger()
    _dbt_deps()

    result = _run_dbt_cmd("test", "financials_score")
    _log_dbt_output(logger, result)

    if result.returncode != 0:
        logger.warning(f"dbt tests reported failures (code {result.returncode}).")
    else:
        logger.info("All dbt tests passed.")


@flow(
    name="amf-financial-signal-pipeline",
    description="Full AMF pipeline: bronze → silver → gold",
)
def amf_financial_signal_pipeline_flow(
    full_refresh: bool = False,
    run_tests: bool = True,
) -> None:
    logger = get_run_logger()
    logger.info(f"Starting AMF pipeline | full_refresh={full_refresh} | run_tests={run_tests}")

    # ── Bronze ──────────────────────────────────────────────────
    amf_flux_flow()

    # ── Silver ──────────────────────────────────────────────────
    amf_financial_signal_silver_flow(
        full_refresh=full_refresh,
        run_tests=run_tests,
    )

    # ── Gold ────────────────────────────────────────────────────
    dbt_run_financials_score_task(full_refresh=full_refresh)
    if run_tests:
        dbt_test_financials_score_task()

    logger.info("AMF pipeline completed.")


if __name__ == "__main__":
    amf_financial_signal_pipeline_flow()
