from __future__ import annotations

import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from bronze.amf_ingest import (
    Config,
    GcsArtifacts,
    extract_and_dump_gcs,
    inject_bq,
    load_config,
)
from bronze.amf_pdf_ingest import run_pdf_ingestion


@task(name="amf-load-config", retries=0)
def load_config_task() -> Config:
    return load_config()


@task(name="amf-extract-and-dump-gcs", retries=2, retry_delay_seconds=30)
def extract_and_dump_gcs_task(config: Config) -> GcsArtifacts:
    return extract_and_dump_gcs(config=config)


@task(name="amf-inject-bq", retries=2, retry_delay_seconds=60)
def inject_bq_task(
    config: Config,
    gcs_artifacts: GcsArtifacts,
) -> None:
    inject_bq(
        config=config,
        gcs_artifacts=gcs_artifacts,
    )


@task(name="amf-pdf-ingest", retries=2, retry_delay_seconds=60)
def amf_pdf_ingest_task() -> None:
    run_pdf_ingestion()


@flow(name="bronze-amf-flux", log_prints=True)
def amf_flux_flow() -> None:
    logger = get_run_logger()

    config = load_config_task()

    gcs_artifacts = extract_and_dump_gcs_task(config=config)

    inject_bq_task(
        config=config,
        gcs_artifacts=gcs_artifacts,
    )

    amf_pdf_ingest_task()

    logger.info(
        (
            "AMF bronze flow complete | run_id=%s | raw_count=%d | clean_count=%d "
            "| raw_uri=%s | clean_uri=%s | pdf_ingestion=done"
        ),
        gcs_artifacts.extraction.run_context.run_id,
        gcs_artifacts.extraction.raw_count,
        gcs_artifacts.extraction.clean_count,
        gcs_artifacts.raw_uri,
        gcs_artifacts.clean_uri,
    )


if __name__ == "__main__":
    amf_flux_flow()
