from __future__ import annotations

from unittest.mock import Mock

import bronze.amf_ingest as ingest_mod
import flows.bronze_amf_flux as flow_mod

# ============================================================================
# Fixtures helpers
# ============================================================================


def make_sample_config() -> ingest_mod.Config:
    return ingest_mod.Config(
        project_id="my-project",
        bucket_name="my-bucket",
        location="EU",
        dataset_id="bronze",
        table_id="amf",
        staging_table_id="amf_staging",
        csv_path="referentiel/boursorama_peapme_final.csv",
        gcs_prefix="amf",
        chunk_size=200,
        request_timeout=120,
        write_disposition_staging="WRITE_TRUNCATE",
    )


def make_sample_gcs_artifacts() -> ingest_mod.GcsArtifacts:
    run_context = ingest_mod.RunContext(
        run_id="2026-04-02T10-00-00Z",
        tmp_dir="/tmp/amf_ingest_xxx",
        raw_output_path="/tmp/amf_ingest_xxx/amf_raw.jsonl",
        clean_output_path="/tmp/amf_ingest_xxx/amf_clean.jsonl",
    )

    extraction = ingest_mod.ExtractionArtifacts(
        run_context=run_context,
        raw_count=12,
        clean_count=10,
    )

    return ingest_mod.GcsArtifacts(
        extraction=extraction,
        raw_uri="gs://my-bucket/amf/raw/run_id=2026-04-02T10-00-00Z/amf_raw.jsonl",
        clean_uri="gs://my-bucket/amf/clean/run_id=2026-04-02T10-00-00Z/amf_clean.jsonl",
    )


# ============================================================================
# Tasks
# ============================================================================


def test_load_config_task_calls_load_config(mocker) -> None:
    sample_config = make_sample_config()
    load_config_mock = mocker.patch.object(flow_mod, "load_config", return_value=sample_config)

    result = flow_mod.load_config_task.fn()

    load_config_mock.assert_called_once_with()
    assert result == sample_config


def test_extract_and_dump_gcs_task_calls_underlying_function(mocker) -> None:
    sample_config = make_sample_config()
    sample_gcs_artifacts = make_sample_gcs_artifacts()

    extract_and_dump_mock = mocker.patch.object(
        flow_mod,
        "extract_and_dump_gcs",
        return_value=sample_gcs_artifacts,
    )

    result = flow_mod.extract_and_dump_gcs_task.fn(sample_config)

    extract_and_dump_mock.assert_called_once_with(config=sample_config)
    assert result == sample_gcs_artifacts


def test_inject_bq_task_calls_underlying_function(mocker) -> None:
    sample_config = make_sample_config()
    sample_gcs_artifacts = make_sample_gcs_artifacts()

    inject_bq_mock = mocker.patch.object(flow_mod, "inject_bq")

    result = flow_mod.inject_bq_task.fn(
        config=sample_config,
        gcs_artifacts=sample_gcs_artifacts,
    )

    inject_bq_mock.assert_called_once_with(
        config=sample_config,
        gcs_artifacts=sample_gcs_artifacts,
    )
    assert result is None


# ============================================================================
# Flow
# ============================================================================


def test_amf_flux_flow_calls_tasks_in_order_and_logs(mocker) -> None:
    sample_config = make_sample_config()
    sample_gcs_artifacts = make_sample_gcs_artifacts()
    logger = Mock()

    get_run_logger_mock = mocker.patch.object(flow_mod, "get_run_logger", return_value=logger)
    load_config_task_mock = mocker.patch.object(
        flow_mod,
        "load_config_task",
        return_value=sample_config,
    )
    extract_and_dump_task_mock = mocker.patch.object(
        flow_mod,
        "extract_and_dump_gcs_task",
        return_value=sample_gcs_artifacts,
    )
    inject_bq_task_mock = mocker.patch.object(flow_mod, "inject_bq_task")

    flow_mod.amf_flux_flow.fn()

    get_run_logger_mock.assert_called_once_with()
    load_config_task_mock.assert_called_once_with()
    extract_and_dump_task_mock.assert_called_once_with(config=sample_config)
    inject_bq_task_mock.assert_called_once_with(
        config=sample_config,
        gcs_artifacts=sample_gcs_artifacts,
    )

    logger.info.assert_called_once()
    log_args = logger.info.call_args.args

    assert "AMF flow complete" in log_args[0]
    assert sample_gcs_artifacts.extraction.run_context.run_id == log_args[1]
    assert sample_gcs_artifacts.extraction.raw_count == log_args[2]
    assert sample_gcs_artifacts.extraction.clean_count == log_args[3]
    assert sample_gcs_artifacts.raw_uri == log_args[4]
    assert sample_gcs_artifacts.clean_uri == log_args[5]