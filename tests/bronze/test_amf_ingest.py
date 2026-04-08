from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

import bronze.amf_ingest as mod

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_config() -> mod.Config:
    return mod.Config(
        project_id="my-project",
        bucket_name="my-bucket",
        location="EU",
        dataset_id="bronze",
        table_id="amf",
        csv_path="referentiel/boursorama_peapme_final.csv",
        gcs_prefix="amf",
        chunk_size=2,
        request_timeout=120,
    )


@pytest.fixture
def sample_run_context(tmp_path: Path) -> mod.RunContext:
    return mod.RunContext(
        run_id="2026-04-02T10-00-00Z",
        tmp_dir=str(tmp_path),
        raw_output_path=str(tmp_path / "amf_raw.jsonl"),
        clean_output_path=str(tmp_path / "amf_clean.jsonl"),
    )


@pytest.fixture
def sample_extraction_artifacts(sample_run_context: mod.RunContext) -> mod.ExtractionArtifacts:
    return mod.ExtractionArtifacts(
        run_context=sample_run_context,
        raw_count=12,
        clean_count=10,
    )


@pytest.fixture
def sample_gcs_artifacts(
    sample_extraction_artifacts: mod.ExtractionArtifacts,
) -> mod.GcsArtifacts:
    return mod.GcsArtifacts(
        extraction=sample_extraction_artifacts,
        raw_uri="gs://my-bucket/amf/raw/run_id=2026-04-02T10-00-00Z/amf_raw.jsonl",
        clean_uri="gs://my-bucket/amf/clean/run_id=2026-04-02T10-00-00Z/amf_clean.jsonl",
    )


# ============================================================================
# validate_isin
# ============================================================================


@pytest.mark.parametrize(
    "isin, expected",
    [
        ("FR0000120271", True),
        ("US0378331005", True),
        ("GB0002634946", True),
        ("fr0000120271", True),
        ("", False),
        ("123", False),
        ("FR000012027", False),
        ("FR00001202711", False),
        ("FR00001202$1", False),
    ],
)
def test_validate_isin(isin: str, expected: bool) -> None:
    assert mod.validate_isin(isin.upper()) is expected


# ============================================================================
# chunked
# ============================================================================


def test_chunked_splits_list_correctly() -> None:
    items = ["a", "b", "c", "d", "e"]

    chunks = list(mod.chunked(items, 2))

    assert chunks == [["a", "b"], ["c", "d"], ["e"]]


def test_chunked_empty_list() -> None:
    assert list(mod.chunked([], 3)) == []


# ============================================================================
# parse_api_datetime
# ============================================================================


@pytest.mark.parametrize(
    "raw_value, expected",
    [
        ("2025-01-31", "2025-01-31T00:00:00+00:00"),
        ("2025-01-31T10:15:00+00:00", "2025-01-31T10:15:00+00:00"),
        ("2025-01-31T10:15:00Z", "2025-01-31T10:15:00+00:00"),
        (None, None),
        ("", None),
        ("   ", None),
    ],
)
def test_parse_api_datetime_valid_cases(raw_value, expected) -> None:
    assert mod.parse_api_datetime(raw_value) == expected


def test_parse_api_datetime_invalid_returns_none() -> None:
    assert mod.parse_api_datetime("not-a-date") is None


# ============================================================================
# prepare_run_context
# ============================================================================


def test_prepare_run_context_creates_expected_paths() -> None:
    result = mod.prepare_run_context()

    assert isinstance(result, mod.RunContext)
    assert result.run_id
    assert Path(result.tmp_dir).exists()
    assert result.raw_output_path.endswith("amf_raw.jsonl")
    assert result.clean_output_path.endswith("amf_clean.jsonl")

    mod.cleanup_run_context(result)


# ============================================================================
# build_where_clause
# ============================================================================


def test_build_where_clause_ok() -> None:
    isins = ["FR0000120271", "US0378331005"]

    result = mod.build_where_clause(isins)

    assert result == "identificationsociete_iso_cd_isi IN ('FR0000120271','US0378331005')"


def test_build_where_clause_raises_on_invalid_isin() -> None:
    with pytest.raises(ValueError, match="Invalid ISIN"):
        mod.build_where_clause(["FR0000120271", "INVALID"])


# ============================================================================
# load_targets
# ============================================================================


def test_load_targets_reads_and_filters_csv(tmp_path: Path) -> None:
    csv_file = tmp_path / "companies.csv"

    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["isin", "name"])
        writer.writeheader()
        writer.writerow({"isin": "FR0000120271", "name": "A"})
        writer.writerow({"isin": "fr0000120271", "name": "A duplicate lowercase"})
        writer.writerow({"isin": "US0378331005", "name": "B"})
        writer.writerow({"isin": "INVALID", "name": "Bad"})
        writer.writerow({"isin": "", "name": "Empty"})

    result = mod.load_targets(str(csv_file))

    assert result == {"FR0000120271", "US0378331005"}


def test_load_targets_raises_if_missing_isin_column(tmp_path: Path) -> None:
    csv_file = tmp_path / "companies.csv"

    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["foo"])
        writer.writeheader()
        writer.writerow({"foo": "bar"})

    with pytest.raises(ValueError, match="CSV must contain an 'isin' column"):
        mod.load_targets(str(csv_file))


# ============================================================================
# build_clean_record
# ============================================================================


def test_build_clean_record_maps_fields_correctly() -> None:
    record = {
        "recordid": "abc123",
        "identificationsociete_iso_nom_soc": "ACME SA",
        "identificationsociete_iso_cd_isi": "FR0000120271",
        "uin_dat_amf": "2025-01-31",
        "url_de_recuperation": "https://example.com/file.pdf",
        "informationdeposee_inf_tit_inf": "Communiqué",
        "sous_type_d_information": "Résultats",
        "type_d_information": "Information réglementée",
    }

    result = mod.build_clean_record(
        record,
        run_id="run-001",
        ingestion_ts="2026-04-01T10:00:00+00:00",
    )

    assert result == {
        "record_id": "abc123",
        "societe": "ACME SA",
        "isin": "FR0000120271",
        "publication_ts": "2025-01-31T00:00:00+00:00",
        "pdf_url": "https://example.com/file.pdf",
        "titre": "Communiqué",
        "sous_type": "Résultats",
        "type_information": "Information réglementée",
        "source": mod.DATA_SOURCE,
        "run_id": "run-001",
        "ingestion_ts": "2026-04-01T10:00:00+00:00",
    }


# ============================================================================
# extract_data
# ============================================================================


def test_extract_data_writes_raw_and_clean_files_and_counts(
    mocker,
    sample_config: mod.Config,
    sample_run_context: mod.RunContext,
) -> None:
    mocker.patch.object(mod, "load_targets", return_value={"FR0000120271", "US0378331005"})
    mocker.patch.object(mod, "build_requests_session", return_value=Mock())
    mocker.patch.object(mod, "isoformat_utc", side_effect=lambda dt: "2026-04-02T10:00:00+00:00")

    response = Mock()
    response.iter_lines.return_value = [
        json.dumps(
            {
                "recordid": "id-1",
                "identificationsociete_iso_nom_soc": "ACME SA",
                "identificationsociete_iso_cd_isi": "FR0000120271",
                "uin_dat_amf": "2025-01-31",
                "url_de_recuperation": "https://example.com/1.pdf",
                "informationdeposee_inf_tit_inf": "Titre 1",
                "sous_type_d_information": "Sous-type 1",
                "type_d_information": "Type 1",
            }
        ),
        json.dumps(
            {
                "recordid": "id-1",
                "identificationsociete_iso_nom_soc": "ACME SA DUP",
                "identificationsociete_iso_cd_isi": "FR0000120271",
                "uin_dat_amf": "2025-01-31",
                "url_de_recuperation": "https://example.com/1-dup.pdf",
                "informationdeposee_inf_tit_inf": "Titre 1 DUP",
                "sous_type_d_information": "Sous-type DUP",
                "type_d_information": "Type DUP",
            }
        ),
        json.dumps(
            {
                "recordid": "id-2",
                "identificationsociete_iso_nom_soc": "APPLE INC",
                "identificationsociete_iso_cd_isi": "US0378331005",
                "uin_dat_amf": "2025-02-01",
                "url_de_recuperation": "https://example.com/2.pdf",
                "informationdeposee_inf_tit_inf": "Titre 2",
                "sous_type_d_information": "Sous-type 2",
                "type_d_information": "Type 2",
            }
        ),
        json.dumps(
            {
                "recordid": "id-3",
                "identificationsociete_iso_nom_soc": "OUT OF SCOPE",
                "identificationsociete_iso_cd_isi": "BE0000000001",
                "uin_dat_amf": "2025-02-01",
                "url_de_recuperation": "https://example.com/3.pdf",
                "informationdeposee_inf_tit_inf": "Titre 3",
                "sous_type_d_information": "Sous-type 3",
                "type_d_information": "Type 3",
            }
        ),
    ]
    mocker.patch.object(mod, "fetch_export_jsonl", return_value=response)

    result = mod.extract_data(
        config=sample_config,
        run_context=sample_run_context,
    )

    assert result == mod.ExtractionArtifacts(
        run_context=sample_run_context,
        raw_count=2,
        clean_count=2,
    )

    raw_path = Path(sample_run_context.raw_output_path)
    clean_path = Path(sample_run_context.clean_output_path)

    assert raw_path.exists()
    assert clean_path.exists()

    raw_lines = raw_path.read_text(encoding="utf-8").splitlines()
    clean_lines = clean_path.read_text(encoding="utf-8").splitlines()

    assert len(raw_lines) == 2
    assert len(clean_lines) == 2

    raw_records = [json.loads(line) for line in raw_lines]
    clean_records = [json.loads(line) for line in clean_lines]

    assert raw_records[0]["recordid"] == "id-1"
    assert raw_records[1]["recordid"] == "id-2"

    assert clean_records[0]["record_id"] == "id-1"
    assert clean_records[1]["record_id"] == "id-2"
    assert clean_records[0]["run_id"] == sample_run_context.run_id
    assert clean_records[0]["source"] == mod.DATA_SOURCE

    response.close.assert_called_once()


# ============================================================================
# dump_gcs
# ============================================================================


def test_dump_gcs_uploads_both_files_and_returns_gcs_artifacts(
    mocker,
    sample_config: mod.Config,
    sample_extraction_artifacts: mod.ExtractionArtifacts,
) -> None:
    raw_path = Path(sample_extraction_artifacts.run_context.raw_output_path)
    clean_path = Path(sample_extraction_artifacts.run_context.clean_output_path)

    raw_path.write_text("raw-content\n", encoding="utf-8")
    clean_path.write_text("clean-content\n", encoding="utf-8")

    fake_storage_client = Mock()
    mocker.patch.object(mod.storage, "Client", return_value=fake_storage_client)

    upload_to_gcs = mocker.patch.object(
        mod,
        "upload_to_gcs",
        side_effect=[
            "gs://my-bucket/amf/raw/run_id=2026-04-02T10-00-00Z/amf_raw.jsonl",
            "gs://my-bucket/amf/clean/run_id=2026-04-02T10-00-00Z/amf_clean.jsonl",
        ],
    )

    result = mod.dump_gcs(
        config=sample_config,
        extraction=sample_extraction_artifacts,
    )

    mod.storage.Client.assert_called_once_with(project=sample_config.project_id)

    assert upload_to_gcs.call_count == 2

    upload_to_gcs.assert_any_call(
        client=fake_storage_client,
        bucket_name=sample_config.bucket_name,
        local_file=raw_path,
        destination_blob="amf/raw/run_id=2026-04-02T10-00-00Z/amf_raw.jsonl",
        location=sample_config.location,
    )

    upload_to_gcs.assert_any_call(
        client=fake_storage_client,
        bucket_name=sample_config.bucket_name,
        local_file=clean_path,
        destination_blob="amf/clean/run_id=2026-04-02T10-00-00Z/amf_clean.jsonl",
        location=sample_config.location,
    )

    assert result == mod.GcsArtifacts(
        extraction=sample_extraction_artifacts,
        raw_uri="gs://my-bucket/amf/raw/run_id=2026-04-02T10-00-00Z/amf_raw.jsonl",
        clean_uri="gs://my-bucket/amf/clean/run_id=2026-04-02T10-00-00Z/amf_clean.jsonl",
    )


# ============================================================================
# cleanup_run_context
# ============================================================================


def test_cleanup_run_context_removes_files_and_directory(tmp_path: Path) -> None:
    raw_file = tmp_path / "amf_raw.jsonl"
    clean_file = tmp_path / "amf_clean.jsonl"

    raw_file.write_text("raw", encoding="utf-8")
    clean_file.write_text("clean", encoding="utf-8")

    run_context = mod.RunContext(
        run_id="run-001",
        tmp_dir=str(tmp_path),
        raw_output_path=str(raw_file),
        clean_output_path=str(clean_file),
    )

    mod.cleanup_run_context(run_context)

    assert not raw_file.exists()
    assert not clean_file.exists()
    assert not tmp_path.exists()


# ============================================================================
# inject_bq
# ============================================================================


def test_inject_bq_calls_expected_steps(
    mocker,
    sample_config: mod.Config,
    sample_gcs_artifacts: mod.GcsArtifacts,
) -> None:
    fake_client = Mock()

    mocker.patch.object(mod.bigquery, "Client", return_value=fake_client)
    ensure_dataset = mocker.patch.object(mod, "ensure_dataset_exists")
    ensure_table = mocker.patch.object(mod, "ensure_table_exists")
    gcs_to_bq = mocker.patch.object(mod, "gcs_to_bq")

    fake_now = datetime(2026, 4, 2, 10, 0, 0, tzinfo=mod.UTC)
    mocker.patch.object(mod, "utc_now", return_value=fake_now)

    query_job = Mock()
    fake_client.query.return_value = query_job

    mod.inject_bq(
        config=sample_config,
        gcs_artifacts=sample_gcs_artifacts,
    )

    mod.bigquery.Client.assert_called_once_with(project=sample_config.project_id)

    ensure_dataset.assert_called_once_with(
        client=fake_client,
        project_id=sample_config.project_id,
        dataset_id=sample_config.dataset_id,
        location=sample_config.location,
    )

    ensure_table.assert_called_once_with(fake_client, sample_config.full_table_id)

    temp_table_id = "my-project.bronze.amf__temp_20260402100000"

    gcs_to_bq.assert_called_once_with(
        client=fake_client,
        gcs_uri=sample_gcs_artifacts.clean_uri,
        full_table_id=temp_table_id,
        write_disposition="WRITE_TRUNCATE",
        schema=mod.BQ_SCHEMA,
    )

    fake_client.query.assert_called_once()
    query_arg = fake_client.query.call_args.args[0]
    assert f"MERGE `{sample_config.full_table_id}` AS T" in query_arg
    assert f"USING `{temp_table_id}` AS S" in query_arg
    assert "WHEN MATCHED THEN" in query_arg
    assert "WHEN NOT MATCHED THEN" in query_arg

    query_job.result.assert_called_once()
    fake_client.delete_table.assert_called_once_with(temp_table_id, not_found_ok=True)
