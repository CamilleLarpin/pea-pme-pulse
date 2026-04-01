from __future__ import annotations

import csv
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
        staging_table_id="amf_staging",
        csv_path="referentiel/boursorama_peapme_final.csv",
        gcs_prefix="amf",
        chunk_size=2,
        request_timeout=120,
        write_disposition_staging="WRITE_TRUNCATE",
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
        ("fr0000120271", True),  # load_targets upper() avant validation
        ("", False),
        ("123", False),
        ("FR000012027", False),  # trop court
        ("FR00001202711", False),  # trop long
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
# build_where_clause
# ============================================================================


def test_build_where_clause_ok() -> None:
    isins = ["FR0000120271", "US0378331005"]

    result = mod.build_where_clause(isins)

    assert result == ("identificationsociete_iso_cd_isi IN ('FR0000120271','US0378331005')")


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
# merge_staging_into_target
# ============================================================================


def test_merge_staging_into_target_executes_query() -> None:
    client = Mock()
    job = Mock()
    client.query.return_value = job

    mod.merge_staging_into_target(
        client=client,
        staging_table_id="my-project.bronze.amf_staging",
        target_table_id="my-project.bronze.amf",
    )

    client.query.assert_called_once()
    query_arg = client.query.call_args.args[0]

    assert "MERGE `my-project.bronze.amf` AS T" in query_arg
    assert "USING `my-project.bronze.amf_staging` AS S" in query_arg
    assert "ON T.record_id = S.record_id" in query_arg

    job.result.assert_called_once()


# ============================================================================
# inject_bq
# ============================================================================


def test_inject_bq_calls_expected_steps(mocker, sample_config: mod.Config) -> None:
    fake_client = Mock()

    mocker.patch.object(mod.bigquery, "Client", return_value=fake_client)
    ensure_dataset = mocker.patch.object(mod, "ensure_dataset_exists")
    ensure_table = mocker.patch.object(mod, "ensure_table_exists")
    gcs_to_bq = mocker.patch.object(mod, "gcs_to_bq")
    merge = mocker.patch.object(mod, "merge_staging_into_target")

    mod.inject_bq(sample_config, "gs://bucket/amf/clean/file.jsonl")

    mod.bigquery.Client.assert_called_once_with(project=sample_config.project_id)

    ensure_dataset.assert_called_once_with(
        fake_client,
        sample_config.project_id,
        sample_config.dataset_id,
        sample_config.location,
    )

    assert ensure_table.call_count == 2
    ensure_table.assert_any_call(fake_client, sample_config.full_table_id)
    ensure_table.assert_any_call(fake_client, sample_config.full_staging_table_id)

    gcs_to_bq.assert_called_once_with(
        client=fake_client,
        gcs_uri="gs://bucket/amf/clean/file.jsonl",
        full_table_id=sample_config.full_staging_table_id,
        write_disposition=sample_config.write_disposition_staging,
        schema=mod.BQ_SCHEMA,
    )

    merge.assert_called_once_with(
        client=fake_client,
        staging_table_id=sample_config.full_staging_table_id,
        target_table_id=sample_config.full_table_id,
    )
