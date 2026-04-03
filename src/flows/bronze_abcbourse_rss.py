"""Prefect flow — Bronze RSS ingestion (ABC Bourse)."""

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

# Configuration to find custom modules (src)
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# GCP credential management for Prefect Managed Runner — write to temp file
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

# Path constants (Referentiel)
REFERENTIEL_PATH = Path(__file__).parent.parent.parent / os.environ.get("GCS_SOURCE_REFERENTIEL")

# --- TASKS ---


@task(name="abcbourse-load-local-db")
def task_load_local_db(filepath="abcbourse_data_raw.json"):
    """
    Loads the local JSON database for deduplication purposes.

    Input:
        - filepath (str): Path to the local JSON file.
    Output:
        - db_items (dict): Dictionary containing historical RSS entries.
    """
    from bronze.rss_abcbourse import load_local_db

    return load_local_db(filepath)


@task(name="abcbourse-fetch-rss", retries=2, retry_delay_seconds=30)
def task_import_rss(db_items: dict) -> dict:
    """
    Fetches ABC Bourse RSS feeds and updates the database dictionary (deduplication).

    Input:
        - db_items (dict): The current in-memory database state.
    Output:
        - updated_db (dict): Database updated with new unique entries.
    """
    from bronze.rss_abcbourse import import_rss

    # Note: import_rss modifies the dictionary in-place in the original script
    import_rss(db_items)
    return db_items


@task(name="abcbourse-save-local-db")
def task_save_local_db(data: dict, filename: str):
    """
    Saves the current database state to a local JSON file.

    Input:
        - data (dict): The dictionary to persist.
        - filename (str): The target local filename.
    Output:
        - None
    """
    from bronze.rss_abcbourse import save_local_db

    save_local_db(data, filename)


@task(name="abcbourse-match-fuzzy", retries=1)
def task_filter_fuzzy(db_items: dict, referentiel: pd.DataFrame) -> dict:
    """
    Performs fuzzy matching between RSS news and companies in the referentiel.

    Input:
        - db_items (dict): The raw ingested entries.
        - referentiel (pd.DataFrame): The reference table of companies.
    Output:
        - filtered_db (dict): Only entries that successfully matched a target company.
    """
    from bronze.rss_abcbourse import filter_rss_entries_fuzzy

    return filter_rss_entries_fuzzy(db_items, referentiel)


@task(name="abcbourse-upload-gcs")
def task_upload_gcs(local_path: str, bucket: str):
    """
    Uploads the RAW JSON dump to Google Cloud Storage.

    Input:
        - local_path (str): Path of the local file to upload.
        - bucket (str): Destination GCS bucket name.
    Output:
        - success (bool): Result of the upload operation.
    """
    from bronze.rss_abcbourse import upload_to_bucket

    upload_to_bucket(local_path, bucket, local_path)


@task(name="abcbourse-load-bigquery")
def task_load_bq(filtered_items: dict, project_id: str, dataset_id: str):
    """
    Loads matched records into BigQuery tables.

    Input:
        - filtered_items (dict): The matched entries to upload.
        - project_id (str): GCP Project ID.
        - dataset_id (str): BigQuery Dataset ID.
    Output:
        - None
    """
    from bronze.rss_abcbourse import upload_full_json_to_bigquery

    upload_full_json_to_bigquery(filtered_items, project_id, dataset_id)


# --- FLOW ---


@flow(name="bronze-abcbourse-rss")
def abcbourse_rss_flow():
    """
    Main orchestration flow for ABC Bourse RSS ingestion.
    """
    logger = get_run_logger()

    # 1. Configuration setup (from env vars)
    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    dataset_id = os.getenv("BQ_DATASET_BRONZE")
    raw_json_path = "abcbourse_data_raw.json"

    # 2. Load local referentiel
    referentiel = pd.read_csv(REFERENTIEL_PATH)
    referentiel.columns = [c.strip().lower() for c in referentiel.columns]

    # 3. Pipeline Execution
    logger.info("Starting ABC Bourse data ingestion...")

    current_db = task_load_local_db(raw_json_path)
    updated_db = task_import_rss(current_db)

    # Save state and backup RAW data
    task_save_local_db(updated_db, raw_json_path)
    task_upload_gcs(raw_json_path, bucket_name)

    # Fuzzy Match and BigQuery Load
    matched_items = task_filter_fuzzy(updated_db, referentiel)

    if matched_items:
        task_load_bq(matched_items, project_id, dataset_id)
        logger.info("Flow complete: Data successfully matched and loaded to BigQuery.")
    else:
        logger.warning("No matches found during this run.")


if __name__ == "__main__":
    abcbourse_rss_flow()
