import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

# Ensure the 'src' directory is in the python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from gold.amf_insider_score import compute_insider_score

# --- GCP Credentials Setup ---
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

# Global configuration for BigQuery tables
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "bootcamp-project-pea-pme")
DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "silver")
DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "gold")

INPUT_TABLE = f"{GCP_PROJECT}.{DATASET_SILVER}.amf_insider_signals"
OUTPUT_TABLE = f"{GCP_PROJECT}.{DATASET_GOLD}.score_insider_signals"
STAGING_TABLE = f"{OUTPUT_TABLE}_staging"


@task(name="fetch-silver-signals", cache_policy=NO_CACHE)
def fetch_silver_signals(client: bigquery.Client) -> pd.DataFrame:
    """
    Fetches raw insider transaction signals from the Silver layer.

    Args:
        client (bigquery.Client): Initialized BigQuery client.

    Returns:
        pd.DataFrame: A DataFrame containing all rows from the silver signals table.
    """
    logger = get_run_logger()
    query = f"SELECT * FROM `{INPUT_TABLE}`"

    logger.info(f"📥 Fetching raw signals from: {INPUT_TABLE}")
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.warning("Empty result set returned from Silver table.")
    return df


@task(name="compute-scores")
def process_insider_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies scoring logic to the insider data and adds audit timestamps.

    Args:
        df (pd.DataFrame): The raw silver signals data.

    Returns:
        pd.DataFrame: Processed data with 'insider_score' and 'updated_at' columns.
    """
    logger = get_run_logger()
    logger.info(f"🧪 Processing {len(df)} records through scoring algorithm...")

    df_scored = compute_insider_score(df)
    df_scored["updated_at"] = pd.Timestamp.now()

    return df_scored


@task(name="idempotent-merge", cache_policy=NO_CACHE)
def idempotent_merge(client: bigquery.Client, df_gold: pd.DataFrame) -> None:
    """
    Performs an idempotent MERGE into the Gold table via a staging table.

    Args:
        client (bigquery.Client): Initialized BigQuery client.
        df_gold (pd.DataFrame): The processed and scored data to be uploaded.

    Returns:
        None: Updates the BigQuery table in-place.
    """
    logger = get_run_logger()

    # Step A: Load results to Staging Table
    logger.info(f"📤 Truncating staging table: {STAGING_TABLE}")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_gold, STAGING_TABLE, job_config=job_config).result()

    # Step B: Merge Staging into Gold Table
    merge_query = f"""
    MERGE `{OUTPUT_TABLE}` T
    USING `{STAGING_TABLE}` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET 
        T.insider_score = S.insider_score, 
        T.updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    logger.info(f"🔄 Executing UPSERT (MERGE) logic into: {OUTPUT_TABLE}")
    try:
        query_job = client.query(merge_query)
        query_job.result()
        logger.info(f"✅ Success. MERGE affected {query_job.num_dml_affected_rows} rows.")
    finally:
        # Cleanup staging table to minimize BigQuery clutter
        client.delete_table(STAGING_TABLE, not_found_ok=True)


@flow(name="gold-insider-scoring")
def gold_insider_flow():
    """
    Main orchestration flow to move Insider Signals from Silver to Gold.

    Execution Steps:
        1. Initialize BigQuery client.
        2. Fetch data from Silver.
        3. Compute scores via Python module.
        4. Upsert (Merge) data into Gold table for idempotency.
    """
    logger = get_run_logger()
    logger.info("🚀 Starting Silver-to-Gold Insider Scoring Flow")

    client = bigquery.Client(project=GCP_PROJECT)

    # Flow Steps
    df_silver = fetch_silver_signals(client)

    if df_silver.empty:
        logger.info("No new signals found. Flow termination.")
        return

    df_gold = process_insider_scores(df_silver)

    idempotent_merge(client, df_gold)

    logger.info("🎯 Gold Insider Scoring Flow finished successfully.")


if __name__ == "__main__":
    gold_insider_flow()
