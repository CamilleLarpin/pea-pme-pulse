"""Prefect flow — Gold Insider Scoring pipeline.

Step 1 — fetch_silver_signals:
    Reads silver.amf_insider_signals from BigQuery.

Step 2 — compute_insider_scores:
    Applies scoring logic to insider data and adds audit timestamps
    via the gold.amf_insider_score module.

Step 3 — idempotent_merge:
    Performs an idempotent UPSERT (MERGE) into gold.score_insider_signals
    using a temporary staging table.
"""

import os
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

# Ensure the 'src' directory is in the python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from gold.amf_insider_score import compute_insider_score

# Global configuration
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "bootcamp-project-pea-pme")
DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "silver")
DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "gold")

INPUT_TABLE = f"{GCP_PROJECT}.{DATASET_SILVER}.amf_insider_signals"
OUTPUT_TABLE = f"{GCP_PROJECT}.{DATASET_GOLD}.score_insider_signals"


@task(name="fetch-silver-signals", cache_policy=NO_CACHE)
def fetch_silver_signals(project_id, table_id):
    """Fetch raw insider signals from BigQuery Silver layer."""
    logger = get_run_logger()
    client = bigquery.Client(project=project_id)

    logger.info(f"Fetching signals from: {table_id}")
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.warning("No data found in Silver table.")
    return df


@task(name="compute-insider-scores")
def compute_insider_scores(df):
    """Apply scoring logic and add audit timestamps."""
    if df.empty:
        return df

    logger = get_run_logger()
    logger.info(f"Computing scores for {len(df)} records...")

    # Logic from your src/gold module
    df_scored = compute_insider_score(df)
    df_scored["updated_at"] = pd.Timestamp.now(tz="Europe/Paris")

    return df_scored


@task(name="idempotent-merge", cache_policy=NO_CACHE)
def idempotent_merge(project_id, target_table, df):
    """Perform an idempotent MERGE via a staging table."""
    if df.empty:
        return {"status": "skipped", "rows_affected": 0}

    logger = get_run_logger()
    client = bigquery.Client(project=project_id)
    staging_table = f"{target_table}_staging"

    # Step A: Load results to Staging Table
    logger.info(f"Loading data to staging: {staging_table}")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()

    # Step B: Merge Staging into Gold Table
    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.source_record_id = S.source_record_id
    WHEN MATCHED THEN
      UPDATE SET 
        T.insider_score = S.insider_score, 
        T.updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    logger.info(f"Executing MERGE logic into: {target_table}")
    try:
        query_job = client.query(merge_query)
        query_job.result()
        affected = query_job.num_dml_affected_rows
        logger.info(f"MERGE complete. Rows affected: {affected}")
        return {"status": "success", "rows_affected": affected}
    finally:
        # Clean up staging table to keep BigQuery tidy
        client.delete_table(staging_table, not_found_ok=True)


@flow(name="gold-insider-scoring")
def gold_insider_scoring_flow():
    """Main orchestration flow: Silver to Gold Insider Scoring."""
    logger = get_run_logger()
    logger.info("Starting Gold Insider Scoring Pipeline")

    # 1. Extraction (Silver)
    df_silver = fetch_silver_signals(GCP_PROJECT, INPUT_TABLE)

    if df_silver.empty:
        logger.info("Pipeline stopped: No input data found in Silver layer.")
        return

    # 2. Transformation (Scoring Logic)
    df_gold = compute_insider_scores(df_silver)

    # 3. Loading (Gold)
    idempotent_merge(GCP_PROJECT, OUTPUT_TABLE, df_gold)

    logger.info("Gold Insider Scoring Pipeline finished successfully")


if __name__ == "__main__":
    gold_insider_scoring_flow()
