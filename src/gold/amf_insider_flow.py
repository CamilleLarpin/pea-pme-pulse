import os

import pandas as pd
from google.cloud import bigquery
from loguru import logger

from src.gold.amf_insider_score import compute_insider_score

# --- CONFIGURATION ---
GCP_PREFIX_INPUT_TABLE_ID = "amf_insider_signals"
GCP_PREFIX_OUTPUT_TABLE_ID = "score_insider_signals"


def run_insider_scoring():
    """
    Orchestrates the scoring pipeline from Silver to Gold BigQuery layers.

    Steps:
    1. Fetches validated insider signals from the Silver dataset.
    2. Computes sentiment scores using the amf_insider_score logic.
    3. Normalizes and validates data types (specifically ensuring DATE format).
    4. Performs an idempotent MERGE into the Gold target table via a staging area.

    Inputs (Environment Variables):
        - GOOGLE_APPLICATION_CREDENTIALS: Path to GCP service account JSON.
        - GCP_PROJECT_ID: The target Google Cloud project.
        - BQ_DATASET_SILVER: Source dataset containing signals.
        - BQ_DATASET_GOLD: Destination dataset for scores.

    Outputs:
        - Updates or inserts records in the {project}.{dataset_gold}.score_insider_signals table.
    """

    # Setting up GCP credentials and BigQuery client
    gcp_json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = os.getenv("GCP_PROJECT_ID", "bootcamp-project-pea-pme")

    if not gcp_json_path or not os.path.exists(gcp_json_path):
        logger.error("❌ Credentials not found")
        return

    client = bigquery.Client(project=project_id)
    dataset_gold = os.getenv("BQ_DATASET_GOLD")
    target_table = f"{project_id}.{dataset_gold}.{GCP_PREFIX_OUTPUT_TABLE_ID}"
    staging_table = f"{target_table}_staging"

    # Extract-Transform-Load (ETL) Process (Extract and Transform steps from Silver to Gold)
    logger.info("📥 Fetching silver signals...")
    try:
        query = f"SELECT * FROM `{project_id}.{os.getenv('BQ_DATASET_SILVER')}.{GCP_PREFIX_INPUT_TABLE_ID}`"
        df_silver = client.query(query).to_dataframe()
    except Exception as e:
        logger.error(f"🔥 Query failed: {e}")
        return

    if df_silver.empty:
        logger.warning("⚠️ No data to process.")
        return

    # Process data and compute scores using the imported function from amf_insider_score.py
    logger.info(f"🧪 Scoring {len(df_silver)} rows...")
    df_gold = compute_insider_score(df_silver)
    df_gold["updated_at"] = pd.Timestamp.now(tz="UTC")

    # Critical: force signal_date to datetime so BigQuery doesn't default to STRING
    df_gold["signal_date"] = pd.to_datetime(df_gold["signal_date"]).dt.date

    # Check if target table exists; if not, initialize it with a strictly typed schema
    try:
        client.get_table(target_table)
        logger.info(f"✅ Target table {GCP_PREFIX_OUTPUT_TABLE_ID} exists.")
    except Exception:
        logger.warning(
            f"⚠️ Table {GCP_PREFIX_OUTPUT_TABLE_ID} not found. Creating with explicit schema..."
        )

        # Explicitly define schema to ensure BQ creates the 'DATE' type (not STRING)
        schema = [
            bigquery.SchemaField("isin", "STRING"),
            bigquery.SchemaField("signal_date", "DATE"),
            bigquery.SchemaField("societe", "STRING"),
            bigquery.SchemaField("insider_names", "STRING"),
            bigquery.SchemaField("num_operations", "INTEGER"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("raw_score", "FLOAT"),
            bigquery.SchemaField("score_1_10", "FLOAT"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]

        # Initializing the empty table with the schema above
        job_config_init = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_EMPTY")
        client.load_table_from_dataframe(
            df_gold.iloc[0:0], target_table, job_config=job_config_init
        ).result()
        logger.success("🆕 Target table created successfully with DATE type.")

    # Persist results to Gold layer using a staging table and an idempotent MERGE
    logger.info("📤 Loading staging...")
    job_config_staging = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_gold, staging_table, job_config=job_config_staging).result()

    # Execute Idempotent MERGE (Upsert logic based on ISIN and Date)
    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.isin = S.isin AND T.signal_date = S.signal_date
    WHEN MATCHED THEN
      UPDATE SET 
        T.insider_names = S.insider_names,
        T.num_operations = S.num_operations,
        T.total_amount = S.total_amount,
        T.raw_score = S.raw_score,
        T.score_1_10 = S.score_1_10, 
        T.updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (isin, societe, signal_date, insider_names, num_operations, total_amount, raw_score, score_1_10, updated_at)
      VALUES (isin, societe, signal_date, insider_names, num_operations, total_amount, raw_score, score_1_10, updated_at)
    """

    logger.info("🔄 Running Idempotent Merge...")
    try:
        client.query(merge_query).result()
        logger.success("✅ Gold table updated!")
    except Exception as e:
        logger.error(f"🔥 Merge error: {e}")
    finally:
        # Cleanup staging resources
        client.delete_table(staging_table, not_found_ok=True)


if __name__ == "__main__":
    run_insider_scoring()
