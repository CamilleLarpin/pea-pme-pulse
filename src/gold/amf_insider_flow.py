import os

import pandas as pd
from google.cloud import bigquery
from loguru import logger

from src.gold.amf_insider_score import compute_insider_score

GCP_PREFIX_INPUT_TABLE_ID = "amf_insider_signals"
GCP_PREFIX_OUTPUT_TABLE_ID = "score_insider_signals"


def run_insider_scoring():
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

    # Extraction from Silver
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

    # Processing & Scoring
    logger.info(f"🧪 Scoring {len(df_silver)} rows...")
    df_gold = compute_insider_score(df_silver)
    df_gold["updated_at"] = pd.Timestamp.now(tz="UTC")

    # CRITICAL: Force signal_date to datetime so BigQuery doesn't default to STRING
    df_gold["signal_date"] = pd.to_datetime(df_gold["signal_date"]).dt.date

    # Check if target table exists, if not create with explicit schema to ensure signal_date is DATE
    try:
        client.get_table(target_table)
        logger.info(f"✅ Target table {GCP_PREFIX_OUTPUT_TABLE_ID} exists.")
    except Exception:
        logger.warning(
            f"⚠️ Table {GCP_PREFIX_OUTPUT_TABLE_ID} not found. Creating with explicit schema..."
        )

        # We explicitly define signal_date as DATE
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

        # Schema is required to ensure signal_date is created as DATE type, preventing future merge issues
        job_config_init = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_EMPTY")
        client.load_table_from_dataframe(
            df_gold.iloc[0:0], target_table, job_config=job_config_init
        ).result()
        logger.success("🆕 Target table created successfully with DATE type.")

    # Idempotent Merge into Gold
    logger.info("📤 Loading staging...")
    # Autodetect schema for staging based on the corrected df_gold
    job_config_staging = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_gold, staging_table, job_config=job_config_staging).result()

    # REMOVED CASTS: Since both are now DATE types, simple equality works
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
        client.delete_table(staging_table, not_found_ok=True)


if __name__ == "__main__":
    run_insider_scoring()
