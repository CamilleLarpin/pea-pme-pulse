"""Prefect flow — Silver RSS → Gold sentiment scoring via Groq.

Reads Silver rows not yet scored → calls Groq per article → writes to gold.article_sentiment.
Triggered by bronze-silver-rss via run_deployment() after Silver dbt refresh.
"""

import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_gcp_creds_json)
    _tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

import subprocess

import pandas as pd
from google.cloud import bigquery
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from gold.sentiment_scorer import MODEL, score_article

GCP_PROJECT = "bootcamp-project-pea-pme"
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"
SILVER_TABLE = f"{GCP_PROJECT}.silver.rss_articles"
GOLD_TABLE = f"{GCP_PROJECT}.gold.article_sentiment"

# DDL — created on first run if table does not exist
_CREATE_GOLD_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{GOLD_TABLE}` (
    row_id          STRING NOT NULL,
    isin            STRING NOT NULL,
    ticker_bourso   STRING,
    matched_name    STRING,
    title           STRING,
    published_at    TIMESTAMP,
    sentiment_score INT64 NOT NULL,
    sentiment_reason STRING,
    groq_model      STRING,
    scored_at       TIMESTAMP NOT NULL
)
"""


@task(name="ensure-gold-table", cache_policy=NO_CACHE)
def ensure_gold_table(client: bigquery.Client) -> None:
    dataset = bigquery.Dataset(f"{GCP_PROJECT}.gold")
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    client.query(_CREATE_GOLD_TABLE).result()


@task(name="fetch-unscored-articles", cache_policy=NO_CACHE)
def fetch_unscored_articles(client: bigquery.Client) -> pd.DataFrame:
    logger = get_run_logger()
    query = f"""
        SELECT
            s.row_id,
            s.isin,
            s.ticker_bourso,
            s.matched_name,
            s.title,
            s.summary,
            s.published_at
        FROM `{SILVER_TABLE}` s
        LEFT JOIN `{GOLD_TABLE}` g ON s.row_id = g.row_id
        WHERE g.row_id IS NULL
    """
    df = client.query(query).to_dataframe()
    logger.info("Unscored articles to process: %d", len(df))
    return df


@task(name="score-and-write", retries=1, retry_delay_seconds=120, cache_policy=NO_CACHE)
def score_and_write(df: pd.DataFrame, client: bigquery.Client) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No new articles to score")
        return 0

    api_key = os.environ.get("GROQ_API_KEY")
    scored_at = datetime.now(timezone.utc)
    rows = []

    for _, row in df.iterrows():
        try:
            result = score_article(row["title"], row.get("summary"), api_key=api_key)
            rows.append({
                "row_id": row["row_id"],
                "isin": row["isin"],
                "ticker_bourso": row.get("ticker_bourso"),
                "matched_name": row.get("matched_name"),
                "title": row["title"],
                "published_at": row.get("published_at"),
                "sentiment_score": result["score"],
                "sentiment_reason": result["reason"],
                "groq_model": MODEL,
                "scored_at": scored_at,
            })
        except Exception as e:
            logger.warning("Skipping row_id=%s — %s", row["row_id"], e)

    if not rows:
        logger.warning("All articles failed scoring — nothing written")
        return 0

    scored_df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(scored_df, GOLD_TABLE, job_config=job_config).result()
    logger.info("Scored and wrote %d articles to %s", len(rows), GOLD_TABLE)
    return len(rows)


@task(name="dbt-run-gold", retries=1, retry_delay_seconds=60, cache_policy=NO_CACHE)
def dbt_run_gold() -> None:
    logger = get_run_logger()
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    cmd = [
        "dbt", "run",
        "--select", "gold.score_news",
        "--project-dir", str(DBT_PROJECT_DIR),
    ]

    if keyfile:
        method = "service-account"
        extra = f"      keyfile: {keyfile}\n"
    else:
        method = "oauth"
        extra = ""

    profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: {method}
      project: {GCP_PROJECT}
      dataset: gold
      threads: 4
      timeout_seconds: 300
      location: EU
{extra}"""
    with tempfile.TemporaryDirectory() as profiles_dir:
        (Path(profiles_dir) / "profiles.yml").write_text(profiles)
        result = subprocess.run(
            cmd + ["--profiles-dir", profiles_dir],
            capture_output=True,
            text=True,
        )
        logger.info("dbt stdout:\n%s", result.stdout)
        if result.returncode != 0:
            logger.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt run failed (exit {result.returncode})")

    logger.info("dbt-run-gold complete")


@flow(name="silver-gold-rss")
def silver_gold_rss_flow() -> None:
    logger = get_run_logger()
    client = bigquery.Client(project=GCP_PROJECT, location="EU")
    ensure_gold_table(client)
    df = fetch_unscored_articles(client)
    n = score_and_write(df, client)
    dbt_run_gold()
    logger.info("silver-gold-rss complete — %d articles scored", n)


if __name__ == "__main__":
    silver_gold_rss_flow()
