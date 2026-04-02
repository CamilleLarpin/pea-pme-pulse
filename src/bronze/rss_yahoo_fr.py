"""Bronze ingestion — Yahoo Finance FR RSS."""

import json
from datetime import datetime, timezone

import feedparser
import pandas as pd
from google.cloud import bigquery, storage
from loguru import logger

from bronze.fuzzy_match import match_companies

# TODO: verify exact URL with team before production
FEED_URL = "https://fr.finance.yahoo.com/news/rssindex"

BQ_PROJECT = "bootcamp-project-pea-pme"
BQ_DATASET = "bronze"
BQ_TABLE = "yahoo_rss"

GCS_BUCKET = "project-pea-pme"
GCS_PREFIX = "rss_yahoo"


def fetch_feed(url: str = FEED_URL) -> list[dict]:
    """Fetch Yahoo Finance FR RSS and return raw entries."""
    feed = feedparser.parse(url)
    fetched_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", ""),
            "fetched_at": fetched_at,
        }
        for entry in feed.entries
    ]


def dump_to_gcs(entries: list[dict]) -> None:
    """Dump raw feed entries to GCS as timestamped JSON.

    RSS feeds have no history — raw dump preserves original data before any filtering.
    """
    client = storage.Client(project=BQ_PROJECT)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"{GCS_PREFIX}/{timestamp}.json"
    bucket = client.bucket(GCS_BUCKET)
    bucket.blob(blob_path).upload_from_string(
        json.dumps(entries, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info("GCS dump: gs://{}/{}", GCS_BUCKET, blob_path)


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write matched records to BigQuery bronze.yahoo_rss."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info("BQ load: {} rows → {}", len(df), table_id)


def run(referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fetch, dump to GCS, match, and load Yahoo Finance FR RSS entries."""
    entries = fetch_feed()
    dump_to_gcs(entries)
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    else:
        logger.info("BQ load: no matched companies — skipping")
    return df
