"""Bronze ingestion — Google News RSS (Euronext Growth + PME Bourse FR)."""

import json
from datetime import UTC, datetime

import feedparser
import pandas as pd
from google.cloud import bigquery, storage
from loguru import logger

from bronze.fuzzy_match import match_companies

FEED_URLS = {
    "euronext_growth": (
        "https://news.google.com/rss/search?q=Euronext+Growth+France&hl=fr&gl=FR&ceid=FR:fr"
    ),
    "pme_bourse_fr": (
        "https://news.google.com/rss/search?q=bourse+PME+France+small+cap&hl=fr&gl=FR&ceid=FR:fr"
    ),
}

BQ_PROJECT = "arched-run-488313-h2"
BQ_DATASET = "bronze"
BQ_TABLE = "google_news_rss"

GCS_BUCKET = "pea-pme-yf"
GCS_PREFIX = "rss_google_news"


def fetch_feed(feed_name: str, url: str) -> list[dict]:
    """Fetch a single Google News RSS feed and return raw entries."""
    feed = feedparser.parse(url)
    fetched_at = datetime.now(UTC).isoformat()
    return [
        {
            "feed_name": feed_name,
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", ""),
            "fetched_at": fetched_at,
        }
        for entry in feed.entries
    ]


def fetch_all_feeds(urls: dict[str, str] = FEED_URLS) -> list[dict]:
    """Fetch all configured Google News RSS feeds and deduplicate by title."""
    all_entries = []
    seen_titles = set()
    for feed_name, url in urls.items():
        for entry in fetch_feed(feed_name, url):
            if entry["title"] not in seen_titles:
                seen_titles.add(entry["title"])
                all_entries.append(entry)
    return all_entries


def dump_to_gcs(entries: list[dict]) -> None:
    """Dump raw feed entries to GCS as timestamped JSON.

    RSS feeds have no history — raw dump preserves original data before any filtering.
    """
    client = storage.Client(project=BQ_PROJECT)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"{GCS_PREFIX}/{timestamp}.json"
    bucket = client.bucket(GCS_BUCKET)
    bucket.blob(blob_path).upload_from_string(
        json.dumps(entries, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info("GCS dump: gs://{}/{}", GCS_BUCKET, blob_path)


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write matched records to BigQuery bronze.google_news_rss."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    if "match_score" in df.columns:
        df = df.copy()
        df["match_score"] = df["match_score"].astype(float)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info("BQ load: {} rows → {}", len(df), table_id)


def run(referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fetch, dump to GCS, match, and load Google News RSS entries."""
    entries = fetch_all_feeds()
    dump_to_gcs(entries)
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    else:
        logger.info("BQ load: no matched companies — skipping")
    return df
