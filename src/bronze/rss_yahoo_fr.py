"""Bronze ingestion — Yahoo Finance FR RSS."""

import json
from datetime import datetime, timezone

import feedparser
import pandas as pd
from google.cloud import bigquery, storage
from rapidfuzz import fuzz, process

# TODO: verify exact URL with team before production
FEED_URL = "https://fr.finance.yahoo.com/news/rssindex"

BQ_PROJECT = "bootcamp-project-pea-pme"
BQ_DATASET = "bronze"
BQ_TABLE = "yahoo_rss"

GCS_BUCKET = "project-pea-pme"
GCS_PREFIX = "rss_yahoo"

MATCH_THRESHOLD = 80
SHORT_NAME_MAX_LEN = 6  # names shorter than this use fuzz.ratio (strict full match)


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
    print(f"GCS dump: gs://{GCS_BUCKET}/{blob_path}")


def _scorer_for(name: str):
    """Use token_set_ratio for short names (requires name as whole token), partial_ratio for longer ones."""
    return fuzz.token_set_ratio if len(name) <= SHORT_NAME_MAX_LEN else fuzz.partial_ratio


def match_companies(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fuzzy-match article titles against company names (rapidfuzz >= 80).

    Short names (<=6 chars) use fuzz.ratio (strict) to avoid false positives.
    Longer names use fuzz.partial_ratio (flexible substring match).
    Matched entries get isin + ticker_bourso attached.
    Unmatched entries are kept with null values for those fields.
    """
    company_names = referentiel["name"].tolist()
    rows = []
    for entry in entries:
        best_match = None
        for name in company_names:
            scorer = _scorer_for(name)
            result = process.extractOne(
                entry["title"],
                [name],
                scorer=scorer,
                score_cutoff=MATCH_THRESHOLD,
                processor=str.casefold,
            )
            if result and (best_match is None or result[1] > best_match[1]):
                best_match = result
        match = best_match
        if match:
            matched_name, score, _ = match
            ref_row = referentiel[referentiel["name"] == matched_name].iloc[0]
            rows.append({
                **entry,
                "matched_name": matched_name,
                "match_score": score,
                "isin": ref_row["isin"],
                "ticker_bourso": ref_row["ticker_bourso"],
            })
        else:
            rows.append({
                **entry,
                "matched_name": None,
                "match_score": None,
                "isin": None,
                "ticker_bourso": None,
            })
    return pd.DataFrame(rows)


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write matched records to BigQuery bronze.yahoo_rss."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"BQ load: {len(df)} rows → {table_id}")


def run(referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fetch, dump to GCS, match, and load Yahoo Finance FR RSS entries."""
    entries = fetch_feed()
    dump_to_gcs(entries)
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    else:
        print("BQ load: no matched companies — skipping")
    return df
