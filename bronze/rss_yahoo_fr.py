"""Bronze ingestion — Yahoo Finance FR RSS."""

from datetime import datetime, timezone

import feedparser
import pandas as pd
from rapidfuzz import fuzz, process

# TODO: verify exact URL with team before production
FEED_URL = "https://fr.finance.yahoo.com/news/rssindex"

# TODO: fill in once GCP project/dataset/service account are ready
BQ_PROJECT = "TODO"
BQ_DATASET = "TODO"
BQ_TABLE = "bronze_yahoo_rss"

MATCH_THRESHOLD = 80


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


def match_companies(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fuzzy-match article titles against company names (rapidfuzz >= 80).

    Matched entries get isin + ticker_bourso attached.
    Unmatched entries are kept with null values for those fields.
    """
    company_names = referentiel["nom"].tolist()
    rows = []
    for entry in entries:
        match = process.extractOne(
            entry["title"],
            company_names,
            scorer=fuzz.partial_ratio,
            score_cutoff=MATCH_THRESHOLD,
            processor=str.casefold,
        )
        if match:
            matched_name, score, _ = match
            ref_row = referentiel[referentiel["nom"] == matched_name].iloc[0]
            rows.append({
                **entry,
                "matched_nom": matched_name,
                "match_score": score,
                "isin": ref_row["isin"],
                "ticker_bourso": ref_row["ticker_bourso"],
            })
        else:
            rows.append({
                **entry,
                "matched_nom": None,
                "match_score": None,
                "isin": None,
                "ticker_bourso": None,
            })
    return pd.DataFrame(rows)


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write matched records to BigQuery Bronze table.

    TODO: implement once GCP project/dataset/service account are ready.
    from google.cloud import bigquery
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    client.insert_rows_from_dataframe(client.get_table(table_id), df)
    """
    raise NotImplementedError("BigQuery write not yet configured — awaiting GCP setup")


def run(referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fetch, match, and load Yahoo Finance FR RSS entries."""
    entries = fetch_feed()
    df = match_companies(entries, referentiel)
    write_to_bigquery(df)
    return df
