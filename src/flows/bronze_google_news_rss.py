"""Prefect flow — Bronze RSS ingestion (Google News)."""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Prefect Managed runner passes GCP credentials as JSON env var — write to temp file
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_gcp_creds_json)
    _tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

import pandas as pd
from prefect import flow, task, get_run_logger

REFERENTIEL_PATH = Path(__file__).parent.parent.parent / "referentiel" / "boursorama_peapme_final.csv"


def _load_referentiel() -> pd.DataFrame:
    return pd.read_csv(REFERENTIEL_PATH)


@task(name="google-news-fetch", retries=2, retry_delay_seconds=30)
def google_news_fetch() -> list[dict]:
    from bronze.rss_google_news import fetch_all_feeds
    return fetch_all_feeds()


@task(name="google-news-dump-gcs", retries=2, retry_delay_seconds=30)
def google_news_dump_gcs(entries: list[dict]) -> None:
    from bronze.rss_google_news import dump_to_gcs
    dump_to_gcs(entries)


@task(name="google-news-match-load-bq", retries=2, retry_delay_seconds=60)
def google_news_match_load_bq(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    from bronze.rss_google_news import match_companies, write_to_bigquery
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    return df


@flow(name="bronze-google-news-rss")
def google_news_rss_flow() -> None:
    logger = get_run_logger()
    referentiel = _load_referentiel()
    entries = google_news_fetch()
    google_news_dump_gcs(entries)
    df = google_news_match_load_bq(entries, referentiel)
    matched = df["isin"].notna().sum()
    logger.info("google_news_rss_flow complete — %d articles, %d matched", len(df), matched)


if __name__ == "__main__":
    google_news_rss_flow()
