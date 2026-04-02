"""Prefect flows — Bronze RSS ingestion (Yahoo Finance FR + Google News)."""

from pathlib import Path

import pandas as pd
from prefect import flow, task, get_run_logger

REFERENTIEL_PATH = Path(__file__).parent.parent.parent / "referentiel" / "boursorama_peapme_final.csv"


def _load_referentiel() -> pd.DataFrame:
    return pd.read_csv(REFERENTIEL_PATH)


# --- Yahoo Finance FR tasks ---

@task(name="yahoo-fetch", retries=2, retry_delay_seconds=30)
def yahoo_fetch() -> list[dict]:
    from bronze.rss_yahoo_fr import fetch_feed
    return fetch_feed()


@task(name="yahoo-dump-gcs", retries=2, retry_delay_seconds=30)
def yahoo_dump_gcs(entries: list[dict]) -> None:
    from bronze.rss_yahoo_fr import dump_to_gcs
    dump_to_gcs(entries)


@task(name="yahoo-match-load-bq", retries=2, retry_delay_seconds=60)
def yahoo_match_load_bq(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    from bronze.rss_yahoo_fr import match_companies, write_to_bigquery
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    return df


# --- Google News tasks ---

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


# --- Flows ---

@flow(name="bronze-yahoo-rss")
def yahoo_rss_flow() -> None:
    logger = get_run_logger()
    referentiel = _load_referentiel()
    entries = yahoo_fetch()
    yahoo_dump_gcs(entries)
    df = yahoo_match_load_bq(entries, referentiel)
    matched = df["isin"].notna().sum()
    logger.info("yahoo_rss_flow complete — %d articles, %d matched", len(df), matched)


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
    yahoo_rss_flow()
    google_news_rss_flow()
