"""Prefect flow — Bronze RSS ingestion (Yahoo Finance FR)."""

from pathlib import Path

import pandas as pd
from prefect import flow, task, get_run_logger

REFERENTIEL_PATH = Path(__file__).parent.parent.parent / "referentiel" / "boursorama_peapme_final.csv"


def _load_referentiel() -> pd.DataFrame:
    return pd.read_csv(REFERENTIEL_PATH)


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


@flow(name="bronze-yahoo-rss")
def yahoo_rss_flow() -> None:
    logger = get_run_logger()
    referentiel = _load_referentiel()
    entries = yahoo_fetch()
    yahoo_dump_gcs(entries)
    df = yahoo_match_load_bq(entries, referentiel)
    matched = df["isin"].notna().sum()
    logger.info("yahoo_rss_flow complete — %d articles, %d matched", len(df), matched)


if __name__ == "__main__":
    yahoo_rss_flow()
