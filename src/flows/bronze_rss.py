"""Prefect flows — Bronze RSS ingestion (Yahoo Finance FR + Google News)."""

from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

REFERENTIEL_PATH = Path(__file__).parent.parent.parent / "referentiel" / "boursorama_peapme_final.csv"


def _load_referentiel() -> pd.DataFrame:
    return pd.read_csv(REFERENTIEL_PATH)


@flow(name="bronze-yahoo-rss")
def yahoo_rss_flow() -> None:
    from bronze.rss_yahoo_fr import run

    logger = get_run_logger()
    referentiel = _load_referentiel()
    df = run(referentiel)
    matched = df["isin"].notna().sum()
    logger.info("yahoo_rss_flow complete — %d articles, %d matched", len(df), matched)


@flow(name="bronze-google-news-rss")
def google_news_rss_flow() -> None:
    from bronze.rss_google_news import run

    logger = get_run_logger()
    referentiel = _load_referentiel()
    df = run(referentiel)
    matched = df["isin"].notna().sum()
    logger.info("google_news_rss_flow complete — %d articles, %d matched", len(df), matched)


@flow(name="bronze-rss-daily")
def bronze_rss_daily() -> None:
    """Orchestrator — runs both RSS ingestion flows sequentially."""
    yahoo_rss_flow()
    google_news_rss_flow()


if __name__ == "__main__":
    bronze_rss_daily()
