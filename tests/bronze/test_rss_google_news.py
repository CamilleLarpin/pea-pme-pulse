"""Tests — Bronze · Google News RSS ingestion."""

import pandas as pd
import pytest

from bronze.rss_google_news import fetch_all_feeds, match_companies

REFERENTIEL = pd.DataFrame([
    {"ticker_bourso": "1rPTHEP", "name": "THERMADOR", "isin": "FR0013333432"},
    {"ticker_bourso": "1rPMDM", "name": "MAISONS DU MONDE", "isin": "FR0013153541"},
    {"ticker_bourso": "1rPVK", "name": "VALLOUREC", "isin": "FR0013506730"},
    {"ticker_bourso": "1rPFII", "name": "LISI", "isin": "FR0000050353"},
    {"ticker_bourso": "1rPALCLA", "name": "CLARANOVA", "isin": "FR0013426004"},
])

REQUIRED_COLS = [
    "feed_name", "title", "link", "published", "summary", "fetched_at",
    "matched_name", "match_score", "isin", "ticker_bourso",
]


def _entry(title: str, feed_name: str = "euronext_growth") -> dict:
    return {
        "feed_name": feed_name,
        "title": title,
        "link": "",
        "published": "",
        "summary": "",
        "fetched_at": "",
    }


def test_exact_match():
    df = match_companies([_entry("THERMADOR annonce ses résultats")], REFERENTIEL)
    assert df.iloc[0]["isin"] == "FR0013333432"
    assert df.iloc[0]["match_score"] >= 80


def test_partial_match():
    df = match_companies([_entry("Maisons du Monde publie son chiffre d'affaires")], REFERENTIEL)
    assert df.iloc[0]["isin"] == "FR0013153541"


def test_no_match_below_threshold():
    df = match_companies([_entry("Résultats d'une société inconnue XYZ Corp")], REFERENTIEL)
    assert pd.isna(df.iloc[0]["isin"])
    assert pd.isna(df.iloc[0]["match_score"])


def test_output_schema():
    df = match_companies([_entry("VALLOUREC signe un contrat")], REFERENTIEL)
    for col in REQUIRED_COLS:
        assert col in df.columns


def test_unmatched_entries_kept():
    entries = [
        _entry("THERMADOR résultats semestriels"),
        _entry("Aucune société connue ici 12345"),
    ]
    df = match_companies(entries, REFERENTIEL)
    assert len(df) == 2
    assert pd.isna(df.iloc[1]["isin"])


def test_multiple_matches():
    entries = [
        _entry("CLARANOVA nouveau produit", feed_name="euronext_growth"),
        _entry("LISI acquisition stratégique", feed_name="pme_bourse_fr"),
    ]
    df = match_companies(entries, REFERENTIEL)
    assert df.iloc[0]["isin"] == "FR0013426004"
    assert df.iloc[1]["isin"] == "FR0000050353"


def test_feed_name_preserved():
    entries = [
        _entry("THERMADOR résultats", feed_name="euronext_growth"),
        _entry("CLARANOVA lève des fonds", feed_name="pme_bourse_fr"),
    ]
    df = match_companies(entries, REFERENTIEL)
    assert df.iloc[0]["feed_name"] == "euronext_growth"
    assert df.iloc[1]["feed_name"] == "pme_bourse_fr"


def test_short_name_no_false_positive():
    df = match_companies([_entry("Analyse des marchés financiers mondiaux")], REFERENTIEL)
    assert pd.isna(df.iloc[0]["isin"])


def test_short_name_exact_match():
    df = match_companies([_entry("LISI annonce une acquisition majeure")], REFERENTIEL)
    assert df.iloc[0]["isin"] == "FR0000050353"


def test_deduplication(monkeypatch):
    """Entries with the same title from two feeds should appear only once."""
    duplicate_title = "THERMADOR résultats record"
    feeds_mock = {
        "euronext_growth": "url_a",
        "pme_bourse_fr": "url_b",
    }

    def mock_fetch_feed(feed_name, url):
        return [_entry(duplicate_title, feed_name=feed_name)]

    monkeypatch.setattr("bronze.rss_google_news.fetch_feed", mock_fetch_feed)
    entries = fetch_all_feeds(feeds_mock)
    assert len(entries) == 1
    assert entries[0]["title"] == duplicate_title
