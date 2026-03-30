"""Tests — Bronze · Yahoo Finance FR RSS ingestion."""

import pandas as pd
import pytest

from bronze.rss_yahoo_fr import match_companies

REFERENTIEL = pd.DataFrame([
    {"ticker_bourso": "1rPTHEP", "nom": "THERMADOR", "isin": "FR0013333432"},
    {"ticker_bourso": "1rPMDM", "nom": "MAISONS DU MONDE", "isin": "FR0013153541"},
    {"ticker_bourso": "1rPVK", "nom": "VALLOUREC", "isin": "FR0013506730"},
    {"ticker_bourso": "1rPFII", "nom": "LISI", "isin": "FR0000050353"},
    {"ticker_bourso": "1rPALCLA", "nom": "CLARANOVA", "isin": "FR0013426004"},
])

REQUIRED_COLS = [
    "title", "link", "published", "summary", "fetched_at",
    "matched_nom", "match_score", "isin", "ticker_bourso",
]


def _entry(title: str) -> dict:
    return {"title": title, "link": "", "published": "", "summary": "", "fetched_at": ""}


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
        _entry("CLARANOVA nouveau produit"),
        _entry("LISI acquisition stratégique"),
    ]
    df = match_companies(entries, REFERENTIEL)
    assert df.iloc[0]["isin"] == "FR0013426004"
    assert df.iloc[1]["isin"] == "FR0000050353"
