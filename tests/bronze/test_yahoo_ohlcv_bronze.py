"""Tests — Bronze · yahoo_ohlcv_bronze.py"""

import json

from bronze.yahoo_ohlcv_bronze import isin_to_yf_ticker, load_overrides, load_referentiel

# ---------------------------------------------------------------------------
# load_referentiel
# ---------------------------------------------------------------------------


def test_load_referentiel_valid(tmp_path):
    """Charge un CSV valide et retourne les colonnes name + isin."""
    csv = tmp_path / "ref.csv"
    csv.write_text(
        "name,ticker_bourso,isin\nCOMPANY A,1rPA,FR0010557264\nCOMPANY B,1rPB,FR0004040608\n"
    )
    df = load_referentiel(csv)
    assert list(df.columns) == ["name", "isin"]
    assert len(df) == 2


def test_load_referentiel_filters_nan_isin(tmp_path):
    """Les lignes avec ISIN NaN sont exclues."""
    csv = tmp_path / "ref.csv"
    csv.write_text("name,ticker_bourso,isin\nCOMPANY A,1rPA,FR0010557264\nCOMPANY B,1rPB,\n")
    df = load_referentiel(csv)
    assert len(df) == 1
    assert df.iloc[0]["isin"] == "FR0010557264"


def test_load_referentiel_filters_invalid_isin_length(tmp_path):
    """Les ISIN de longueur != 12 sont exclus."""
    csv = tmp_path / "ref.csv"
    csv.write_text(
        "name,ticker_bourso,isin\n"
        "COMPANY A,1rPA,FR0010557264\n"
        "COMPANY B,1rPB,TOOSHORT\n"
        "COMPANY C,1rPC,TOOLONGXXXXXXXXXXXXX\n"
    )
    df = load_referentiel(csv)
    assert len(df) == 1
    assert df.iloc[0]["isin"] == "FR0010557264"


def test_load_referentiel_deduplicates_isin(tmp_path):
    """Les doublons ISIN sont dédupliqués — une seule ligne par ISIN."""
    csv = tmp_path / "ref.csv"
    csv.write_text(
        "name,ticker_bourso,isin\nCOMPANY A,1rPA,FR0010557264\nCOMPANY A BIS,1rPABIS,FR0010557264\n"
    )
    df = load_referentiel(csv)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# load_overrides
# ---------------------------------------------------------------------------


def test_load_overrides_valid(tmp_path):
    """Charge un JSON valide et retourne le dict ISIN → ticker."""
    overrides_file = tmp_path / "overrides.json"
    overrides_file.write_text(json.dumps({"FR0013295789": "TFF.PA"}))
    result = load_overrides(overrides_file)
    assert result == {"FR0013295789": "TFF.PA"}


def test_load_overrides_missing_file(tmp_path):
    """Retourne un dict vide si le fichier est absent."""
    result = load_overrides(tmp_path / "nonexistent.json")
    assert result == {}


# ---------------------------------------------------------------------------
# isin_to_yf_ticker
# ---------------------------------------------------------------------------


def test_isin_to_yf_ticker_override(monkeypatch):
    """L'override manuel est retourné sans appel réseau."""
    overrides = {"FR0013295789": "TFF.PA"}
    result = isin_to_yf_ticker("FR0013295789", overrides)
    assert result == "TFF.PA"


def test_isin_to_yf_ticker_yahoo_resolution(monkeypatch):
    """Si pas d'override, résout via Yahoo Finance (mocké)."""
    monkeypatch.setattr(
        "bronze.yahoo_ohlcv_bronze.yf.Ticker",
        lambda isin: type("T", (), {"info": {"symbol": "OVH.PA"}})(),
    )
    result = isin_to_yf_ticker("FR0014005HJ9", {})
    assert result == "OVH.PA"


def test_isin_to_yf_ticker_unknown_isin(monkeypatch):
    """Retourne None si Yahoo ne reconnaît pas l'ISIN."""
    monkeypatch.setattr(
        "bronze.yahoo_ohlcv_bronze.yf.Ticker",
        lambda isin: type("T", (), {"info": {}})(),
    )
    result = isin_to_yf_ticker("FR0000000000", {})
    assert result is None


def test_isin_to_yf_ticker_yahoo_exception(monkeypatch):
    """Retourne None si Yahoo lève une exception."""

    def raise_exc(isin):
        raise ValueError("Invalid ISIN")

    monkeypatch.setattr("bronze.yahoo_ohlcv_bronze.yf.Ticker", raise_exc)
    result = isin_to_yf_ticker("FR0013231180", {})
    assert result is None
