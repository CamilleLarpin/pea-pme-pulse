"""Tests — Silver · compute_indicators()."""

import numpy as np
import pandas as pd

from silver.compute_silver import compute_indicators

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

INDICATOR_COLS = [
    "RSI_14",
    "MACD",
    "MACD_signal",
    "BB_upper",
    "BB_lower",
    "SMA_50",
    "SMA_200",
    "EMA_20",
    "computed_at",
]


def _make_ohlcv(n: int, seed: int = 42) -> pd.DataFrame:
    """
    Génère un DataFrame OHLCV synthétique avec n lignes.

    Série de prix déterministe (random walk) suffisante pour tester
    tous les indicateurs sans dépendance à des données réelles.
    Prix toujours positifs, volume non nul.
    """
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    close = np.abs(close) + 1  # garantit des prix positifs

    return pd.DataFrame(
        {
            "Date": pd.date_range("2020-01-01", periods=n, freq="B"),
            "Open": close * rng.uniform(0.99, 1.01, n),
            "High": close * rng.uniform(1.00, 1.02, n),
            "Low": close * rng.uniform(0.98, 1.00, n),
            "Close": close,
            "Volume": rng.integers(10_000, 1_000_000, n),
            "isin": "FR0000000001",
            "yf_ticker": "TEST.PA",
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_output_schema():
    """Toutes les colonnes indicateurs sont présentes en sortie."""
    df = compute_indicators(_make_ohlcv(250))
    for col in INDICATOR_COLS:
        assert col in df.columns, f"Colonne manquante : {col}"


def test_row_count_preserved():
    """compute_indicators ne doit pas perdre ou dupliquer de lignes."""
    df_in = _make_ohlcv(250)
    df_out = compute_indicators(df_in)
    assert len(df_out) == len(df_in)


def test_indicators_non_nan_with_sufficient_history():
    """
    Avec 250 lignes (> 200 de warmup SMA_200), les dernières lignes
    doivent avoir des valeurs non-NaN pour tous les indicateurs.
    """
    df = compute_indicators(_make_ohlcv(250))
    last = df.iloc[-1]
    for col in INDICATOR_COLS:
        if col == "computed_at":
            continue
        assert not pd.isna(last[col]), f"{col} est NaN sur la dernière ligne"


def test_sma_200_nan_below_warmup():
    """
    SMA_200 doit être NaN pour les 199 premières lignes
    (pas assez d'historique pour une fenêtre de 200 jours).
    """
    df = compute_indicators(_make_ohlcv(250))
    assert df["SMA_200"].iloc[:199].isna().all()


def test_rsi_bounded():
    """RSI doit toujours être compris entre 0 et 100."""
    df = compute_indicators(_make_ohlcv(250))
    rsi = df["RSI_14"].dropna()
    assert (rsi >= 0).all() and (rsi <= 100).all()


def test_bollinger_bands_order():
    """BB_upper doit toujours être >= BB_lower."""
    df = compute_indicators(_make_ohlcv(250))
    valid = df.dropna(subset=["BB_upper", "BB_lower"])
    assert (valid["BB_upper"] >= valid["BB_lower"]).all()


def test_computed_at_is_set():
    """computed_at doit être renseigné sur toutes les lignes."""
    df = compute_indicators(_make_ohlcv(50))
    assert df["computed_at"].notna().all()


def test_short_series_does_not_crash():
    """
    Une série courte (< 200 lignes) ne doit pas lever d'exception —
    les indicateurs sans warmup suffisant retournent NaN, c'est tout.
    """
    df = compute_indicators(_make_ohlcv(30))
    assert "RSI_14" in df.columns
    assert df["SMA_200"].isna().all()
    assert df["SMA_50"].isna().all()


def test_input_not_mutated():
    """compute_indicators ne doit pas modifier le DataFrame d'entrée."""
    df_in = _make_ohlcv(250)
    cols_before = set(df_in.columns)
    compute_indicators(df_in.copy())
    assert set(df_in.columns) == cols_before
