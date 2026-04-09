# src/gold/stocks_scorer.py
#
# Stocks Scorer — Technical indicators → score_technique [0, 10]
# Source  : silver.yahoo_ohlcv (RSI_14, MACD, MACD_signal, BB_upper, BB_lower,
#                                SMA_50, SMA_200, EMA_20, Close)
# Output  : same DataFrame + rsi_signal, macd_signal, golden_cross_signal,
#           bollinger_signal, trend_signal, score_technique, score_7d_avg
#
# Scoring rules (5 signals × 0/1/2 pts → total [0, 10]):
#   rsi_signal        : RSI_14 < 35 → 2 | 35–65 → 1 | ≥ 65 → 0 | NaN → 1
#   macd_signal       : MACD > MACD_signal → 2 | ≤ → 0 | NaN → 1
#   golden_cross      : SMA_50 > SMA_200 → 2 | ≤ → 0 | NaN → 1
#   bollinger_signal  : %B < 0.2 → 2 | 0.2–0.8 → 1 | %B > 0.8 → 0 | NaN → 1
#   trend_signal      : Close > EMA_20 → 2 | ≤ → 0 | NaN → 1
#
# Mirror of dbt/models/gold/stocks_score.sql — kept in sync for unit testing.

import pandas as pd


def compute_stocks_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les signaux techniques et le score d'attractivité [0, 10].

    Le DataFrame doit être trié par Date ASC et ne contenir qu'un seul ISIN
    (ou être groupé par ISIN avant appel) pour que score_7d_avg soit correct.

    Paramètres
    ----------
    df : pd.DataFrame
        Colonnes requises : Close, RSI_14, MACD, MACD_signal,
        BB_upper, BB_lower, SMA_50, SMA_200, EMA_20

    Retourne
    --------
    pd.DataFrame
        DataFrame original enrichi des colonnes de scoring.
    """
    df = df.copy()

    # --- RSI signal ---
    df["rsi_signal"] = 1.0  # neutre par défaut (NaN)
    df.loc[df["RSI_14"] < 35, "rsi_signal"] = 2.0
    df.loc[(df["RSI_14"] >= 35) & (df["RSI_14"] < 65), "rsi_signal"] = 1.0
    df.loc[df["RSI_14"] >= 65, "rsi_signal"] = 0.0

    # --- MACD signal ---
    df["macd_signal"] = 1.0  # NaN → neutre
    macd_valid = df["MACD"].notna() & df["MACD_signal"].notna()
    df.loc[macd_valid & (df["MACD"] > df["MACD_signal"]), "macd_signal"] = 2.0
    df.loc[macd_valid & (df["MACD"] <= df["MACD_signal"]), "macd_signal"] = 0.0

    # --- Golden Cross (SMA_50 vs SMA_200) ---
    df["golden_cross_signal"] = 1.0  # NaN → neutre
    gc_valid = df["SMA_50"].notna() & df["SMA_200"].notna()
    df.loc[gc_valid & (df["SMA_50"] > df["SMA_200"]), "golden_cross_signal"] = 2.0
    df.loc[gc_valid & (df["SMA_50"] <= df["SMA_200"]), "golden_cross_signal"] = 0.0

    # --- Bollinger signal (%B = position relative dans la bande) ---
    df["bollinger_signal"] = 1.0  # NaN or in bands → neutre
    bb_valid = df["BB_lower"].notna() & df["BB_upper"].notna()
    band_width = df["BB_upper"] - df["BB_lower"]
    pct_b = (df["Close"] - df["BB_lower"]) / band_width
    df.loc[bb_valid & (pct_b < 0.2), "bollinger_signal"] = 2.0
    df.loc[bb_valid & (pct_b > 0.8), "bollinger_signal"] = 0.0

    # --- Trend signal (EMA_20) ---
    df["trend_signal"] = 1.0  # NaN → neutre
    ema_valid = df["EMA_20"].notna()
    df.loc[ema_valid & (df["Close"] > df["EMA_20"]), "trend_signal"] = 2.0
    df.loc[ema_valid & (df["Close"] <= df["EMA_20"]), "trend_signal"] = 0.0

    # --- Composite score ---
    df["score_technique"] = (
        df["rsi_signal"]
        + df["macd_signal"]
        + df["golden_cross_signal"]
        + df["bollinger_signal"]
        + df["trend_signal"]
    ).round(1)

    # --- 7-day rolling average (requires Date-sorted DataFrame per ISIN) ---
    df["score_7d_avg"] = df["score_technique"].rolling(window=7, min_periods=1).mean().round(1)

    return df
