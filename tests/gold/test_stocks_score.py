"""Tests unitaires — gold.stocks_scorer.compute_stocks_score()

Valide la logique de scoring technique sans connexion BigQuery.
DataFrames mockés avec des valeurs contrôlées pour chaque signal.
"""

import numpy as np
import pandas as pd
import pytest

from gold.stocks_scorer import compute_stocks_score


def _make_row(**kwargs) -> pd.DataFrame:
    """Crée un DataFrame d'une ligne avec des valeurs par défaut neutres."""
    defaults = {
        "Close": 100.0,
        "RSI_14": 50.0,       # neutre
        "MACD": 0.0,
        "MACD_signal": 0.0,   # égalité → baissier (0)
        "BB_upper": 110.0,
        "BB_lower": 90.0,
        "SMA_50": 100.0,
        "SMA_200": 100.0,     # égalité → baissier (0)
        "EMA_20": 100.0,      # égalité → baissier (0)
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


class TestRsiSignal:
    def test_oversold_gives_2(self):
        df = compute_stocks_score(_make_row(RSI_14=25.0))
        assert df["rsi_signal"].iloc[0] == 2.0

    def test_neutral_gives_1(self):
        df = compute_stocks_score(_make_row(RSI_14=50.0))
        assert df["rsi_signal"].iloc[0] == 1.0

    def test_overbought_gives_0(self):
        df = compute_stocks_score(_make_row(RSI_14=75.0))
        assert df["rsi_signal"].iloc[0] == 0.0

    def test_nan_gives_1(self):
        df = compute_stocks_score(_make_row(RSI_14=float("nan")))
        assert df["rsi_signal"].iloc[0] == 1.0

    def test_boundary_30_is_neutral(self):
        df = compute_stocks_score(_make_row(RSI_14=30.0))
        assert df["rsi_signal"].iloc[0] == 1.0

    def test_boundary_70_is_bearish(self):
        df = compute_stocks_score(_make_row(RSI_14=70.0))
        assert df["rsi_signal"].iloc[0] == 0.0


class TestMacdSignal:
    def test_macd_above_signal_gives_2(self):
        df = compute_stocks_score(_make_row(MACD=1.0, MACD_signal=0.5))
        assert df["macd_signal"].iloc[0] == 2.0

    def test_macd_below_signal_gives_0(self):
        df = compute_stocks_score(_make_row(MACD=-0.5, MACD_signal=0.5))
        assert df["macd_signal"].iloc[0] == 0.0

    def test_macd_nan_gives_1(self):
        df = compute_stocks_score(_make_row(MACD=float("nan")))
        assert df["macd_signal"].iloc[0] == 1.0


class TestGoldenCross:
    def test_golden_cross_gives_2(self):
        df = compute_stocks_score(_make_row(SMA_50=110.0, SMA_200=100.0))
        assert df["golden_cross_signal"].iloc[0] == 2.0

    def test_death_cross_gives_0(self):
        df = compute_stocks_score(_make_row(SMA_50=90.0, SMA_200=100.0))
        assert df["golden_cross_signal"].iloc[0] == 0.0

    def test_sma_200_nan_gives_1(self):
        df = compute_stocks_score(_make_row(SMA_200=float("nan")))
        assert df["golden_cross_signal"].iloc[0] == 1.0


class TestBollingerSignal:
    def test_below_lower_band_gives_2(self):
        df = compute_stocks_score(_make_row(Close=85.0, BB_lower=90.0, BB_upper=110.0))
        assert df["bollinger_signal"].iloc[0] == 2.0

    def test_above_upper_band_gives_0(self):
        df = compute_stocks_score(_make_row(Close=115.0, BB_lower=90.0, BB_upper=110.0))
        assert df["bollinger_signal"].iloc[0] == 0.0

    def test_inside_bands_gives_1(self):
        df = compute_stocks_score(_make_row(Close=100.0, BB_lower=90.0, BB_upper=110.0))
        assert df["bollinger_signal"].iloc[0] == 1.0

    def test_bb_nan_gives_1(self):
        df = compute_stocks_score(_make_row(BB_lower=float("nan"), BB_upper=float("nan")))
        assert df["bollinger_signal"].iloc[0] == 1.0


class TestTrendSignal:
    def test_above_ema_gives_2(self):
        df = compute_stocks_score(_make_row(Close=105.0, EMA_20=100.0))
        assert df["trend_signal"].iloc[0] == 2.0

    def test_below_ema_gives_0(self):
        df = compute_stocks_score(_make_row(Close=95.0, EMA_20=100.0))
        assert df["trend_signal"].iloc[0] == 0.0

    def test_ema_nan_gives_1(self):
        df = compute_stocks_score(_make_row(EMA_20=float("nan")))
        assert df["trend_signal"].iloc[0] == 1.0


class TestCompositeScore:
    def test_perfect_bullish_score_is_10(self):
        df = compute_stocks_score(_make_row(
            RSI_14=25.0,        # +2
            MACD=1.0, MACD_signal=0.0,  # +2
            SMA_50=110.0, SMA_200=100.0,  # +2
            Close=85.0, BB_lower=90.0, BB_upper=110.0,  # +2
            EMA_20=80.0,        # +2
        ))
        assert df["score_technique"].iloc[0] == 10.0

    def test_perfect_bearish_score_is_0(self):
        df = compute_stocks_score(_make_row(
            RSI_14=75.0,        # +0
            MACD=-1.0, MACD_signal=0.0,  # +0
            SMA_50=90.0, SMA_200=100.0,  # +0
            Close=115.0, BB_lower=90.0, BB_upper=110.0,  # +0
            EMA_20=120.0,       # +0
        ))
        assert df["score_technique"].iloc[0] == 0.0

    def test_all_nan_gives_5(self):
        df = compute_stocks_score(_make_row(
            RSI_14=float("nan"),
            MACD=float("nan"),
            MACD_signal=float("nan"),
            SMA_50=float("nan"),
            SMA_200=float("nan"),
            BB_lower=float("nan"),
            BB_upper=float("nan"),
            EMA_20=float("nan"),
        ))
        assert df["score_technique"].iloc[0] == 5.0

    def test_score_bounds_random(self):
        rng = np.random.default_rng(42)
        n = 500
        df_input = pd.DataFrame({
            "Close": rng.uniform(50, 200, n),
            "RSI_14": rng.uniform(0, 100, n),
            "MACD": rng.uniform(-5, 5, n),
            "MACD_signal": rng.uniform(-5, 5, n),
            "BB_upper": rng.uniform(150, 200, n),
            "BB_lower": rng.uniform(50, 100, n),
            "SMA_50": rng.uniform(80, 150, n),
            "SMA_200": rng.uniform(80, 150, n),
            "EMA_20": rng.uniform(80, 150, n),
        })
        result = compute_stocks_score(df_input)
        assert (result["score_technique"] >= 0).all()
        assert (result["score_technique"] <= 10).all()


class TestScore7dAvg:
    def test_7d_avg_single_row(self):
        df = compute_stocks_score(_make_row(
            RSI_14=25.0, MACD=1.0, MACD_signal=0.0,
            SMA_50=110.0, SMA_200=100.0,
            Close=85.0, BB_lower=90.0, BB_upper=110.0,
            EMA_20=80.0,
        ))
        # Single row: score_7d_avg == score_technique
        assert df["score_7d_avg"].iloc[0] == df["score_technique"].iloc[0]

    def test_7d_avg_rolling(self):
        # 7 identical rows → avg == score
        rows = [_make_row(RSI_14=25.0, MACD=1.0, MACD_signal=0.0,
                          SMA_50=110.0, SMA_200=100.0,
                          Close=85.0, BB_lower=90.0, BB_upper=110.0,
                          EMA_20=80.0) for _ in range(7)]
        df = pd.concat(rows, ignore_index=True)
        result = compute_stocks_score(df)
        assert (result["score_7d_avg"] == 10.0).all()
