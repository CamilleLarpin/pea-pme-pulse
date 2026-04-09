import numpy as np
import pandas as pd


def compute_insider_score(
    df: pd.DataFrame, cluster_bonus_weight: float = 0.1, max_log_amount: float = 7.0
) -> pd.DataFrame:
    """
    Python mirror of dbt model: models/gold/score_insider.sql

    Parameters:
    -----------
    df : pd.DataFrame
        Input from Silver layer (amf_insider_signals).
        Required columns: isin, societe, date_signal, dirigeant,
                          source_record_id, montant, type_operation
    cluster_bonus_weight : float
        Weight for multiple operations (from dbt_project.yml vars)
    max_log_amount : float
        Normalization factor (from dbt_project.yml vars)
    """

    # Filter for "Achat" operations only, as per the original SQL logic
    df_buys = df[df["type_operation"] == "Achat"].copy()

    # Ensure signal_date is treated as a date (cast(date_signal as date))
    df_buys["signal_date"] = pd.to_datetime(df_buys["date_signal"]).dt.date

    # Daily aggregation by ISIN, Company, and Date
    grouped = df_buys.groupby(["isin", "societe", "signal_date"])

    daily_agg = grouped.agg(
        insider_names=("dirigeant", lambda x: ", ".join(x.unique())),
        num_operations=("source_record_id", "nunique"),
        total_amount=("montant", "sum"),
    ).reset_index()

    # Score Calculation: log10(sum + 1) * (1 + (count - 1) * cluster_bonus_weight)
    daily_agg["raw_score"] = np.log10(daily_agg["total_amount"] + 1) * (
        1 + (daily_agg["num_operations"] - 1) * cluster_bonus_weight
    )

    # Normalization to [1.0, 10.0] (normalized)
    # Formule: least(10.0, greatest(1.0, round((raw / max) * 10, 1)))
    daily_agg["score_1_10"] = ((daily_agg["raw_score"] / max_log_amount) * 10).round(1)

    # Apply clamping (SQL least/greatest equivalent)
    daily_agg["score_1_10"] = daily_agg["score_1_10"].clip(lower=1.0, upper=10.0)

    return daily_agg
