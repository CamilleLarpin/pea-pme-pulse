import os
from datetime import date
from functools import lru_cache

from fastapi import FastAPI, Query
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI(title="pea-pme-pulse API", version="0.1.0")

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "bootcamp-project-pea-pme")
GOLD_DATASET = "gold"


@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


# ── Overview (PR #38 — Camille) ───────────────────────────────────────────────


@app.get("/overview")
def overview():
    """List all datasets in the BQ project with their tables."""
    client = get_bq_client()
    result = {}
    for dataset in client.list_datasets():
        tables = [t.table_id for t in client.list_tables(dataset.dataset_id)]
        result[dataset.dataset_id] = tables
    return {"project": GCP_PROJECT, "datasets": result}


@app.get("/overview/gold")
def overview_gold():
    """List tables in the gold dataset."""
    client = get_bq_client()
    tables = [t.table_id for t in client.list_tables("gold")]
    return {"project": GCP_PROJECT, "dataset": "gold", "tables": tables}


# ── Data models ───────────────────────────────────────────────────────────────


class StockScore(BaseModel):
    isin: str
    company_name: str
    yf_ticker: str
    date: date
    close: float
    score_technique: float
    rsi_signal: float
    macd_signal: float
    golden_cross_signal: float
    bollinger_signal: float
    trend_signal: float


class ScoreHistory(BaseModel):
    isin: str
    company_name: str
    date: date
    score_technique: float


# ── Stock score endpoints ─────────────────────────────────────────────────────


@app.get("/gold/stocks-score/latest", response_model=list[StockScore])
def get_latest_scores():
    """Scores techniques du jour pour toutes les PME (is_latest = TRUE)."""
    client = get_bq_client()
    query = f"""
        SELECT
            isin,
            company_name,
            yf_ticker,
            date,
            Close AS close,
            score_technique,
            rsi_signal,
            macd_signal,
            golden_cross_signal,
            bollinger_signal,
            trend_signal
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.stocks_score`
        WHERE is_latest = TRUE
        ORDER BY score_technique DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/stocks-score/history", response_model=list[ScoreHistory])
def get_score_history(
    isins: list[str] = Query(..., description="Liste d'ISINs"),  # noqa: B008
    days: int = Query(default=30, ge=1, le=365, description="Nombre de jours d'historique"),  # noqa: B008
):
    """Historique du score technique pour une liste d'ISINs."""
    client = get_bq_client()
    isin_list = ", ".join(f"'{i}'" for i in isins)
    query = f"""
        SELECT isin, company_name, date, score_technique
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.stocks_score`
        WHERE isin IN ({isin_list})
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY isin, date
    """
    return [dict(row) for row in client.query(query).result()]
