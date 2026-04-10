import os
import re
from datetime import date, datetime
from functools import lru_cache

from fastapi import FastAPI, HTTPException, Query
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
    score_7d_avg: float | None
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
    score_7d_avg: float | None


class CompanyScore(BaseModel):
    isin: str
    name: str
    ticker_bourso: str | None
    score_date: date
    composite_score: float
    score_news: float
    score_stock: float
    score_insider: float
    score_financials: float


class NewsScore(BaseModel):
    isin: str
    matched_name: str
    ticker_bourso: str | None
    mention_count_45d: int
    avg_sentiment_45d: float
    investment_score: int
    score_date: date


class InsiderScore(BaseModel):
    isin: str
    societe: str
    signal_date: date
    insider_names: str
    num_operations: int
    total_amount: float
    score_1_10: float


class FinancialsScore(BaseModel):
    ticker: str
    isin: str | None
    date_cloture_exercice: date
    score_fondamental: float
    ca_growth_pct: float | None
    marge_op_pct: float | None
    levier_dette_ebitda: float | None
    fcf_yield_pct: float | None
    nb_metrics: int
    coverage_score: float


class ArticleSentiment(BaseModel):
    isin: str
    matched_name: str
    title: str
    published_at: datetime
    sentiment_score: float
    sentiment_reason: str


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
            score_7d_avg,
            rsi_signal,
            macd_signal,
            golden_cross_signal,
            bollinger_signal,
            trend_signal
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.stocks_score`
        WHERE is_latest = TRUE
        ORDER BY score_technique DESC, score_7d_avg DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/stocks-score/history", response_model=list[ScoreHistory])
def get_score_history(
    isins: list[str] = Query(..., description="Liste d'ISINs"),  # noqa: B008
    days: int = Query(default=30, ge=1, le=365, description="Nombre de jours d'historique"),  # noqa: B008
):
    """Historique du score technique pour une liste d'ISINs."""
    isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
    invalid = [i for i in isins if not isin_re.match(i)]
    if invalid:
        raise HTTPException(status_code=422, detail=f"ISINs invalides : {invalid}")
    client = get_bq_client()
    isin_list = ", ".join(f"'{i}'" for i in isins)
    query = f"""
        SELECT isin, company_name, date, score_technique, score_7d_avg
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.stocks_score`
        WHERE isin IN ({isin_list})
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY isin, date
    """
    return [dict(row) for row in client.query(query).result()]


# ── Composite score endpoints ─────────────────────────────────────────────────


@app.get("/gold/company-scores/latest", response_model=list[CompanyScore])
def get_company_scores():
    """Scores composites du jour pour toutes les PME."""
    client = get_bq_client()
    query = f"""
        SELECT isin, name, ticker_bourso, score_date,
               composite_score, score_news, score_stock, score_insider, score_financials
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.company_scores`
        WHERE score_date = (SELECT MAX(score_date) FROM `{GCP_PROJECT}.{GOLD_DATASET}.company_scores`)
        ORDER BY composite_score DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/score-news/latest", response_model=list[NewsScore])
def get_news_scores():
    """Scores actualités du jour (45j glissants)."""
    client = get_bq_client()
    query = f"""
        SELECT isin, matched_name, ticker_bourso,
               mention_count_45d, avg_sentiment_45d, investment_score, score_date
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.score_news`
        WHERE score_date = (SELECT MAX(score_date) FROM `{GCP_PROJECT}.{GOLD_DATASET}.score_news`)
        ORDER BY investment_score DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/score-insider/latest", response_model=list[InsiderScore])
def get_insider_scores():
    """Signaux insiders AMF des 45 derniers jours."""
    client = get_bq_client()
    query = f"""
        SELECT isin, societe, signal_date, insider_names,
               num_operations, total_amount, score_1_10
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.score_insider`
        WHERE signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
        ORDER BY signal_date DESC, score_1_10 DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/financials-score/latest", response_model=list[FinancialsScore])
def get_financials_scores():
    """Score fondamental le plus récent par entreprise."""
    client = get_bq_client()
    query = f"""
        SELECT ticker, isin, date_cloture_exercice, score_fondamental,
               ca_growth_pct, marge_op_pct, levier_dette_ebitda, fcf_yield_pct,
               nb_metrics, coverage_score
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.financials_score`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY isin ORDER BY date_cloture_exercice DESC) = 1
        ORDER BY score_fondamental DESC
    """
    return [dict(row) for row in client.query(query).result()]


@app.get("/gold/article-sentiment/latest", response_model=list[ArticleSentiment])
def get_article_sentiments():
    """Articles avec sentiment des 45 derniers jours (max 500)."""
    client = get_bq_client()
    query = f"""
        SELECT isin, matched_name, title, published_at, sentiment_score, sentiment_reason
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.article_sentiment`
        WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
        ORDER BY published_at DESC
        LIMIT 500
    """
    return [dict(row) for row in client.query(query).result()]
