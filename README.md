# pea-pme-pulse

Pipeline de scoring automatisé pour identifier les meilleures opportunités parmi les PME éligibles PEA-PME sur un horizon mid/long term (18+ mois).

**Output** : rapport quotidien classant les PME éligibles par score composite 0–10, prêt à alimenter une décision d'investissement. Bonus : portail Talk To My Data pour poser des questions d'analyse en langage naturel.

## Architecture

Médaillon Bronze → Silver → Gold · Sources 100% gratuites · Orchestration complète.

### Bronze — Ingestion

| Source | Type | Historique | Coût |
|---|---|---|---|
| yfinance + ta | Librairie Python | ✅ 20 ans | Gratuit |
| AMF flux-amf-new-prod | API REST JSON v2 | ✅ Complet | Gratuit, sans clé |
| ABCBourse RSS | Flux RSS XML | ❌ ~30 articles | Gratuit |
| Yahoo Finance FR RSS | Flux RSS XML | ❌ ~50 articles | Gratuit |

### Silver — Preprocessing

| Module | Source | LLM |
|---|---|---|
| `ohlc_cleaner` | yfinance | — |
| `pdf_parser` | AMF (PDF financiers) | Groq Llama 3.3 70B |
| `insider_parser` | AMF (transactions dirigeants) | — |
| `rss_cleaner` | ABCBourse + Yahoo FR RSS | Groq Llama 3.1 8B |

### Gold — Scoring

| Scorer | Signal | Poids |
|---|---|---|
| Stocks | Golden Cross, RSI, MACD, Bollinger → 0–10 | 35% |
| Financials | Croissance CA, marge, dette, FCF → 0–10 | 25% |
| Insider | Achats/ventes dirigeants sur 6 mois → 0–10 | 25% |
| News | Sentiment pondéré par fraîcheur → 0–10 | 15% |
| Final Score | TBC | TBD |

`score_composite = 0.35 × stocks + 0.25 × financials + 0.25 × insider + 0.15 × news`

## Stack

| Catégorie | Outil |
|---|---|
| Versionning & CI/CD | GitHub + GitHub Actions |
| Qualité code | Ruff + SQLFluff + pre-commit |
| Containerisation | Docker + docker-compose |
| Infrastructure | GCP VM e2-small + GCS |
| Stockage & transformation | BigQuery + dbt-bigquery |
| Orchestration | Prefect Cloud |
| Talk To My Data | nao (getnao.io) |
| LLM backend | Groq API |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
# Only if you don't have .env yet
cp -n .env.example .env
```
