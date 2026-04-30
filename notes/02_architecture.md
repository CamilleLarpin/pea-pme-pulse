# 02 — Architecture

## Architecture Medallion (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCES EXTERNES                                               │
│  yfinance · AMF API · RSS feeds · Boursorama                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Prefect @task
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE (BigQuery dataset `bronze`)                             │
│  Données brutes, minimalement transformées                      │
│  yfinance_ohlcv · yahoo_rss · google_news_rss · abcbourse_rss  │
│  amf · boursorama                                               │
│                                                                 │
│  GCS (backup audit) : RSS dumps JSON, AMF PDFs/XMLs/JSONL      │
└──────────────────────┬──────────────────────────────────────────┘
                       │ dbt models + Python (compute_silver)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER (BigQuery dataset `silver`)                             │
│  Nettoyé, dédupliqué, enrichi d'indicateurs                     │
│  rss_articles · yahoo_ohlcv_clean · yahoo_ohlcv                 │
│  companies · amf_insider_signals · amf_financial_signal         │
└──────────────────────┬──────────────────────────────────────────┘
                       │ dbt models + Groq LLM scoring
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD (BigQuery dataset `gold`)                                 │
│  Prêt pour la décision, scoré, agrégé                           │
│  stocks_score · article_sentiment · score_news                  │
│  score_insider · financials_score · company_scores              │
└──────────────────────┬──────────────────────────────────────────┘
                       │ FastAPI
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  EXPOSITION                                                     │
│  FastAPI (11 endpoints) · Streamlit (4 onglets) · Grafana       │
└─────────────────────────────────────────────────────────────────┘
```

### Règles d'architecture (non négociables)

1. **Le Gold n'est jamais écrit par des flows d'ingestion** — uniquement dbt ou scoring Python
2. **Bronze et Gold ne communiquent jamais directement** — toujours via Silver
3. **La déduplication se fait en Silver**, pas en Bronze
4. **GCS = audit/backup** pour les données non-retchables (PDFs AMF, snapshots RSS). yfinance est re-fetchable → direct BQ, pas GCS

---

## Conventions de code

### Structure des modules

```
src/
├── bronze/      # Modules Python purs — AUCUN import Prefect
│   └── yahoo_ohlcv_bronze.py, rss_yahoo_fr.py, ...
├── silver/      # Modules Python purs — AUCUN import Prefect
│   └── compute_silver.py, ...
├── gold/        # Modules Python purs — AUCUN import Prefect
│   └── stocks_scorer.py, sentiment_scorer.py, ...
├── flows/       # Wrappers Prefect @flow + @task
│   └── bronze_<source>.py, silver_<source>.py, ...
├── api/         # FastAPI application
└── dashboard/   # Streamlit application

dbt/models/
├── silver/      # Nettoyage, dédup
└── gold/        # Scoring, agrégation

referentiel/     # Données statiques (571 sociétés CSV)
tests/           # Mirrors src/ (bronze/, silver/, gold/)
```

### Conventions de nommage

| Règle | Exemple |
|---|---|
| 1 source = 1 flow file | `bronze_yahoo_rss.py` |
| Convention flow : `bronze_<source>` | `bronze-yahoo-rss` |
| Modules source Prefect-free | `src/bronze/rss_yahoo_fr.py` |
| Wrappers Prefect dans `flows/` | `src/flows/bronze_yahoo_rss.py` |

### dbt

- **Matérialisation :** toutes les tables sont `materialized='table'` (pas view)
- **Schema Silver :** `+schema: silver`
- **Schema Gold :** `+schema: gold`
- **Docs :** `persist_docs` activé sur relations et colonnes
- **Documentation :** 100% couverture colonnes via `{{ doc('...') }}` dans `definitions.md`
- **Tests :** `not_null`, `unique`, `accepted_values` dans `schema.yml`

---

## Infrastructure de déploiement

```
VM GCP e2-small (35.241.252.5)
├── nginx (Docker, port 80) — Reverse proxy + auth
├── prefect-server (Docker, port 4200) — Orchestrateur
├── prefect-worker (Docker) — Exécuteur de flows
├── grafana (Docker, port 3000) — Dashboards monitoring
├── prometheus (Docker, port 9090) — Collecte métriques
├── node-exporter (Docker, port 9100) — Métriques VM
└── pea-pme-api (systemd, port 8000) — FastAPI

BigQuery (EU)
├── bronze.*   — Données brutes
├── silver.*   — Données nettoyées
├── gold.*     — Données scorées
└── work.*     — Tables staging temporaires

GCS bucket : project-pea-pme
├── rss_yahoo/          — Dumps RSS Yahoo (JSON horodatés)
├── rss_google_news/    — Dumps RSS Google News (JSON horodatés)
├── amf/                — PDFs, XMLs, JSONL AMF
└── boursorama_peapme_final.csv — Référentiel mensuel
```

---

## Tech Stack détaillé (versions réelles depuis pyproject.toml)

| Outil | Version spec | Rôle |
|---|---|---|
| Python | >=3.10, <4.0 | Langage principal |
| prefect | >=3.1.0, <4.0.0 | Orchestration |
| prefect-gcp | >=0.6.2, <1.0.0 | Intégration GCP |
| dbt-bigquery | — | Transformations SQL |
| google-cloud-bigquery | — | Client BQ |
| google-cloud-storage | — | Client GCS |
| google-api-core | >=2.30.1, <3.0.0 | APIs Google |
| yfinance | — | Données boursières |
| ta | — | Indicateurs techniques |
| feedparser | — | Parsing RSS/Atom |
| beautifulsoup4 + lxml | — | Scraping Web |
| rapidfuzz | — | Fuzzy matching noms sociétés |
| groq | — | Client API Groq (LLM) |
| pdfplumber | — | Extraction texte PDF |
| fastapi | — | API REST |
| uvicorn[standard] | — | Serveur ASGI |
| streamlit | >=1.56.0, <2.0.0 | Dashboard |
| plotly | >=6.6.0, <7.0.0 | Graphiques interactifs |
| prometheus-fastapi-instrumentator | >=0.9 | Métriques API |
| pandas + pandas-gbq | — | Manipulation données |
| loguru | — | Logging structuré |
| ruff | — | Linting + formatting |
| pytest + pytest-mock | >=3.15.1 | Tests |
