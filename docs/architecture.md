# Architecture & Naming Conventions

## Repository layout

```
src/
├── bronze/       # ingestion — one module per source (incl. boursorama)
├── silver/       # cleaning/parsing
├── gold/         # scoring & ranking
└── flows/        # Prefect flow definitions — one file per milestone/layer
referentiel/      # static reference data files (CSV) — companies list used to match/join across sources
dbt/              # SQL transformation models running on BigQuery (Silver/Gold logic)
scripts/          # one-off dev/exploration scripts — never imported by pipeline code
tests/            # unit and integration tests — mirrors src/ structure (tests/bronze/, tests/silver/, tests/gold/)
docs/             # this folder
prefect.yaml      # Prefect deployment config
```

Pipeline code lives under `src/` · tooling and data files at root level.

---

## BigQuery — medallion datasets

One dataset per layer, one table per source:

```
project-pea-pme
├── bronze
│   ├── yahoo_rss
│   ├── amf
│   ├── abcbourse_rss
│   └── boursorama
├── silver
│   └── ... (naming convention TBD)
└── gold
    └── ... (naming convention TBD)
```

---

## Google Cloud Storage

GCS stores raw data that cannot be re-fetched. Two rules:
- **RSS feeds** — ephemeral, no history → full raw feed dumped to GCS at fetch time, then filtered + matched entries loaded to BQ Bronze
- **AMF** — full history available via API → GCS for auditability of original regulatory documents (PDFs/XMLs/jsonl)
- **yfinance** — structured data fetched by ISIN, always re-fetchable → straight to BQ Bronze, no GCS step

```
gs://project_bucket/
├── rss_yahoo/         # raw feed dumps, timestamped
├── rss_google_news/   # raw feed dumps, timestamped
├── rss_abcbourse/
└── amf/               # AMF PDFs/XMLs/jsonl
```

---

## Orchestration — Prefect

Flows live under `src/flows/` · deployment config in `prefect.yaml`.

| Flow | File | Schedule |
|---|---|---|
| `bronze-yahoo-rss` | `src/flows/bronze_rss.py` | called by orchestrator |
| `bronze-google-news-rss` | `src/flows/bronze_rss.py` | called by orchestrator |
| `bronze-rss-daily` | `src/flows/bronze_rss.py` | cron `30 17 * * 1-5` (17:30 UTC = 18:30 CET, Mon–Fri) |

Work pool: `bronze-rss-pool` (process type) · target: GCP e2-small

Deploy:
```bash
prefect work-pool create bronze-rss-pool --type process
prefect deploy --name bronze-rss-daily
prefect worker start --pool bronze-rss-pool   # on GCP e2-small
```

---

## Environment variables

| Variable | Example value | Description |
|---|---|---|
| `GCP_PROJECT_ID` | `bootcamp-project-pea-pme` | GCP project |
| `GCS_BUCKET_NAME` | `project-pea-pme` | GCS bucket for raw dumps |
| `REFERENTIEL_PATH` | `referentiel/companies_draft.csv` | Override referentiel CSV · defaults to full referentiel (569 companies) · set to draft (5 companies) for local dev |


---

## Referentiel

Master referentiel: `referentiel/boursorama_peapme_final.csv` (569 companies)
Key columns: `name`, `ticker_bourso`, `isin`

Draft (5 companies for testing): `referentiel/companies_draft.csv`

---

## Conventions

### Logging
Use `loguru` across all pipeline modules (`src/`). Do not use `print()`.

```python
from loguru import logger

logger.info("BQ load: {} rows → {}", len(df), table_id)
logger.warning("No match found for ISIN: {}", isin)
```

`loguru` outputs structured logs with timestamps and levels — required for observability in production (Prefect on GCP).
