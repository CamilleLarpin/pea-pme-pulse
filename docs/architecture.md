# Architecture & Naming Conventions

## Repository layout

```
src/
├── bronze/       # ingestion — one module per source (incl. boursorama)
├── silver/       # cleaning/parsing
├── gold/         # scoring & ranking
└── flows/        # Prefect flow definitions — one file per source (convention: bronze_<source>.py)
referentiel/      # static reference data files (CSV) — companies list used to match/join across sources
dbt/              # SQL transformation models running on BigQuery (Silver/Gold logic)
scripts/          # one-off dev/exploration scripts — never imported by pipeline code
tests/            # unit and integration tests — mirrors src/ structure (tests/bronze/, tests/silver/, tests/gold/)
docs/             # this folder
prefect.yaml      # Prefect deployment config
```

Pipeline code lives under `src/` · tooling and data files at root level.

> Note: `boursorama/` at root is temporary — the Boursorama scraper should move to `src/bronze/boursorama.py` to follow this layout.

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

### Conventions

- **1 source = 1 flow** — each developer owns their own flow file for their data source · file naming: `bronze_<source>.py`
- **@task decomposition** — each flow is split into `@task` steps: `fetch` → `dump_gcs` → `match_load_bq` · benefits: failed tasks are retried individually (no full re-run) · each step is visible with its own state and logs in the Prefect Cloud UI
- **Source modules are Prefect-free** — `src/bronze/` contains pure Python · `@task` wrappers live in `src/flows/` only

### Flows

| Flow | File | Schedule |
|---|---|---|
| `bronze-yahoo-rss` | `src/flows/bronze_yahoo_rss.py` | cron `0 0 * * *` Europe/Paris |
| `bronze-google-news-rss` | `src/flows/bronze_google_news_rss.py` | cron `0 0 * * *` Europe/Paris |

Workspace: `camille-larpin/pea-pme` on Prefect Cloud · work pool: `bronze-rss-pool` (Docker) · target: GCP e2-small

Deploy:
```bash
prefect work-pool create bronze-rss-pool --type docker
prefect deploy --all
prefect worker start --pool bronze-rss-pool   # on GCP e2-small
```

---

## Environment variable naming - TBD

| Variable | Example value |
|---|---|
| `GCP_PROJECT_ID` | `bootcamp-project-pea-pme` |
| `GCS_BUCKET_NAME` | `project-pea-pme` |


---

## Referentiel

Master referentiel: `referentiel/boursorama_peapme_final.csv` (569 companies)
Key columns: `nom`, `ticker_bourso`, `isin`
