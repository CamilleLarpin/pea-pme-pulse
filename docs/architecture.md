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
│   ├── google_news_rss
│   ├── amf
│   ├── abcbourse_rss
│   └── boursorama
├── silver
│   └── rss_articles        # unified RSS feed (all sources, deduplicated)
└── gold
    └── score_news          # 45-day mention count per ISIN → normalized 1–10 (planned)
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
| `bronze-yahoo-rss` | `src/flows/bronze_yahoo_rss.py` | cron `0 */4 * * *` Europe/Paris |
| `bronze-google-news-rss` | `src/flows/bronze_google_news_rss.py` | cron `0 */4 * * *` Europe/Paris |

Workspace: `camille-larpin/pea-pme` on Prefect Cloud · work pool: `bronze-pool` (Docker) · target: GCP e2-small

Deploy:
```bash
prefect work-pool create bronze-pool --type docker
prefect deploy --all
prefect worker start --pool bronze-pool   # on GCP e2-small
```

---

## Transformations — dbt

Silver and Gold logic lives in `dbt/models/` as SQL files. Bronze tables are declared as dbt sources — dbt does not own them.

### Conventions

- **Bronze → Silver**: cleaning only — dedup, type casting, timestamp parsing, column standardisation · no enrichment, no scoring
- **Silver → Gold**: enrichment and scoring — aggregations, LLM outputs, normalized scores
- **Materialization**: all models are `table` (not view) — Silver and Gold are rebuilt on each run
- **Schema routing**: `+schema: silver` / `+schema: gold` in `dbt_project.yml` writes directly to the matching BQ dataset · enforced by `macros/generate_schema_name.sql`
- **Sources**: always reference Bronze tables via `{{ source('bronze', 'table_name') }}` — never hardcode BQ paths in models
- **Tests**: every model has at minimum `not_null` on key columns · add `accepted_values` for enum columns

### Models

| Model | Dataset | Sources | Description |
|---|---|---|---|
| `rss_articles` | `silver` | `bronze.yahoo_rss`, `bronze.google_news_rss` | Unified RSS articles — deduplicated, timestamp parsed |
| `score_news` | `gold` | `silver.rss_articles` | 45-day mention count per ISIN → normalized 1–10 (planned) |

### Documentation convention

Every table in the pipeline must be fully documented. Each piece of information has exactly one home — no duplication across files.

| File | Owns |
|---|---|
| `dbt/models/definitions.md` | One definition per business concept (isin, sentiment_score, etc.) — all schema files reference these via `{{ doc('term') }}` · never define a concept inline |
| `dbt/models/sources.yml` | All external source declarations (tables dbt reads but does not build: Bronze + `gold.article_sentiment`) · column docs via `{{ doc() }}` · table-level grain, pipeline ownership, when-to-use, when-NOT-to-use |
| `dbt/models/silver/schema.yml` | Column docs for Silver dbt models only |
| `dbt/models/gold/schema.yml` | Column docs for Gold dbt models only |
| `dbt/semantic/nao_context.yml` | Agent-only additions: routing keywords, example questions, query patterns — nothing that belongs in schema files |

Rules:
- **100% column coverage** — every column in every table must have a description
- **Single definition** — if a concept already exists in `definitions.md`, reference it; never introduce a second definition
- **No cross-file duplication** — business context in `sources.yml`/`schema.yml`; routing hints in `nao_context.yml`; never both

### Local dev

```bash
cd dbt
dbt run        # rebuild all models
dbt test       # run all tests
dbt run --select silver.rss_articles   # run one model only
```

Production runs are triggered by Prefect after Bronze flows complete — not scheduled independently.

---

## Environment variable naming - TBD

| Variable | Example value |
|---|---|
| `GCP_PROJECT_ID` | `bootcamp-project-pea-pme` |
| `GCS_BUCKET_NAME` | `project-pea-pme` |


---

## Referentiel

Master referentiel: `referentiel/boursorama_peapme_final.csv` (571 companies)
Key columns: `name`, `ticker_bourso`, `isin`
