# Infrastructure & Components — pea-pme-pulse

> Who is this for: a Junior Data Engineer joining the project who wants to understand how all the
> pieces fit together — where code runs, how auth works, and how to operate the system.

---

## What the system does

Automated daily scoring of PEA-PME-eligible SMEs. It ingests financial data from multiple sources
(RSS feeds, market data, company referentiel), processes it through a Bronze → Silver → Gold
pipeline, and produces a composite investment score per company.

---

## Where code runs

There are two execution environments:

**Your laptop (local dev)**
Used for: writing code, running flows manually to test, deploying new configs, querying BigQuery.
Nothing in production depends on your laptop being on.

**GCP VM (`35.241.252.5`, zone `europe-west1-b`)**
This is where all scheduled production pipelines run. It runs 24/7 and hosts:
- the Prefect server (orchestration)
- the Prefect worker (flow execution)
- nginx (HTTP access control)

The VM is a GCP `e2-small` instance managed in the `bootcamp-project-pea-pme` GCP project.

---

## Data storage — BigQuery + GCS

All data lives in GCP, not on any machine.

**Google Cloud Storage (GCS)** — bucket `project-pea-pme`
Raw dumps and reference files. Flows write here first, before anything goes to BigQuery.
- `rss_yahoo/` — raw Yahoo RSS JSON dumps (timestamped)
- `rss_google_news/` — raw Google News RSS JSON dumps
- `boursorama_peapme_final.csv` — monthly company referentiel snapshot

**BigQuery** — project `bootcamp-project-pea-pme`, location EU
Structured tables, organized in three datasets (the "medallion" architecture):

| Dataset | Purpose | Example tables |
|---|---|---|
| `bronze` | Raw ingested data, minimal transformation | `yahoo_rss`, `google_news_rss`, `boursorama` |
| `silver` | Cleaned, deduplicated, unified | `rss_articles`, `companies`, `yahoo_ohlcv` |
| `gold` | Business-ready, scored, aggregated | `article_sentiment`, `score_news`, `company_scores`, `stocks_score` |

Data flows strictly forward: Bronze → Silver → Gold. Gold tables are never written by ingestion
flows — only by dbt or scoring steps.

---

## Orchestration — Prefect (self-hosted on the VM)

Prefect is the scheduler and executor. Think of it like a smart cron that knows about tasks,
retries, dependencies, and logs.

**Three concepts to understand:**

**Deployment** — a named, versioned flow config stored in the Prefect server. Defined in
`prefect.yaml`. Each deployment has a schedule (or is manual-only), an entrypoint (which Python
function to run), and optional environment variable injections.

**Worker** — a process running on the VM that watches for scheduled runs and executes them. It
clones the repo from GitHub (`main` branch), installs dependencies, and runs the flow.

**Flow** — the actual Python code (in `src/flows/`). Each flow is a `@flow`-decorated function
that calls `@task`-decorated steps in sequence.

```
Prefect server (VM) → schedule triggers → worker (VM) → git clone main → pip install → run flow
```

The server UI is at `http://35.241.252.5` (nginx basic auth, credentials in `.env`).

**Current deployments:**

| Deployment | Schedule | Trigger |
|---|---|---|
| `bronze-silver-rss` | Every 4h | cron `0 */4 * * *` |
| `silver-gold-rss` | None | triggered by `bronze-silver-rss` on completion |
| `yfinance-ohlcv-pipeline` | Weekdays 19h30 | cron `30 19 * * 1-5` |
| `bronze-abcbourse-rss` | Weekdays 08h | cron `0 8 * * 1-5` |
| `bronze-amf` | Every 4h | cron `0 */4 * * *` |
| `bronze-silver-gold-boursorama` | 1st of month 08h | cron `0 8 1 * *` |

---

## Transformation — dbt

dbt (data build tool) handles all SQL transformations: Silver cleaning and Gold aggregation.
It runs as a subprocess *inside* Prefect flows — not as a standalone scheduler.

The dbt project lives in `dbt/`. Models are `.sql` files in `dbt/models/{silver,gold}/`. dbt
reads from BigQuery and writes back to BigQuery (using `CREATE OR REPLACE TABLE` or `MERGE`
depending on the model strategy).

Each flow that touches Silver or Gold calls `dbt run --select <model>` at the end of its
Python work. The dbt profile (BigQuery connection config) is built dynamically in a temp dir at
runtime — there is no `~/.dbt/profiles.yml` on the VM.

---

## Auth — how the system proves its identity to GCP

This is the most important thing to understand: **no password or key file is stored on the VM**.

**ADC — Application Default Credentials**
The GCP VM has a Service Account attached at the OS level:
`pea-pme-ingestor@bootcamp-project-pea-pme.iam.gserviceaccount.com`

When any code running on the VM calls a GCP API (BigQuery, GCS), GCP automatically identifies
it as that service account — no config needed. This is called ADC (Application Default Credentials).

Locally, your own Google account plays the same role after running:
```bash
gcloud auth application-default login
```

**What this means in practice:**
- No JSON key files on the VM
- No `GOOGLE_APPLICATION_CREDENTIALS` env var needed in production flows
- If GCP access breaks, it's an IAM permissions issue on the SA, not a key rotation issue

**Historical exception — `silver-yfinance-ohlcv` (replaced)**
This flow was written before the VM migration and injected a JSON SA key via a Prefect Secret
block (`gcp-sa-key`). That key was revoked on 2026-04-08 when all user-managed SA keys were
deleted. The flow has since been superseded by `yfinance-ohlcv-pipeline`, which uses ADC like
all other production flows. The old deployments (`bronze-yfinance-ohlcv`, `silver-yfinance-ohlcv`)
have been removed from Prefect.

---

## Secrets — how API keys are managed

Secrets never live in the code or in Git. Two locations:

**`.env` file (local only, gitignored)**
Used when running flows on your laptop. Copy `.env.example` and fill in real values.
```
GROQ_API_KEY=...
PREFECT_API_URL=http://35.241.252.5/api
```

**Prefect Secret blocks (production)**
Key/value secrets stored in the Prefect server database (on the VM). Referenced in `prefect.yaml`
as `{{ prefect.blocks.secret.<block-name> }}`. The worker injects them as env vars before
running the flow.

Current secret blocks:
| Block name | Injected as | Used by |
|---|---|---|
| `groq-api-key` | `GROQ_API_KEY` | `silver-gold-rss` |
| `gcp-sa-key` | `GOOGLE_APPLICATION_CREDENTIALS_JSON` | legacy — revoked 2026-04-08, no longer used |

To rotate a secret: update the value in the Prefect UI (Blocks section). No redeploy needed.

---

## How a flow run works end-to-end (example: `bronze-silver-rss`)

1. Prefect server clock triggers the deployment at `0 */4 * * *`
2. The worker on the VM picks up the run
3. Worker clones `main` branch from GitHub into a temp dir
4. Worker runs `pip install -e ".[dev]"` (pull step in `prefect.yaml`)
5. Worker executes `src/flows/silver_rss.py:bronze_silver_rss_flow`
6. Flow runs `bronze_yahoo_rss_flow` and `google_news_rss_flow` as subflows → data lands in
   `bronze.yahoo_rss` and `bronze.google_news_rss`
7. Flow runs `dbt run --select rss_articles` → Silver merge runs → `silver.rss_articles` updated
8. Flow calls `run_deployment("silver-gold-rss/silver-gold-rss")` → triggers the scoring flow
9. `silver-gold-rss` fetches unscored Silver rows → calls Groq API per article → writes
   `gold.article_sentiment` → runs `dbt run --select score_news` → `gold.score_news` updated

GCP auth at every step: ADC via the VM's attached SA. Groq auth: `GROQ_API_KEY` from the
`groq-api-key` Prefect Secret block, injected at step 2.

---

## How to operate the system

**Check if flows are running**
Open `http://35.241.252.5` → Flow Runs. Runs should appear every 4h for `bronze-silver-rss`.

**SSH to the VM**
```bash
gcloud compute ssh prefect-server --zone=europe-west1-b --project=bootcamp-project-pea-pme
cd ~/prefect
docker compose ps          # are containers up?
docker compose logs --tail=50  # what happened recently?
```

**Trigger a manual run**
Via the Prefect UI: open a deployment → Run → Quick run.
Via CLI (requires `PREFECT_API_URL` set in env):
```bash
prefect deployment run bronze-silver-rss/bronze-silver-rss
```

**Deploy after a code change**
Code changes only take effect after a new deployment. Always deploy from `main`:
```bash
git checkout main && git pull --rebase origin main
set -a && source .env && set +a
prefect deploy --all
```

**Rotate the GROQ API key**
1. Prefect UI → Blocks → `groq-api-key` → edit value (takes effect on next run)
2. Update `GROQ_API_KEY` in your local `.env`

**Nuclear reset (wipes all Prefect state — use only as last resort)**
```bash
# On the VM
cd ~/prefect
docker compose down -v && docker compose up -d
# Then redeploy from your laptop
prefect deploy --all
```

---

## yfinance OHLCV pipeline — how and why

This section covers the market data pipeline: from raw price fetching to the final technical
score in `gold.stocks_score`. It is the most data-intensive pipeline in the project and the one
that runs on the tightest daily schedule (weekdays at 19h30, after market close).

### Overview

```
yfinance (Python library)
  → bronze.yfinance_ohlcv          raw OHLCV, 462 ISINs
  → silver.yahoo_ohlcv_clean       deduplicated, validated (dbt)
  → silver.yahoo_ohlcv             + 7 technical indicators (Python, library `ta`)
  → gold.stocks_score              5 signals → score_technique [0–10] (dbt)
  → gold.company_scores            composite refresh (dbt)
```

All five steps run sequentially inside a single Prefect deployment:
`yfinance-ohlcv-pipeline` (`src/flows/yfinance_pipeline.py`).

### Why schedule at 19h30?

French markets (Euronext) close at 17h30 Paris time. yfinance typically has the closing price
available 30–60 minutes after close. Scheduling at 19h30 gives a comfortable buffer and ensures
the full session OHLCV row is available for all 462 ISINs before the pipeline runs.

### Bronze — `bronze.yfinance_ohlcv`

yfinance resolves ISINs to Yahoo Finance tickers via `referentiel/ticker_overrides.json` (manual
overrides for tickers that yfinance cannot resolve automatically). The library fetches OHLCV
history up to 20 years back on first run and incremental daily rows on subsequent runs.

Rows are written with APPEND mode, keyed on `(isin, date)` — the Silver cleaning step handles
deduplication, so the Bronze layer stays append-only and re-fetchable if needed.

### Silver — `silver.yahoo_ohlcv_clean` (dbt)

The dbt model deduplicates on `(isin, Date)`, filters out rows where the price is zero or null
(data quality issues from yfinance), and adds a `last_trading_date` column. It is partitioned
by month to keep query costs low when downstream models scan only recent dates.

It supports `full_refresh` (passed as a dbt variable) to force a complete rebuild from Bronze
when schema changes are needed.

### Silver — `silver.yahoo_ohlcv` (Python, `src/silver/compute_silver.py`)

The technical indicators cannot be computed in SQL — they require rolling window calculations
over ordered time series per ISIN. The `ta` library (Technical Analysis library for Python)
handles this, producing 7 indicators per ISIN:

| Indicator | Why it is included |
|---|---|
| `RSI_14` | Measures momentum; identifies overbought/oversold conditions |
| `MACD` + `MACD_signal` | Captures trend acceleration and direction changes |
| `BB_upper` + `BB_lower` | Defines volatility bands; %B shows position within the band |
| `SMA_50` + `SMA_200` | Used together for the Golden Cross signal (long-term trend) |
| `EMA_20` | Short-term trend; faster than SMA, more reactive to recent price action |

**Write strategy:** WRITE_TRUNCATE on the first ISIN processed, WRITE_APPEND on all subsequent
ones. This rebuilds the table cleanly each run without requiring a DELETE/INSERT cycle.

### Gold — `gold.stocks_score` (dbt)

The dbt model converts the 7 continuous indicators into 5 discrete signals, each scored 0/1/2:

| Signal | 2 pts (bullish) | 1 pt (neutral) | 0 pts (bearish) |
|---|---|---|---|
| RSI | RSI < 35 (oversold) | 35 ≤ RSI < 65 or NaN | RSI ≥ 65 (overbought) |
| MACD | MACD > MACD_signal | NaN | MACD ≤ MACD_signal |
| Golden Cross | SMA_50 > SMA_200 | NaN | SMA_50 ≤ SMA_200 |
| Bollinger %B | %B < 0.2 (low band) | 0.2 ≤ %B ≤ 0.8 or NaN | %B > 0.8 (high band) |
| EMA Trend | Close > EMA_20 | NaN | Close ≤ EMA_20 |

`score_technique = sum of 5 signals ∈ [0, 10]`

NaN values (not enough history for a signal to compute) default to 1 (neutral) rather than 0,
so that companies with short data history are not penalised. The model also computes
`score_7d_avg` (7-day moving average) and flags `is_latest = TRUE` for the most recent row per
ISIN.

### Why a concurrency limit of 1 on `yfinance-ohlcv-pipeline`?

The Silver Python step writes using WRITE_TRUNCATE on the first ISIN and WRITE_APPEND on the
rest. If two pipeline instances ran in parallel, the second WRITE_TRUNCATE would wipe the
partial results of the first run. The concurrency limit of 1 prevents this.

---

## API — FastAPI (`src/api/main.py`)

The API is a thin read layer that exposes Gold tables to the Streamlit dashboard. It runs as a
`systemd` service (`pea-pme-api`) directly on the VM — not inside Docker — which keeps it
independent from the Prefect container stack.

**Service details:**
- WorkingDirectory: `/home/mathias/pea-pme-pulse`
- Python venv: `/home/mathias/api-venv/`
- Port: `8000` (reached by nginx via the VM's internal IP `10.132.0.2`)

nginx routes to the API using a regex location matching the known path prefixes
(`gold`, `overview`, `health`, `metrics`, `docs`, `openapi.json`). All other requests go to
the Prefect UI.

**Design choices:**
- A single BigQuery client instance is shared across all requests via `@lru_cache(maxsize=1)`,
  avoiding a new connection on every call.
- Each endpoint runs one parameterised BigQuery query and returns the result as JSON — no
  business logic, no aggregation. All computation happened at pipeline time.
- `/health` returns `{"status": "ok", "timestamp": ...}` for uptime monitoring.
- `/metrics` is exposed by `prometheus-fastapi-instrumentator` (3-line setup) and scraped every
  30 seconds by Prometheus. This gives request counts, error rates, and latency histograms per
  endpoint without any manual instrumentation.

```bash
sudo systemctl status pea-pme-api
sudo journalctl -u pea-pme-api -f
```

---

## Dashboard — Streamlit (`src/dashboard/app.py`)

The dashboard is a Streamlit app that calls the FastAPI endpoints above and renders the results.
It does not query BigQuery directly — all data comes through the API.

**Key design choices:**
- `@st.cache_data(ttl=3600)` on every data loading function: data is fetched once per hour per
  browser session. The "Actualiser les données" button in the sidebar calls
  `st.cache_data.clear()` + `st.rerun()` to force an immediate refresh.
- The API base URL is read from `st.secrets["api_base_url"]` → env `API_BASE_URL` → default
  `http://localhost:8000`. This makes the app portable across local dev and the VM without
  code changes.
- Four tabs (Composite, Actualités, Insiders & Fondamentaux, Analyse Technique) map directly
  to the four scoring dimensions. The Technical tab has three sub-tabs: ranking, signal heatmap,
  and score history.

**Connecting the app to the VM API:**
Set `API_BASE_URL=http://35.241.252.5` in the deployment environment (Streamlit Cloud secrets
or local `.env`).

