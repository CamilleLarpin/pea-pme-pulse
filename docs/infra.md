# Infrastructure & Components — pea-pme-pulse

> Who is this for: a Junior Data Engineer joining the project who wants to understand how all the
> pieces fit together — not just where things live, but *why* each component exists and how data
> moves through the system from raw source to a dashboard score.

---

## What the system does

Automated daily scoring of PEA-PME-eligible SMEs. It ingests financial data from multiple sources
(RSS feeds, market prices, insider filings, company referentiel), processes it through a
Bronze → Silver → Gold pipeline, and produces a composite investment score per company that is
exposed via a REST API and visualized in a Streamlit dashboard.

---

## Big picture — how everything connects

```
External sources                 GCS (raw dump)         BigQuery (structured)
─────────────────                ──────────────          ─────────────────────

Yahoo Finance RSS ──────────────► rss_yahoo/         ──► bronze.yahoo_rss
Google News RSS  ──────────────► rss_google_news/    ──► bronze.google_news_rss
ABCBourse RSS    ──────────────► rss_abcbourse/      ──► bronze.abcbourse_rss
AMF API (XML/PDF)──────────────► amf/                ──► bronze.amf
yfinance (OHLCV) ──────────────► (skip GCS)          ──► bronze.yfinance_ohlcv
Boursorama scrape──────────────► (referentiel CSV)   ──► bronze.boursorama

                                    dbt (SQL transformations)
                                    ─────────────────────────
                                    bronze.*  ──► silver.*
                                    silver.*  ──► gold.*

                                    Groq LLM
                                    ────────
                                    silver.rss_articles ──► gold.article_sentiment

                                    gold.*
                                      │
                                    FastAPI (REST)
                                      │
                                    Streamlit (dashboard)
                                      │
                                    nao (natural language queries)
```

Prefect orchestrates all of the above: it schedules flows, chains them in the right order,
handles retries, and gives you visibility (logs, run history) via its web UI.

---

## Where code runs

There are two execution environments:

**Your laptop (local dev)**
Used for: writing code, running flows manually to test, deploying new configs, querying BigQuery.
Nothing in production depends on your laptop being on.

**GCP VM (`34.77.145.245`, zone `europe-west1-b`)**
This is where all scheduled production pipelines run. It runs 24/7 and hosts:
- the Prefect server (orchestration scheduler + database)
- the Prefect worker (flow executor)
- nginx (HTTP reverse proxy + basic auth)
- FastAPI (REST API serving Gold data)

The VM is a GCP `e2-small` instance managed in the `bootcamp-project-pea-pme` GCP project.

---

## Components — what they are and why they fit here

### Google Cloud Storage (GCS)

**What it is:** an object store — think of it as a folder in the cloud where you drop arbitrary
files (JSON, CSV, PDF). No schema, no SQL, just files.

**Why it's here:** some of our sources are ephemeral. An RSS feed only shows the last ~20 articles.
If you call it again in 4 hours, the earlier articles are gone. GCS is the safety net: every
flow dumps the raw response to GCS *before* filtering and loading to BigQuery. This means you
can always go back and reprocess the raw data if a bug in the parsing logic is found later.

**What we store:**
```
gs://project-pea-pme/
├── rss_yahoo/          # raw Yahoo RSS JSON dumps, one file per run (timestamped)
├── rss_google_news/    # raw Google News RSS JSON dumps, timestamped
├── rss_abcbourse/      # raw ABCBourse RSS JSON dumps
└── amf/                # AMF PDFs, XMLs, and JSONL — regulatory documents
```

**Not everything goes to GCS.** yfinance data is always re-fetchable (structured, historical API)
so it goes straight to BigQuery. Boursorama data lands in GCS as a CSV snapshot (the referentiel),
which is then read by subsequent flows.

---

### BigQuery

**What it is:** Google's serverless data warehouse. You query it with SQL. It handles tables
ranging from a few hundred rows to billions — same syntax, no infrastructure to manage.

**Why it's here:** all structured data in this pipeline ends up in BigQuery. It's the single
source of truth for every layer. dbt, FastAPI, and nao all read from BigQuery. Streamlit reads
*indirectly* via FastAPI, which queries BigQuery. Using one warehouse means there is only one
place to debug data quality issues.

**The medallion architecture (Bronze → Silver → Gold):**

This is a standard data engineering pattern. The idea is that each layer has a clear, single
responsibility. You never skip a layer.

| Dataset | Responsibility | Who writes it |
|---|---|---|
| `bronze` | Raw ingested data — minimal transformation, close to source | Python ingestion flows |
| `silver` | Cleaned, deduplicated, standardized — no business logic yet | dbt SQL + Python (technical indicators) |
| `gold` | Business-ready: scored, aggregated, presentation-ready | dbt SQL + Python (LLM scoring) |

**Why this matters:** if a Gold score looks wrong, you check Silver first — is the data clean?
Then Bronze — is the raw data correct? You can always trace a problem back to its layer.
Mixing layers (e.g., writing a score directly from a Bronze table) would make debugging impossible.

**Current tables:**

```
bootcamp-project-pea-pme
├── bronze
│   ├── yahoo_rss
│   ├── google_news_rss
│   ├── abcbourse_rss
│   ├── amf
│   ├── boursorama
│   └── yfinance_ohlcv
├── silver
│   ├── rss_articles        # unified + deduplicated RSS from all 3 sources
│   ├── companies           # referentiel: 571 PEA-PME companies with ISIN
│   ├── yahoo_ohlcv_clean   # dbt: dedup + validation of bronze.yfinance_ohlcv
│   └── yahoo_ohlcv         # Python: OHLCV + RSI_14, MACD, BB, SMA_50/200, EMA_20
└── gold
    ├── article_sentiment   # Groq LLM sentiment score per article (0–10)
    ├── score_news          # 45-day mention count + avg sentiment → normalized 1–10 per ISIN
    ├── stocks_score        # 5 technical signals → score_technique [0–10] per (isin, date)
    ├── score_insider       # AMF insider trades → score_1_10 per company
    ├── financials_score    # AMF financial data → score_fondamental per company
    └── company_scores      # composite score = average of 4 dimensions per company per day
```

---

### dbt (data build tool)

**What it is:** a SQL-first transformation framework. You write `.sql` files that express
`SELECT` queries, and dbt handles the `CREATE OR REPLACE TABLE` boilerplate, dependency
ordering, documentation, and tests.

**Why it's here:** all Bronze → Silver and Silver → Gold transformations that can be expressed
in SQL live in dbt. This gives three things you can't easily get with raw SQL scripts:

1. **Lineage** — dbt knows which models depend on which tables. Run `dbt docs generate` and
   you get an interactive graph showing exactly how data flows from Bronze sources to Gold outputs.
   Useful when you want to understand impact before changing a column.

2. **Tests** — every column in every model has at minimum a `not_null` test. Run `dbt test`
   and you know immediately if a transformation produced unexpected nulls or invalid values.
   These tests run automatically inside Prefect flows after each `dbt run`.

3. **Documentation** — every table and column has a description in `dbt/models/`. The `dbt docs`
   site (served by nginx at `http://34.77.145.245/dbt-docs`) lets you browse the full data
   dictionary without reading SQL files.

**How dbt runs in this project:** dbt is not scheduled independently — it runs as a subprocess
*inside* Prefect flows. A flow calls `dbt run --select <model>` after its Python ingestion step
completes. The dbt BigQuery connection profile is built dynamically in a temp directory at
runtime (no `~/.dbt/profiles.yml` on the VM).

**What dbt does NOT own:** Bronze tables. They are declared as dbt `source()` references, but
dbt never creates or writes them — that is the Python ingestion flows' job. dbt only transforms
data that already exists.

**dbt project layout:**
```
dbt/models/
├── sources.yml          # declares all bronze tables dbt reads (but does not build)
├── definitions.md       # one definition per business concept (isin, sentiment_score, …)
├── silver/
│   ├── schema.yml       # column docs for silver models
│   ├── rss_articles.sql
│   ├── companies.sql
│   └── yahoo_ohlcv_clean.sql
└── gold/
    ├── schema.yml       # column docs for gold models
    ├── score_news.sql
    ├── stocks_score.sql
    ├── score_insider.sql
    ├── financials_score.sql
    └── company_scores.sql
```

---

### Prefect (self-hosted on the GCP VM)

**What it is:** an orchestration tool — a smarter version of cron. It schedules Python
functions to run at given times, chains them in dependency order, retries failed steps, and
stores a log of every run so you can debug failures.

**Why it's here and self-hosted:** we run Prefect on our own VM rather than using Prefect Cloud
because it avoids usage costs and keeps all pipeline data within our GCP project. The trade-off
is that we are responsible for keeping the VM and containers running.

**Three concepts to understand:**

**Flow** — a Python function decorated with `@flow`. It is the unit of execution. Flows live in
`src/flows/`. Each flow calls `@task`-decorated steps in sequence. Tasks can be retried
individually without re-running the whole flow.

**Deployment** — a named, versioned registration of a flow in the Prefect server. Defined in
`prefect.yaml`. A deployment says: "run *this* flow function, on *this* schedule, with *these*
environment variables." Deploying does not run the flow — it just registers the config.

**Worker** — a long-running process on the VM that watches the server for scheduled or manually
triggered runs, picks them up, clones the repo, installs dependencies, and executes the flow.

```
Prefect server (VM) → schedule triggers run → worker (VM)
  → git clone main branch
  → pip install -e ".[dev]"
  → run flow function
  → report result (success/failure + logs) back to server
```

The Prefect UI is at `http://34.77.145.245` (nginx basic auth, credentials in `.env`).

**Current deployments:**

| Deployment | Schedule | Notes |
|---|---|---|
| `bronze-silver-rss` | Every 4h (`0 */4 * * *`) | Orchestrator: Yahoo + Google News → Silver dbt → Gold Groq scoring |
| `silver-gold-rss` | None (manual or triggered) | Triggered by `bronze-silver-rss` on completion |
| `yfinance-ohlcv-pipeline` | Weekdays 19h30 (`30 19 * * 1-5`) | Full Bronze→Silver→Gold OHLCV pipeline, concurrency limit 1 |
| `bronze-abcbourse-rss` | Weekdays 08h (`0 8 * * 1-5`) | Morning market open |
| `amf-pipeline` | Bi-annually (`0 0 1 4,10 *`) | April 1st and October 1st — regulatory filing seasons |
| `bronze-silver-gold-boursorama` | 1st of month 08h (`0 8 1 * *`) | Monthly referentiel refresh |
| `silver-insider-signals` | None (triggered) | Triggered by `bronze-amf-flux` on completion |
| `gold-insider-signals` | None (triggered) | Triggered by `silver-insider-signals` on completion |

---

### Docker

**What it is:** a containerization tool. A Docker container packages an application with all its
dependencies into an isolated, reproducible environment. `docker compose` manages multiple
containers as a group.

**Why it's here:** running Prefect server, worker, nginx, and FastAPI as raw processes on a VM
is fragile — a process crash means manual restart, and dependency conflicts between services
become a maintenance problem. Docker gives each service a clean, isolated environment and
`restart: unless-stopped` means containers come back up automatically after a crash or VM reboot.

**Our services (`infra/docker-compose.prefect.yml`):**

| Service | Image | What it does |
|---|---|---|
| `prefect-server` | `prefecthq/prefect:3-latest` | Runs the Prefect API + database + scheduler |
| `prefect-worker` | `prefecthq/prefect:3-latest` | Picks up flow runs, clones repo, executes flows |
| `nginx` | `nginx:alpine` | Reverse proxy on port 80 — routes `/api` to Prefect, `/dbt-docs` to static files, adds basic auth |

**Note:** FastAPI does not run in Docker. It runs as a `systemd` service directly on the VM —
see the FastAPI section below.

The Prefect worker container does not run flow code directly inside its own image. Instead, it
spawns a new process (process worker type), clones the repo, and runs the flow there. This is
why flows can use packages from `pyproject.toml` even though they are not baked into the image.

---

### FastAPI

**What it is:** a Python web framework for building REST APIs. It is fast, auto-generates
OpenAPI docs at `/docs`, and validates request/response schemas using Pydantic models.

**Why it's here:** Streamlit (and potentially other consumers like nao) needs a stable, typed
interface to query BigQuery Gold data. Putting raw BigQuery queries in the Streamlit app would
couple the dashboard directly to the warehouse schema — any column rename would break the UI.
FastAPI acts as a contract layer: the dashboard speaks HTTP, the API speaks BigQuery.

**Auth:** no token auth between Streamlit and FastAPI — both are internal to our infrastructure.
FastAPI authenticates to BigQuery via ADC (the VM's service account).

**Current endpoints (`src/api/main.py`):**

| Endpoint | What it returns |
|---|---|
| `GET /overview` | All BQ datasets and their tables |
| `GET /overview/gold` | Gold dataset tables only |
| `GET /gold/stocks-score/latest` | Latest technical score per company (is_latest = TRUE) |
| `GET /gold/stocks-score/history` | Score time series for a list of ISINs, over N days |
| `GET /gold/company-scores/latest` | Composite score (4 dimensions) for today |
| `GET /gold/score-news/latest` | News sentiment score per company, 45-day window |
| `GET /gold/score-insider/latest` | AMF insider trade signals, last 45 days |
| `GET /gold/financials-score/latest` | Fundamental score per company (latest filing) |
| `GET /gold/article-sentiment/latest` | Raw article sentiments, last 45 days, max 500 |

**How it runs:** FastAPI is deployed as a `systemd` service (`pea-pme-api`) directly on the VM,
not in Docker. This keeps it independent from the Prefect container stack.
- WorkingDirectory: `/home/mathias/pea-pme-pulse`
- Python venv: `/home/mathias/api-venv/`
- Port: `8000` (reached by nginx via the VM's internal IP `10.132.0.2`)

nginx routes to FastAPI using a regex location matching the known path prefixes
(`gold`, `overview`, `health`, `metrics`, `docs`, `openapi.json`).

A shared `@lru_cache(maxsize=1)` BigQuery client instance is reused across requests.
`/health` and `/metrics` (Prometheus) are exposed for monitoring.

```bash
sudo systemctl status pea-pme-api
sudo journalctl -u pea-pme-api -f
```

---

### Streamlit

**What it is:** a Python library that turns a script into an interactive web application.
No frontend code required — you write Python and it renders a browser UI.

**Why it's here:** the goal of the pipeline is investment scoring, not data engineering. The
scores need to be readable by a non-technical user. Streamlit lets the team ship a usable
dashboard quickly in Python — the same language as the rest of the project.

**How it works:** Streamlit is deployed on Streamlit Cloud (free tier, always on), connected
to the `main` branch of this repo. On load, it calls all FastAPI endpoints to fetch the latest
Gold data, caches responses for 1 hour (`@st.cache_data(ttl=3600)`), and renders:

- Global KPIs (universe size, average composite score, opportunities ≥ 7)
- Tab A — Vue Composite: top-25 company ranking by composite score + radar chart for top 5
- Tab B — Actualités: news sentiment ranking + volume vs sentiment scatter plot
- Tab C — Insiders & Fondamentaux: AMF insider signals + fundamental metrics
- Tab D — Analyse Technique: technical score ranking, signal heatmap (RSI/MACD/Golden Cross/
  Bollinger/EMA), score history charts for top 5

The `api_base_url` (pointing to `http://34.77.145.245:8000`) is set as a Streamlit Cloud secret
in the app's Advanced Settings — it is not in the repository.

The `api_base_url` (pointing to `http://34.77.145.245:8000`) is set as a Streamlit Cloud secret
in the app's Advanced Settings — it is not in the repository.

---

### Prefect UI

**What it is:** a web UI served by the Prefect server, accessible at `http://34.77.145.245`
(protected by nginx basic auth).

**Why it matters operationally:** this is where you go to:
- See which flows ran and whether they succeeded (Flow Runs view)
- Read logs for a specific run — each `@task` step has its own logs
- Trigger a manual run without needing the CLI (Deployments → Quick run)
- Check and rotate secret values (Blocks → Secret)
- See the work pool health (is the worker alive?)

If a flow goes red, start here. Expand the failed task to see the Python traceback.

---

### dbt docs

**What it is:** a static documentation site generated by `dbt docs generate`. It includes:
- a lineage graph (visual DAG of all sources → models → models)
- a column-level data dictionary (description, type, tests)
- model SQL source code

**Why it matters:** when you are joining a table you have never used, the dbt docs site is
faster than reading SQL. It tells you what each column means, where the data came from, and
what tests are run on it.

**Access:** `http://34.77.145.245/dbt-docs` — served as static files by nginx from
`/var/www/dbt-docs` on the VM. Regenerate after schema changes:

```bash
cd dbt && dbt docs generate
# Copy target/ to the VM: scp -r target/* user@34.77.145.245:/var/www/dbt-docs/
```

---

### nao (natural language → BigQuery)

**What it is:** an AI agent that translates natural language questions into BigQuery SQL queries
and returns results. It runs locally (Gemini 2.5 Flash model) using the schema definitions in
`dbt/semantic/nao_context.yml` as context.

**Why it fits here:** the pipeline produces ~10 Gold tables. Analysts want to ask questions like
"which companies have a composite score above 7 and a positive insider signal this week?" without
writing SQL. nao handles this without needing a separate BI tool.

**Configuration:** `nao_config.yaml` (gitignored). Schema context lives in
`dbt/semantic/nao_context.yml` — routing keywords, example questions, and query patterns for
each Gold table. Keep this file in sync with schema changes; nao will produce wrong queries if
column names or table structures drift.

**Status:** working locally. Public sharing via ngrok is the next step.

---

## Flows — what makes each one specific

### `bronze-silver-rss` (orchestrator, every 4h)

This is the main orchestrator flow. It does not fetch data itself — it calls three subflows in
sequence and chains them:

1. `yahoo_rss_flow` → ingests Yahoo Finance FR RSS
2. `google_news_rss_flow` → ingests Google News RSS
3. `dbt run silver.rss_articles` → merges and deduplicates both sources into a single Silver table
4. `silver_gold_rss_flow` (triggered via Prefect API) → Groq scoring

**What's specific:** the flow uses `run_deployment()` to trigger the scoring step as a separate
Prefect deployment rather than calling it as an inline subflow. This means the scoring step
appears as its own flow run in the UI — you can see its logs independently, and if scoring fails,
it does not roll back the Bronze or Silver work that already completed.

The dbt profile is built dynamically in a `tempfile.TemporaryDirectory()` at runtime — there is
no `profiles.yml` on the VM. This is intentional: profile config changes are managed in code, not
via a manual file on the server.

---

### Yahoo Finance FR RSS (`src/bronze/rss_yahoo_fr.py`)

**What's specific:** Yahoo Finance does not publish a clean RSS feed per ISIN. This flow fetches
the French-language Yahoo Finance RSS, then uses fuzzy matching against the company referentiel
(`referentiel/boursorama_peapme_final.csv`) to identify which articles mention a PEA-PME company.
Only matched articles are loaded to BigQuery. The raw feed (all articles, matched or not) is
dumped to GCS first — so false negatives in matching can be audited without re-fetching.

---

### Google News RSS (`src/bronze/rss_google_news.py`)

**What's specific:** Google News RSS is queried per company name (one request per company in the
referentiel). This makes it more precise than Yahoo but slower — the flow loops over all 571
companies. Rate limiting is a real concern; the flow is scheduled every 4h to stay within Google's
informal limits. The GCS dump is per-company, per-run.

---

### ABCBourse RSS (`src/flows/bronze_abcbourse_rss.py`, weekdays 08h)

**What's specific:** ABCBourse is a French financial news site with a structured RSS feed that
already includes company tickers. Matching is done via ticker rather than fuzzy name matching,
which makes it more reliable than Yahoo or Google News. The flow runs at 08h (market open) to
capture overnight news before the trading day.

---

### yfinance OHLCV (`src/flows/yfinance_pipeline.py`, weekdays 19h30)

This single deployment (`yfinance-ohlcv-pipeline`) runs the full Bronze → Silver → Gold market
data pipeline in sequence. A concurrency limit of 1 is enforced: the Silver write step uses
WRITE_TRUNCATE on the first ISIN then WRITE_APPEND on the rest — a second parallel instance
would corrupt the table by truncating partial results.

**Why 19h30?** French markets (Euronext) close at 17h30. yfinance has the closing price
available ~30–60 minutes after close. 19h30 gives a safe buffer.

**Why no GCS dump?** yfinance is a structured historical API — data is always re-fetchable.
Unlike RSS feeds (ephemeral, 20-article window), there is no risk of losing data if the pipeline
is delayed. GCS backup would add cost and complexity without benefit here.

**Step by step:**

1. `yfinance_ohlcv_flow` — fetches daily OHLCV for 462 ISINs via `yfinance`, writes to
   `bronze.yfinance_ohlcv` (APPEND). ISINs are resolved to Yahoo Finance tickers via
   `referentiel/ticker_overrides.json` (manual overrides for tickers yfinance cannot resolve).

2. `dbt run silver.yahoo_ohlcv_clean` — deduplicates on `(isin, Date)`, filters zero/null
   prices, adds `last_trading_date`. Partitioned by month to keep downstream query costs low.
   Supports `full_refresh` to force a complete rebuild from Bronze.

3. `yfinance_silver_compute` — computes 7 technical indicators per ISIN using the `ta` library
   (rolling window calculations that cannot be expressed in SQL):

   | Indicator | Role in scoring |
   |---|---|
   | `RSI_14` | Momentum — identifies oversold/overbought conditions |
   | `MACD` + `MACD_signal` | Trend acceleration and direction changes |
   | `BB_upper` + `BB_lower` | Volatility bands — %B position within the band |
   | `SMA_50` + `SMA_200` | Long-term trend (Golden Cross signal) |
   | `EMA_20` | Short-term trend, more reactive than SMA |

4. `dbt run gold.stocks_score` — converts the 7 indicators into 5 discrete signals (0/1/2 pts
   each), summed into `score_technique ∈ [0, 10]`:

   | Signal | 2 pts (bullish) | 1 pt (neutral) | 0 pts (bearish) |
   |---|---|---|---|
   | RSI | < 35 (oversold) | 35–65 or NaN | ≥ 65 (overbought) |
   | MACD | MACD > MACD_signal | NaN | MACD ≤ MACD_signal |
   | Golden Cross | SMA_50 > SMA_200 | NaN | SMA_50 ≤ SMA_200 |
   | Bollinger %B | %B < 0.2 | 0.2 ≤ %B ≤ 0.8 or NaN | %B > 0.8 |
   | EMA Trend | Close > EMA_20 | NaN | Close ≤ EMA_20 |

   NaN defaults to 1 (neutral) so companies with short history are not penalised. The model
   also produces `score_7d_avg` (7-day moving average) and `is_latest = TRUE` for the most
   recent row per ISIN.

5. `dbt run gold.company_scores` — refreshes the composite score with the updated `score_stock`.

---

### AMF pipeline (`src/flows/pipeline_amf.py`, bi-annually)

**What's specific:** the AMF (Autorité des Marchés Financiers) publishes company financial
disclosures as PDFs and XML feeds. This flow:
1. Fetches the AMF XML flux → `bronze.amf`
2. Downloads PDFs for matched companies → stored in GCS (`amf/`)
3. Extracts financial metrics from PDFs using a Groq LLM (prompt version tracked as an env var
   `AMF_ACTIVE_PROMPT_VERSION`)
4. Runs `dbt run financials_score` → `gold.financials_score`
5. Optionally runs `dbt test financials_score` after the Gold model

The bi-annual schedule (April 1st and October 1st) aligns with French fiscal year reporting
seasons. The `full_refresh` parameter allows wiping and rebuilding the Gold model from scratch
— useful when the scoring prompt is updated.

---

### AMF insider signals (triggered chain)

**What's specific:** insider trade declarations (purchases by company executives) are a separate
AMF data feed from the financial disclosures above. They flow through a three-deployment chain:
`bronze-amf-flux` → (triggers) → `silver-insider-signals` → (triggers) → `gold-insider-signals`.
Each link is a separate Prefect deployment so you can restart from any failed step without
re-running the earlier ones. The Gold step scores each insider event on a 1–10 scale based on
transaction volume and number of operations.

---

### Boursorama (`src/flows/bronze_silver_gold_boursorama.py`, 1st of month)

**What's specific:** Boursorama is the authoritative source for the PEA-PME eligible company
list (571 companies with ISIN and ticker). The flow scrapes the Boursorama PEA-PME listing,
writes it to GCS as a CSV snapshot, then runs `dbt run silver.companies` and
`dbt run gold.company_scores`. It is the *only* flow that updates the referentiel — all other
flows read `referentiel/boursorama_peapme_final.csv` as a static file. The monthly schedule
reflects how often the eligible list changes (AMF rule changes, IPOs, delistings).

---

## Auth — how the system proves its identity to GCP

This is the most important operational concept to understand: **no password or key file is stored
on the VM**.

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
This flow injected a JSON SA key via a Prefect Secret block (`gcp-sa-key`). That key was revoked
on 2026-04-08. The flow has since been superseded by `yfinance-ohlcv-pipeline`, which uses ADC
like all other production flows. The old deployments have been removed from Prefect.

---

## Secrets — how API keys are managed

Secrets never live in the code or in Git. Two locations:

**`.env` file (local only, gitignored)**
Used when running flows on your laptop. Copy `.env.example` and fill in real values.
```
GROQ_API_KEY=...
PREFECT_API_URL=http://34.77.145.245/api
```

**Prefect Secret blocks (production)**
Key/value secrets stored in the Prefect server database (on the VM). Referenced in `prefect.yaml`
as `{{ prefect.blocks.secret.<block-name> }}`. The worker injects them as env vars before
running the flow.

Current secret blocks:
| Block name | Injected as | Used by |
|---|---|---|
| `groq-api-key` | `GROQ_API_KEY` | `bronze-silver-rss`, `amf-pipeline` |
| `gcp-sa-key` | `GOOGLE_APPLICATION_CREDENTIALS_JSON` | legacy — revoked 2026-04-08, no longer used |

To rotate a secret: update the value in the Prefect UI (Blocks section). No redeploy needed.

---

## How a flow run works end-to-end (example: `bronze-silver-rss`)

1. Prefect server clock triggers the deployment at `0 */4 * * *`
2. The worker on the VM picks up the run
3. Worker clones `main` branch from GitHub into a temp dir
4. Worker runs `pip install -e ".[dev]"` (pull step in `prefect.yaml`)
5. Worker executes `src/flows/silver_rss.py:bronze_silver_rss_flow`
6. Flow runs `yahoo_rss_flow` → data lands in `bronze.yahoo_rss` and GCS
7. Flow runs `google_news_rss_flow` → data lands in `bronze.google_news_rss` and GCS
8. Flow runs `dbt deps` then `dbt run --select silver.rss_articles` → Silver merge runs
9. Flow calls `run_deployment("silver-gold-rss/silver-gold-rss")` → triggers scoring flow
10. `silver-gold-rss` fetches unscored Silver rows → calls Groq API per article →
    writes `gold.article_sentiment` → runs `dbt run --select score_news` → `gold.score_news` updated

GCP auth at every step: ADC via the VM's attached SA. Groq auth: `GROQ_API_KEY` from the
`groq-api-key` Prefect Secret block, injected at step 2.

---

## How to operate the system

**Check if flows are running**
Open `http://34.77.145.245` → Flow Runs. Runs should appear every 4h for `bronze-silver-rss`.

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

## Observations — where the pipeline could improve

These are not urgent fixes, but patterns worth addressing as the project matures:

**1. FastAPI as a bus to BigQuery is a bottleneck**
Every Streamlit page load fires multiple BigQuery queries sequentially via FastAPI. BigQuery is
a batch warehouse, not an OLTP database — cold query startup alone adds ~2–3s per request.
Consider adding a lightweight caching layer (e.g., Redis or a simple in-process TTL cache in
FastAPI) so repeated requests within a time window return a cached response. The Streamlit
`@st.cache_data(ttl=3600)` helps at the browser level but not when multiple users hit the API.

**2. No alerting on flow failures**
Currently, a failed flow is only visible if someone manually checks the Prefect UI. Add a
Prefect notification block (Slack or email) to the critical flows (`bronze-silver-rss`,
`bronze-yfinance-ohlcv`, `amf-pipeline`) so failures are surfaced proactively.

**3. dbt docs are manually regenerated**
The dbt docs site at `/dbt-docs` goes stale whenever a schema changes. Consider automating
`dbt docs generate` as a post-deploy step in the Prefect `prefect.yaml` pull section, or
running it as a dedicated scheduled deployment.

**4. The Google News RSS loop is fragile at scale**
One HTTP request per company (571 companies) in a single flow run is slow and brittle — one
timeout crashes the whole batch. Consider chunking the company list and using Prefect's
`submit()` for concurrent task execution, with per-company retries.

**5. No data quality monitoring over time**
dbt tests (`not_null`, `unique`) catch structural errors but do not catch business-level drift —
e.g., a sudden drop in article volume could indicate the RSS source changed format without
raising a dbt test error. Adding a simple row-count check per Bronze table (alert if < 10 rows
after a run) would catch silent failures early.

**6. nao context is not version-controlled alongside schema changes**
`dbt/semantic/nao_context.yml` is manually maintained. When a Gold column is renamed or a new
table is added, nao context can drift silently. Consider adding a CI check that verifies all
tables/columns referenced in `nao_context.yml` exist in the current dbt schema.

---

## Monitoring — Prometheus + Grafana

Prometheus and Grafana run as Docker services alongside Prefect (defined in
`infra/docker-compose.prefect.yml`).

**Prometheus** scrapes `/metrics` on `host.docker.internal:8000` every 30 seconds.
`host.docker.internal` resolves to the VM host via the `extra_hosts` Docker setting, allowing
the containerised Prometheus to reach the FastAPI service running outside Docker.

The FastAPI `/metrics` endpoint is exposed by `prometheus-fastapi-instrumentator` — it provides
request counts, error rates, and latency histograms per endpoint with no manual instrumentation.

**Grafana** is accessible at `http://34.77.145.245/grafana/`. The Prometheus datasource is
auto-provisioned from `infra/grafana/provisioning/datasources/prometheus.yml`. Dashboard JSON
files placed in `infra/grafana/dashboards/` are loaded automatically at startup via the provider
config in `infra/grafana/provisioning/dashboards/provider.yml`.

Key panels in the main dashboard:
- **API Status** — `up{job="pea-pme-api"}` (stat, green/red)
- **CPU / RAM VM** — Node Exporter metrics (gauge)
- **Latence p95 par endpoint** — `histogram_quantile(0.95, ...)` per handler (time series)
- **Taux erreur API** — rate of 5xx responses (stat)
- **Fraîcheur pipeline** — BigQuery query returning last update date per Gold table
