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
| `bronze-yfinance-ohlcv` | Weekdays 19h30 | cron `30 19 * * 1-5` |
| `silver-yfinance-ohlcv` | None | triggered by `bronze-yfinance-ohlcv` on completion |
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

**One exception — `silver-yfinance-ohlcv`**
This flow was written before the VM migration. It still injects a JSON SA key via a Prefect Secret
block (`gcp-sa-key`). That key has since been revoked (all user-managed SA keys were deleted
2026-04-08). This flow needs to be updated to use ADC like all other flows. See below.

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
| `gcp-sa-key` | `GOOGLE_APPLICATION_CREDENTIALS_JSON` | `silver-yfinance-ohlcv` (legacy — to be removed) |

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