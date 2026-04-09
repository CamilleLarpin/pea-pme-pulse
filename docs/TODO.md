# TODO — pea-pme-pulse

> Shared team backlog. For personal task tracking, use your own `.claude/TODOS.md` (gitignored).
> Update this file when picking up or completing an item.

---

## Open PRs

| PR | Description | Owner |
|---|---|---|
| #38 | FastAPI read-only API — `GET /overview` + `GET /overview/gold` | Camille |

---

## Urgent / Blockers

- [ ] **`silver-yfinance-ohlcv` broken in prod** — still injects `GOOGLE_APPLICATION_CREDENTIALS_JSON` from Prefect Secret `gcp-sa-key`; that SA key was revoked 2026-04-08. Fix: remove `job_variables` from `prefect.yaml` for this flow + update dbt tasks to use `oauth` profile (same pattern as `gold_sentiment.py`) — *teammate*
- [ ] **Teammates: update `.env`** — set `PREFECT_API_URL=http://35.241.252.5/api`, comment out `PREFECT_API_KEY` — *all*

---

## Next

- [ ] **FastAPI — merge PR #38** then: API key auth → deploy on GCP VM — *Camille*
- [ ] **Prefect alerts** — Slack notification on flow `Crashed` / `Failed` for `bronze-silver-rss` and `silver-gold-rss` — *Camille*
- [ ] **nao semantic layer enrichment** — declare semantic models + metrics in dbt · improves nao query quality — *Camille*
  - Step 1 — `dbt/models/gold/semantic_models.yml` · `score_news` + `article_sentiment` · entity `company` (primary key `isin`)
  - Step 2 — `dbt/models/gold/metrics.yml` · `investment_score`, `avg_sentiment_45d`, `mention_count_45d`
  - Step 3–4 — `dbt/semantic/nao_context.yml` · `rules:` block + `metrics:` + `dimensions:` index

---

## Backlog

- [ ] Slack alert on new PR / commits (team coordination)
- [ ] FastAPI Phase 2 — score endpoints (when more gold data accumulates)
- [ ] `bronze.yahoo_rss` — 0 rows since 2026-03-31 · run manually once to confirm matching issue
- [ ] `abcbourse_chroniques_rss` — never populated in BQ · investigate or remove from Silver union

---

## Done ✅

- [x] Bronze → Silver → Gold end-to-end chain validated — auto-triggered every 4h
- [x] Self-hosted Prefect on GCP VM — `http://35.241.252.5` · all 7 flows deployed
- [x] nao Talk To My Data — deployed at `http://35.241.252.5:5005` · Gemini 2.5 Flash · always on
- [x] `silver.companies` + `gold.company_scores` — Boursorama pipeline Bronze → Gold (PR #40)
- [x] GCP SA key + Prefect Cloud API key rotated — 2026-04-08
