# 04 — Flows Prefect

## Configuration globale

- **Version Prefect :** 3.6.24 (dans `prefect.yaml`)
- **Work Pool :** `pea-pme`
- **Work Queue :** `default`
- **Timezone :** Europe/Paris (tous les schedules)
- **Pull steps (tous les flows) :**
  1. `git_clone` depuis `https://github.com/CamilleLarpin/pea-pme-pulse.git` (branche `main`)
  2. `pip install ".[dev]"` (avec `flock` pour sérialiser les installs)

---

## Inventaire des 10 déploiements

### 1. `bronze-silver-rss`
- **Fichier :** `src/flows/silver_rss.py:bronze_silver_rss_flow`
- **Schedule :** `0 */4 * * *` (toutes les 4h)
- **Secret :** `groq-api-key` → `GROQ_API_KEY`
- **Étapes :**
  1. Appelle `yahoo_rss_flow()` — Bronze ingestion Yahoo RSS
  2. Appelle `google_news_rss_flow()` — Bronze ingestion Google News RSS
  3. `dbt deps` — Install packages dbt
  4. `dbt run --select rss_articles` → `silver.rss_articles`
  5. Déclenche `silver-gold-rss` (run_deployment)
- **Tables écrites :** `bronze.yahoo_rss`, `bronze.google_news_rss`, `silver.rss_articles`

---

### 2. `bronze-yahoo-rss`
- **Fichier :** `src/flows/bronze_yahoo_rss.py:yahoo_rss_flow`
- **Schedule :** Manuel uniquement
- **Étapes :**
  1. `yahoo-fetch` — `bronze.rss_yahoo_fr.fetch_feed()`
  2. `yahoo-dump-gcs` — Dump JSON dans GCS `gs://project-pea-pme/rss_yahoo/`
  3. `yahoo-match-load-bq` — Fuzzy match + écriture `bronze.yahoo_rss`
- **Retries :** 2–3, délai 30–60s

---

### 3. `silver-gold-rss`
- **Fichier :** `src/flows/gold_sentiment.py:silver_gold_rss_flow`
- **Schedule :** Manuel (déclenché par `bronze-silver-rss`)
- **Secret :** `groq-api-key` → `GROQ_API_KEY`
- **Étapes :**
  1. `ensure-gold-table` — Crée `gold.article_sentiment` si inexistante
  2. `fetch-unscored-articles` — Anti-join `silver.rss_articles` vs `gold.article_sentiment`
  3. `score-and-write` — Groq `llama-3.1-8b-instant` par article → APPEND `gold.article_sentiment`
  4. `dbt run --select score_news score_insider company_scores` → `gold.score_news`, `gold.score_insider`, `gold.company_scores`
- **Retries :** 1, délai 120s
- **Tables écrites :** `gold.article_sentiment`, `gold.score_news`, `gold.score_insider`, `gold.company_scores`

---

### 4. `bronze-google-news-rss`
- **Fichier :** `src/flows/bronze_google_news_rss.py:google_news_rss_flow`
- **Schedule :** Manuel uniquement
- **Étapes :**
  1. `google-news-fetch` — 2 flux RSS (Euronext Growth + PME Bourse FR)
  2. `google-news-dump-gcs` — Dump JSON `gs://project-pea-pme/rss_google_news/`
  3. `google-news-match-load-bq` — Fuzzy match + `bronze.google_news_rss`
- **Retries :** 2–3, délai 30–60s

---

### 5. `yfinance-ohlcv-pipeline`
- **Fichier :** `src/flows/yfinance_pipeline.py:yfinance_ohlcv_pipeline_flow`
- **Schedule :** `30 19 * * 1-5` (Lun-Ven 19h30 Paris)
- **Concurrency limit :** 1
- **Paramètre :** `full_refresh: bool = False`
- **Étapes (dans l'ordre) :**
  1. `yfinance_ohlcv_flow()` — Ingestion Bronze, 462 ISINs, APPEND BQ
  2. `dbt_deps()` — Install packages dbt
  3. `dbt_run_yahoo_ohlcv_clean(full_refresh)` — `silver.yahoo_ohlcv_clean`
  4. `yfinance_silver_compute()` — 7 indicateurs techniques → `silver.yahoo_ohlcv`
  5. `dbt_run_stocks_score()` — 5 signaux → `gold.stocks_score`
  6. `dbt_run_company_scores()` — `gold.company_scores` refresh
- **Tables écrites :** `bronze.yfinance_ohlcv`, `silver.yahoo_ohlcv_clean`, `silver.yahoo_ohlcv`, `gold.stocks_score`, `gold.company_scores`

---

### 6. `amf-pipeline`
- **Fichier :** `src/flows/pipeline_amf.py:amf_financial_signal_pipeline_flow`
- **Schedule :** `0 0 1 4,10 *` (1er avril et 1er octobre à 00h00)
- **Secrets :** `groq-api-key` → `GROQ_API_KEY`
- **Variables injectées :** `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `AMF_ACTIVE_PROMPT_VERSION=v5`
- **Paramètres :** `full_refresh`, `run_tests`
- **Étapes :**
  1. `amf_flux_flow()` — Bronze AMF (API + PDFs → GCS + `bronze.amf`)
  2. `amf_financial_signal_silver_flow()` — PDF → Groq → `silver.amf_financial_signal`
  3. `dbt run --select financials_score` → `gold.financials_score`
  4. `dbt test financials_score` (optionnel si `run_tests=True`)
- **Tables écrites :** `bronze.amf`, `silver.amf_financial_signal`, `gold.financials_score`

---

### 7. `bronze-abcbourse-rss`
- **Fichier :** `src/flows/bronze_abcbourse_rss.py:abcbourse_rss_flow`
- **Schedule :** `0 8 * * 1-5` (Lun-Ven 08h00 Paris)
- **Variables injectées :** `GCS_SOURCE_REFERENTIEL`, `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `BQ_DATASET_BRONZE`
- **Étapes :**
  1. `abcbourse-load-local-db` — Chargement JSON local pour dédup
  2. `abcbourse-fetch-rss` — Fetch + dédup
  3. `abcbourse-save-local-db` — Persistance état local
  4. `abcbourse-upload-gcs` — Backup JSON dans GCS
  5. `abcbourse-match-fuzzy` — Fuzzy match contre référentiel
  6. `abcbourse-load-bq` — Écriture `bronze.abcbourse_rss`
- **Tables écrites :** `bronze.abcbourse_rss`

---

### 8. `bronze-silver-gold-boursorama`
- **Fichier :** `src/flows/bronze_silver_gold_boursorama.py:bronze_silver_gold_boursorama_flow`
- **Schedule :** `0 8 1 * *` (1er du mois 08h00 Paris)
- **Étapes :**
  1. `bronze_boursorama_flow()` — Scraping Boursorama → GCS CSV → `bronze.boursorama`
  2. `dbt run --select companies` → `silver.companies`
  3. `dbt run --select company_scores` → `gold.company_scores`
- **Tables écrites :** `bronze.boursorama`, `silver.companies`, `gold.company_scores`

---

### 9. `silver-insider-signals`
- **Fichier :** `src/flows/silver_insider_parser.py:amf_insider_parser_flow`
- **Schedule :** Manuel (déclenché par `bronze-amf-flux` à la complétion)
- **Concurrency limit :** 1
- **Étapes :**
  1. `amf-load-pending-docs` — Anti-join `bronze.amf` vs `silver.amf_insider_signals` (15 docs max)
     - Filtre : type_information contient 'DIRIGEANT' ou 'ARTICLE 19', 90 derniers jours, pdf_download_status='success'
  2. `amf-extract-pdf-text` — `pdfplumber` → texte brut par PDF
  3. `amf-ai-parse-signals` — Groq `llama-3.1-8b-instant` → JSON `{signals: [...]}`
  4. `amf-validate-and-enrich` — Validation heuristique + enrichissement metadata
  5. `amf-upload-to-silver` — WRITE_APPEND → `silver.amf_insider_signals`
- **Tables écrites :** `silver.amf_insider_signals`

---

### 10. `gold-insider-signals`
- **Fichier :** `src/flows/gold_insider_signals.py:gold_insider_scoring_flow`
- **Schedule :** Manuel (déclenché par `silver-insider-signals`)
- **Concurrency limit :** 1
- **Étapes :**
  1. `fetch-silver-signals` — Lecture `silver.amf_insider_signals`
  2. `compute-insider-scores` — Scoring Python `gold.amf_insider_score.compute_insider_score()`
  3. `idempotent-merge` — UPSERT via staging :
     - WRITE_TRUNCATE → `gold.score_insider_signals_staging`
     - MERGE sur `source_record_id` → `gold.score_insider_signals`
     - Nettoyage staging
- **Tables écrites :** `gold.score_insider_signals`

> **Note :** `gold.score_insider_signals` (Python MERGE) ≠ `gold.score_insider` (dbt model, lu par l'API).
> Les deux tables sont alimentées par `silver.amf_insider_signals` mais via des chemins différents.
> `gold.score_insider` est le modèle dbt exécuté aussi par `silver-gold-rss` (voir flow 3).

---

## Chaînes de déclenchement

```
Planifié toutes les 4h
└─ bronze-silver-rss
     └─ (run_deployment) silver-gold-rss

Planifié toutes les 4h
└─ bronze-amf-flux
     └─ (à la complétion) silver-insider-signals
          └─ (à la complétion) gold-insider-signals

Planifié Lun-Ven 19h30
└─ yfinance-ohlcv-pipeline
     (tout en séquence interne, pas de déclenchement externe)
```

---

## Secrets et variables d'environnement

| Block Prefect | Variable injectée | Utilisé par | Statut |
|---|---|---|---|
| `groq-api-key` | `GROQ_API_KEY` | bronze-silver-rss, silver-gold-rss, amf-pipeline | ✅ Actif |
| `gcp-sa-key` | `GOOGLE_APPLICATION_CREDENTIALS_JSON` | silver-yfinance-ohlcv (LEGACY) | ❌ Révoqué 2026-04-08 |

**Production :** Auth GCP via ADC (metadata server VM), aucune clé JSON sur disque.

---

## Flows orphelins (à supprimer dans l'UI Prefect)

Les déploiements suivants sont remplacés par `yfinance-ohlcv-pipeline` :
- `bronze-yfinance-ohlcv` — Dernier run : 2026-04-08
- `silver-yfinance-ohlcv` — Dernier run : 2026-04-08

Ces flows utilisaient encore la clé SA révoquée. À supprimer dans Prefect UI → Deployments.
