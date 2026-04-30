# 03 — Pipeline de données (Bronze → Silver → Gold)

## COUCHE BRONZE

### Tables BigQuery (`bronze.*`)

#### `bronze.yfinance_ohlcv`
- **Source :** Library Python `yfinance`
- **Contenu :** OHLCV quotidien (Open, High, Low, Close, Volume)
- **Couverture :** 462 ISINs (résolution ISIN → ticker Yahoo via `referentiel/ticker_overrides.json`)
- **Historique :** Jusqu'à 20 ans
- **Grain :** (isin, date)
- **Clé :** (isin, date) — idempotent par date
- **Mode écriture :** APPEND (nouvelles lignes seulement)
- **GCS :** Non — yfinance est re-fetchable, pas de backup nécessaire

#### `bronze.yahoo_rss`
- **Source :** Yahoo Finance FR RSS
- **Contenu :** Articles financiers matchés sur les sociétés du référentiel
- **Grain :** row_id (un article par ligne)
- **GCS :** `gs://project-pea-pme/rss_yahoo/` (dumps JSON horodatés)
- **Matching :** fuzzy match via `rapidfuzz` contre `referentiel/boursorama_peapme_final.csv`
- **⚠️ Note :** 0 rows depuis 2026-03-31 — problème de matching à investiguer

#### `bronze.google_news_rss`
- **Source :** Google News RSS (2 flux : Euronext Growth + PME Bourse FR)
- **Grain :** row_id
- **GCS :** `gs://project-pea-pme/rss_google_news/`
- **Matching :** fuzzy match contre référentiel

#### `bronze.abcbourse_rss`
- **Source :** ABCBourse RSS
- **Dédup locale :** Fichier `abcbourse_data_raw.json` (état persisté entre runs)
- **GCS :** Backup du JSON local
- **Grain :** row_id

#### `bronze.amf`
- **Source :** AMF API `flux-amf-new-prod` (REST JSON v2, sans clé API)
- **Contenu :** Informations réglementées continues (insider transactions, publications financières)
- **Colonnes clés :** record_id, societe, isin, type_information, publication_ts, pdf_url, pdf_download_status, titre
- **GCS :** `gs://project-pea-pme/amf/` (PDFs, XMLs, JSONL — pour auditabilité)
- **Grain :** record_id

#### `bronze.boursorama`
- **Source :** Scraping page listing PEA-PME Boursorama
- **Contenu :** 571 sociétés avec ISIN, ticker Boursorama, données de marché
- **Type :** External table → source depuis GCS CSV
- **GCS :** `gs://project-pea-pme/boursorama_peapme_final.csv`
- **Fréquence :** 1 fois par mois (1er du mois)

---

## COUCHE SILVER

### Modèles dbt

#### `silver.rss_articles` (dbt)
- **Sources :** `bronze.yahoo_rss` + `bronze.google_news_rss`
- **Transformation :** Union + déduplication + standardisation colonnes
- **Colonnes :** isin, matched_name, title, published_at, source, row_id
- **Usage :** Source pour scoring sentiment Groq

#### `silver.yahoo_ohlcv_clean` (dbt)
- **Source :** `bronze.yfinance_ohlcv`
- **Transformation :** Déduplication (isin, Date), filtrage prix invalides, ajout `last_trading_date`
- **Matérialisation :** table, partition mensuelle
- **Support `full_refresh` :** Oui (reconstruction complète)

### Modules Python

#### `silver.yahoo_ohlcv` (Python — `src/silver/compute_silver.py`)
- **Source :** `silver.yahoo_ohlcv_clean`
- **Traitement :** Calcul de 7 indicateurs techniques par ISIN via library `ta`
- **Indicateurs calculés :**
  - `RSI_14` — Relative Strength Index (14 jours)
  - `MACD` — Moving Average Convergence Divergence
  - `MACD_signal` — Signal MACD
  - `BB_upper` — Bande de Bollinger haute
  - `BB_lower` — Bande de Bollinger basse
  - `SMA_50` — Moyenne mobile simple 50 jours
  - `SMA_200` — Moyenne mobile simple 200 jours
  - `EMA_20` — Moyenne mobile exponentielle 20 jours
- **Mode écriture :** WRITE_TRUNCATE sur 1er ISIN, WRITE_APPEND sur les suivants

#### `silver.amf_insider_signals` (Python + Groq → dbt)
- **Source :** `bronze.amf` (PDFs téléchargés)
- **Filtre :** type_information = 'Informations réglementées continues', titre contient 'DIRIGEANT' ou 'ARTICLE 19', 90 derniers jours
- **Extraction :** `pdfplumber` → texte → Groq `llama-3.1-8b-instant`
- **Schema extrait :** `{signals: [{dirigeant, type_operation (Achat/Vente), montant (float), date_signal (YYYY-MM-DD)}]}`
- **Validation :** heuristiques (nom en 2+ parties, mots interdits)
- **Mode écriture :** WRITE_APPEND avec schema updates

#### `silver.amf_financial_signal` (Python + Groq → dbt)
- **Source :** `bronze.amf` (publications financières)
- **Extraction :** PDF → Groq → métriques financières
- **Staging :** `work.amf_financial_signal_staging` (table temporaire)
- **dbt :** Merge staging → `silver.amf_financial_signal`

#### `silver.companies` (dbt)
- **Source :** `bronze.boursorama`
- **Transformation :** Snapshot incrémental
- **Colonnes :** isin, name, ticker_bourso

---

## COUCHE GOLD

### Modèles dbt + Python

#### `gold.stocks_score` (dbt — `dbt/models/gold/stocks_score.sql`)
- **Source :** `silver.yahoo_ohlcv`
- **Transformation :** 5 signaux techniques → score_technique [0–10]
- **Grain :** (isin, date)
- **Colonnes produites :**
  - `isin`, `company_name`, `yf_ticker`, `date`, `Close`
  - `rsi_signal`, `macd_signal`, `golden_cross_signal`, `bollinger_signal`, `trend_signal` (chacun [0/1/2])
  - `score_technique` = somme des 5 signaux [0–10]
  - `score_7d_avg` = moyenne mobile 7j du score technique
  - `score_14d_avg` = moyenne mobile 14j du score technique
  - `is_latest` = TRUE pour le dernier jour disponible par ISIN
- **Join :** `silver.companies` pour enrichir `company_name`

#### `gold.article_sentiment` (Python — `src/gold/sentiment_scorer.py`)
- **Source :** `silver.rss_articles` (articles sans score = LEFT JOIN anti)
- **LLM :** Groq `llama-3.1-8b-instant`
- **Input :** title + summary
- **Output :** `{score: int, reason: str}`
- **Schema :** row_id, isin, ticker_bourso, matched_name, title, published_at, sentiment_score (int64), sentiment_reason, groq_model, scored_at
- **Mode écriture :** APPEND (incrémental, ne rescote pas)

#### `gold.score_news` (dbt)
- **Sources :** `silver.rss_articles` + `gold.article_sentiment`
- **Fenêtre :** 45 jours glissants
- **Colonnes :** isin, matched_name, ticker_bourso, mention_count_45d, avg_sentiment_45d, investment_score [1–10], score_date
- **Grain :** (isin, score_date)

#### `gold.score_insider` (dbt — via `gold.score_insider_signals`)
- **Source :** `silver.amf_insider_signals`
- **Scoring :** Variables dbt — achat_base_score=100, vente_base_score=-50, cluster_bonus_weight=0.5, max_log_amount=6
- **UPSERT :** Via table staging + MERGE sur source_record_id
- **Colonnes :** isin, societe, signal_date, insider_names, num_operations, total_amount, score_1_10

#### `gold.financials_score` (dbt)
- **Source :** `silver.amf_financial_signal`
- **4 métriques :** CA growth (%), marge opérationnelle (%), levier dette/EBITDA, FCF yield (%)
- **Colonnes :** ticker, isin, date_cloture_exercice, score_fondamental [0–10], ca_growth_pct, marge_op_pct, levier_dette_ebitda, fcf_yield_pct, nb_metrics, coverage_score
- **Grain :** (isin, date_cloture_exercice)

#### `gold.company_scores` (dbt)
- **Sources :** `gold.stocks_score`, `gold.score_news`, `gold.score_insider`, `gold.financials_score`
- **Score composite :** 0.25 × chaque dimension (4 × 25%)
- **Colonnes :** isin, name, ticker_bourso, score_date, composite_score [0–10], score_news, score_stock, score_insider, score_financials
- **Grain :** (isin, score_date)
- **Refresh :** À chaque run `yfinance-ohlcv-pipeline` + `bronze-silver-rss`

---

## Flux de données complet (timeline quotidienne)

```
08h00 (Lun-Ven)
  └─ bronze-abcbourse-rss → bronze.abcbourse_rss

00h00, 04h00, 08h00, 12h00, 16h00, 20h00 (tous les jours)
  └─ bronze-silver-rss
       ├─ bronze.yahoo_rss (si dispo)
       ├─ bronze.google_news_rss
       ├─ dbt → silver.rss_articles
       └─ silver-gold-rss
            ├─ Groq → gold.article_sentiment
            └─ dbt → gold.score_news + gold.company_scores

19h30 (Lun-Ven)
  └─ yfinance-ohlcv-pipeline
       ├─ bronze.yfinance_ohlcv (yfinance)
       ├─ dbt → silver.yahoo_ohlcv_clean
       ├─ Python → silver.yahoo_ohlcv (7 indicateurs)
       ├─ dbt → gold.stocks_score
       └─ dbt → gold.company_scores

00h00 (toutes les 4h)
  └─ bronze-amf-flux → bronze.amf
       └─ silver-insider-parser (déclenché)
            └─ Groq → silver.amf_insider_signals
                 └─ gold-insider-scoring (déclenché)
                      └─ dbt → gold.score_insider_signals

1er avril / 1er octobre 00h00
  └─ amf-pipeline
       ├─ bronze.amf
       ├─ silver.amf_financial_signal
       └─ dbt → gold.financials_score

1er du mois 08h00
  └─ bronze-silver-gold-boursorama
       ├─ bronze.boursorama (scraping)
       ├─ dbt → silver.companies
       └─ dbt → gold.company_scores
```
