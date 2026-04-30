# 06 — API FastAPI

## Déploiement

- **Fichier :** `src/api/main.py`
- **Serveur :** uvicorn, port 8000
- **Hébergement :** systemd service `pea-pme-api` sur VM GCP
  - WorkingDirectory : `/home/mathias/pea-pme-pulse`
  - Venv : `/home/mathias/api-venv/`
- **Accès externe :** via nginx `location ~ ^/(gold|overview|health|metrics|docs|openapi\.json)`
  - Proxy vers `http://10.132.0.2:8000` (IP interne VM)

## Détails techniques

- **BQ Client :** `@lru_cache(maxsize=1)` — instance unique partagée
- **GCP Project :** `os.environ.get("GCP_PROJECT_ID", "bootcamp-project-pea-pme")`
- **Gold Dataset :** `"gold"` (constante)
- **Instrumentation :** `Instrumentator().instrument(app).expose(app)` → endpoint `/metrics` *(branche `feat/monitoring-grafana`, non encore dans main)*
- **Validation :** ISIN validé par regex `^[A-Z]{2}[A-Z0-9]{10}$`

---

## Endpoints

### Système

#### `GET /health` *(feat/monitoring-grafana — non encore dans main)*
```json
{"status": "ok", "timestamp": "2026-04-11T01:10:36.738990"}
```
Timestamp en UTC ISO 8601.

#### `GET /metrics` *(feat/monitoring-grafana — non encore dans main)*
Métriques Prometheus (scraped toutes les 30s par Prometheus).

#### `GET /docs`
Interface Swagger UI auto-générée.

#### `GET /openapi.json`
Schéma OpenAPI complet.

---

### Discovery

#### `GET /overview`
```json
{
  "project": "bootcamp-project-pea-pme",
  "datasets": {
    "bronze": ["yfinance_ohlcv", "yahoo_rss", ...],
    "silver": ["rss_articles", ...],
    "gold": ["stocks_score", ...]
  }
}
```
Liste tous les datasets et leurs tables via `client.list_datasets()` + `client.list_tables()`.

#### `GET /overview/gold`
```json
{
  "project": "bootcamp-project-pea-pme",
  "dataset": "gold",
  "tables": ["stocks_score", "company_scores", ...]
}
```

---

### Stocks Score

#### `GET /gold/stocks-score/latest`
**Source BQ :** `gold.stocks_score WHERE is_latest = TRUE ORDER BY score_technique DESC, score_7d_avg DESC`

**Réponse** `List[StockScore]` :
```python
{
    isin: str,
    company_name: str,
    yf_ticker: str,
    date: date,
    close: float,           # Cours de clôture (colonne BQ "Close")
    score_technique: float,
    score_7d_avg: float | None,
    rsi_signal: float,
    macd_signal: float,
    golden_cross_signal: float,
    bollinger_signal: float,
    trend_signal: float
}
```

#### `GET /gold/stocks-score/history`
**Paramètres :**
- `isins` (requis) : `List[str]` — liste d'ISINs
- `days` (défaut=30, min=1, max=365) : fenêtre temporelle

**Validation :** ISINs validés par regex — HTTP 422 si invalide.

**Source BQ :**
```sql
WHERE isin IN (...)
  AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
ORDER BY isin, date
```

**Réponse** `List[ScoreHistory]` :
```python
{isin, company_name, date, score_technique, score_7d_avg}
```

---

### Composite Scores

#### `GET /gold/company-scores/latest`
**Source BQ :**
```sql
WHERE score_date = (SELECT MAX(score_date) FROM gold.company_scores)
ORDER BY composite_score DESC
```

**Réponse** `List[CompanyScore]` :
```python
{
    isin, name, ticker_bourso,
    score_date: date,
    composite_score: float,   # [0–10]
    score_news: float,
    score_stock: float,
    score_insider: float,
    score_financials: float
}
```

---

### News

#### `GET /gold/score-news/latest`
**Source BQ :**
```sql
WHERE score_date = (SELECT MAX(score_date) FROM gold.score_news)
ORDER BY investment_score DESC
```

**Réponse** `List[NewsScore]` :
```python
{
    isin, matched_name, ticker_bourso,
    mention_count_45d: int,
    avg_sentiment_45d: float,
    investment_score: int,    # [1–10]
    score_date: date
}
```

#### `GET /gold/article-sentiment/latest`
**Source BQ :**
```sql
WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
ORDER BY published_at DESC
LIMIT 500
```

**Réponse** `List[ArticleSentiment]` :
```python
{
    isin, matched_name, title,
    published_at: datetime,
    sentiment_score: float,
    sentiment_reason: str
}
```

---

### Insider

#### `GET /gold/score-insider/latest`
**Source BQ :**
```sql
WHERE signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
ORDER BY signal_date DESC, score_1_10 DESC
```

**Réponse** `List[InsiderScore]` :
```python
{
    isin, societe,
    signal_date: date,
    insider_names: str,
    num_operations: int,
    total_amount: float,
    score_1_10: float
}
```

---

### Financials

#### `GET /gold/financials-score/latest`
**Source BQ :**
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY isin ORDER BY date_cloture_exercice DESC) = 1
ORDER BY score_fondamental DESC
```
→ Une seule ligne par ISIN (exercice le plus récent).

**Réponse** `List[FinancialsScore]` :
```python
{
    ticker: str,
    isin: str | None,
    date_cloture_exercice: date,
    score_fondamental: float,
    ca_growth_pct: float | None,
    marge_op_pct: float | None,
    levier_dette_ebitda: float | None,
    fcf_yield_pct: float | None,
    nb_metrics: int,
    coverage_score: float
}
```

---

## Tableau récapitulatif

| Endpoint | Méthode | Source BQ | Tri | Limite |
|---|---|---|---|---|
| `/health` | GET | — | — | — |
| `/metrics` | GET | — | — | — |
| `/overview` | GET | Toutes | — | — |
| `/overview/gold` | GET | gold.* | — | — |
| `/gold/stocks-score/latest` | GET | gold.stocks_score | score DESC | is_latest |
| `/gold/stocks-score/history` | GET | gold.stocks_score | isin, date | params |
| `/gold/company-scores/latest` | GET | gold.company_scores | composite DESC | MAX date |
| `/gold/score-news/latest` | GET | gold.score_news | investment DESC | MAX date |
| `/gold/score-insider/latest` | GET | gold.score_insider | date DESC | 45j |
| `/gold/financials-score/latest` | GET | gold.financials_score | score DESC | 1/ISIN |
| `/gold/article-sentiment/latest` | GET | gold.article_sentiment | date DESC | 500 |
