# Synthèse PPT — RNCP37827 BC01
# Collecte, stockage et mise à disposition des données

**Candidat :** Mathias Pinault
**Certification :** RNCP37827 — Bloc de compétences 01
**Projet :** PEA-PME Pulse
**Durée soutenance :** 15 min présentation + 10 min questions jury
**Format cible :** 10 à 12 slides principales + annexes code (réservées aux questions)

---

## DISTINCTION RAPPORT / PRÉSENTATION ORALE

Le rapport écrit est déjà finalisé. La présentation orale n'en est PAS un résumé.

**Rapport :** exhaustif, citations de code avec numéros de lignes, justifications détaillées, lu attentivement par le jury avant la soutenance.

**Présentation orale :** démonstration vivante en 15 min. Le jury a lu le rapport. L'objectif est de convaincre par la preuve — visuels, architecture, résultats concrets, démo live si possible. Pas de lecture de code à l'écran : les extraits de code essentiels sont placés en annexe, à sortir uniquement si le jury pose une question technique précise.

---

## CONTEXTE PROJET (pour que Claude comprenne le projet)

### Problème métier
Les PME françaises cotées sur Euronext Growth (éligibles PEA-PME) manquent de visibilité. Les outils d'analyse existent pour les grands groupes du CAC40, pas pour les 462 PME de cette catégorie. PEA-PME Pulse est un système de scoring automatisé qui produit chaque soir un score composite [0-10] sur 4 dimensions pour chaque société.

### Équipe et périmètre Mathias
Projet porté par une équipe de 4 personnes. Chaque membre est responsable d'une dimension :
- **Mathias** : prix boursiers (yfinance) + infrastructure VM + API REST
- **Camille** : actualités (RSS + analyse de sentiment via Groq/LLM)
- Membres 3 et 4 : transactions dirigeants (AMF insiders) + fondamentaux financiers (AMF)

Le score composite agrège les 4 dimensions à pondération égale (25% chacune).

### Stack technique
- **Cloud :** Google Cloud Platform — BigQuery, GCS, VM e2-small (europe-west1-b)
- **Orchestration :** Prefect self-hosted sur VM (Docker)
- **Transformation :** dbt (SQL) + Python (lib `ta` pour indicateurs techniques)
- **API :** FastAPI + Uvicorn + nginx (reverse proxy)
- **Authentification GCP :** ADC — Application Default Credentials (zéro fichier de clé JSON)
- **Versioning :** Git + GitHub, branches feature, pull requests
- **Dépôt :** https://github.com/Mathias-PP/pea-pme-pulse

### Chiffres clés à retenir
- 462 sociétés PEA-PME analysées chaque soir en semaine
- Cron : lundi-vendredi à 19h30 (juste après clôture des marchés français)
- 5 étapes séquentielles orchestrées par Prefect (Bronze → Silver dbt → Silver Python → Gold dbt × 2)
- 7 indicateurs techniques calculés (RSI, MACD, signal MACD, Bollinger haut/bas, SMA50, SMA200, EMA20)
- 5 signaux haussier/neutre/baissier (0/1/2 pts) → score technique [0-10]
- 4 dimensions × 25% → score composite [0-10]
- 9 endpoints API REST documentés automatiquement (Swagger UI)
- 1 dashboard Streamlit consomme l'API avec cache 1h

---

## ARCHITECTURE TECHNIQUE

### Flux complet
```
yfinance (Yahoo Finance)
    ↓
bronze.yfinance_ohlcv       — données brutes OHLCV, WRITE_APPEND
    ↓ dbt
silver.yahoo_ohlcv_clean    — déduplication, filtres qualité
    ↓ Python (lib ta)
silver.yahoo_ohlcv          — + RSI, MACD, Bollinger, SMA, EMA
    ↓ dbt
gold.stocks_score           — 5 signaux → score technique [0-10]
    ↓ dbt
gold.company_scores         — composite 25%×4 dimensions [0-10]
    ↓
API FastAPI → Dashboard Streamlit
```

### Infrastructure VM
- e2-small GCP : Prefect (Docker), nginx (Docker), Grafana (Docker), Prometheus (Docker)
- FastAPI : service systemd hors Docker (accès au metadata server GCP pour ADC)
- nginx : reverse proxy — une seule IP publique (35.241.252.5), plusieurs services
- Prefect UI : http://35.241.252.5/ — historique des runs, relance manuelle, logs

---

## SLIDES PRINCIPALES (10-12 slides)

### Slide 1 — Titre
- "PEA-PME Pulse — Scoring automatisé de 462 PME françaises"
- Mathias Pinault | RNCP37827 — BC01 | Avril 2026
- Visuel sobre (logo, fond neutre)

### Slide 2 — Contexte et problème (30 sec)
**Contenu :**
- 462 PME cotées Euronext Growth, éligibles PEA-PME
- Problème : pas d'outils d'analyse pour cette catégorie (contrairement au CAC40)
- Solution : pipeline quotidien automatisé → score composite [0-10] pour chaque société
- Équipe de 4 — Mathias : dimension "prix boursiers" + infrastructure + API
**Visuels suggérés :** logo Euronext Growth, chiffre "462 PME" mis en avant

### Slide 3 — Architecture globale (45 sec)
**Contenu :**
- Diagramme du flux : Sources → Bronze → Silver → Gold → API → Streamlit
- Mettre en évidence l'architecture en 3 couches (medallion)
- Indiquer l'orchestrateur Prefect et le cron 19h30
- Surligner le périmètre Mathias
**Visuels suggérés :** diagramme de flux avec couleurs par couche (bronze/argent/or), icône horloge pour le cron

### Slide 4 — C1 : Collecte automatisée (2 min)
**Contenu :**
- Source : Yahoo Finance via bibliothèque Python yfinance — données OHLCV (Open, High, Low, Close, Volume)
- Résolution ISIN → ticker Yahoo en 2 étapes : override manuel si connu, résolution auto sinon
- Première ingestion : historique complet (jusqu'à 20 ans) — nécessaire pour le warmup des indicateurs
- Runs suivants : uniquement les nouvelles lignes depuis la dernière date connue par ISIN (idempotence)
- Écriture en BigQuery : schéma explicite 11 colonnes, mode WRITE_APPEND, `ingested_at` pour traçabilité
- Orchestration : Prefect, cron `30 19 * * 1-5`, relance automatique en cas d'échec
**Visuels suggérés :** timeline du run quotidien, icône entonnoir pour la collecte, screenshot Prefect UI (flow runs)

### Slide 5 — C2 : Nettoyage des données (1 min 30)
**Contenu :**
- Modèle dbt `silver.yahoo_ohlcv_clean` — transformation SQL déclarative
- 3 règles de nettoyage : prix invalides exclus (Close > 0, Open > 0), sessions incomplètes exclues (marché pas encore fermé à 18h Paris), doublons supprimés (une seule ligne par ISIN + date, la plus récente)
- Partitionnement par mois + clustering par ISIN : BigQuery ne lit que les partitions concernées
- Métadonnées Bronze (Dividends, Stock Splits, ingested_at) non propagées en Silver — séparation des responsabilités
**Visuels suggérés :** tableau avant/après nettoyage, icône filtre, schéma partitionnement

### Slide 6 — C2/C3 : Indicateurs techniques (1 min 30)
**Contenu :**
- Script Python `compute_silver.py` — bibliothèque `ta` (Technical Analysis)
- 7 indicateurs calculés sur la colonne Close : RSI 14j, MACD + signal, Bollinger haut/bas, SMA 50j et 200j, EMA 20j
- Warmup : jusqu'à 200 jours d'historique nécessaires — justifie le téléchargement complet à la première exécution
- NaN propagés en Silver (comportement normal de la lib) → traités comme signal neutre (1 pt) en Gold
- Idempotence : WRITE_TRUNCATE sur le premier ISIN (repart de zéro), WRITE_APPEND pour les 461 suivants
**Visuels suggérés :** graphique cours + indicateurs superposés (RSI, MACD), tableau des 7 indicateurs

### Slide 7 — C3 : Score technique (1 min)
**Contenu :**
- 5 signaux issus des indicateurs : RSI, MACD, Golden Cross (SMA50 > SMA200), Bollinger, Tendance (Close > EMA20)
- Chaque signal vaut 0 (baissier), 1 (neutre) ou 2 (haussier) — NaN → 1 par défaut
- Score technique = somme des 5 signaux → [0-10]
- Moyennes mobiles 7j et 14j pour lisser le bruit journalier
- Modèle dbt `gold.stocks_score` — `is_latest = TRUE` pour identifier le dernier jour de bourse par ISIN
**Visuels suggérés :** tableau des 5 signaux avec exemples de valeurs, jauge [0-10], heatmap signaux

### Slide 8 — C3 : Score composite (45 sec)
**Contenu :**
- 4 dimensions agrégées à pondération égale : technique (yfinance), actualités (sentiment RSS), insiders (AMF), fondamentaux (AMF)
- Normalisation [1-10] → [0-10] pour homogénéiser les sources
- Dimension absente → 5.0 (point médian) — ne pénalise pas une société sans données
- Score composite [0-10] — grain : (isin, score_date), snapshot quotidien historisé
**Visuels suggérés :** graphique radar à 4 axes, formule composite 25%×4

### Slide 9 — C4 : Base de données BigQuery (1 min 30)
**Contenu :**
- Choix BigQuery : data warehouse managé GCP, SQL standard, partitionnement natif, zéro serveur à maintenir
- Architecture 3 datasets : Bronze (brut, append), Silver (propre + enrichi), Gold (scores métier)
- Clé de jointure universelle : `isin` — aucune clé étrangère en BigQuery, ordre garanti par Prefect
- ADC : zéro clé JSON stockée — la VM s'authentifie via le metadata server GCP, tokens éphémères
- MLD complet en annexe
**Visuels suggérés :** diagramme Bronze/Silver/Gold avec colonnes clés par table, icône cadenas pour ADC

### Slide 10 — C5 : API FastAPI (2 min)
**Contenu :**
- FastAPI (Python) — framework REST, contrat de données typé (Pydantic), documentation auto
- 9 endpoints exposant tous les scores : technique, composite, actualités, insiders, fondamentaux, historique
- Swagger UI à /docs : documentation interactive, testable en live dans le navigateur
- Sécurité : validation ISIN par regex (protection injection SQL), connexion BQ unique en cache
- Hébergement : service systemd sur VM GCP, derrière nginx (reverse proxy)
- Consommation : dashboard Streamlit avec cache 1h — zéro requête BQ à chaque clic
**Visuels suggérés :** screenshot Swagger UI, liste des 9 endpoints, screenshot dashboard Streamlit

### Slide 11 — Démo live (optionnel, si connexion disponible) (1 min)
**Contenu :**
- Ouvrir http://35.241.252.5/docs → montrer les endpoints
- Tester GET /gold/stocks-score/latest → résultat JSON en direct
- Ouvrir Prefect UI → montrer le dernier run, les 5 étapes
**Alternative si pas de connexion :** screenshots préparés des deux interfaces

### Slide 12 — Conclusion (30 sec)
**Contenu :**
- Pipeline quotidien automatisé couvrant les 5 compétences BC01 de bout en bout
- Travail en équipe : Git + GitHub, branches feature, pull requests, revues de code
- Perspectives : HTTPS/TLS, authentification API par clé, alertes automatiques
- Dépôt : https://github.com/Mathias-PP/pea-pme-pulse

---

## ANNEXES CODE (slides réservées aux questions du jury)

Ces slides ne sont PAS présentées. Elles sont accessibles en fin de deck pour répondre aux questions techniques.

### Annexe A — C1 : Résolution ISIN → ticker

```python
# src/bronze/yahoo_ohlcv_bronze.py

def isin_to_yf_ticker(isin: str, overrides: dict) -> str | None:
    if isin in overrides:                    # 1. Override manuel (prioritaire)
        return overrides[isin]
    try:
        info = yf.Ticker(isin).info          # 2. Résolution automatique Yahoo
        return info.get("symbol") or None
    except Exception as e:
        logger.warning(f"Erreur résolution ISIN {isin} : {e}")
        return None
```

### Annexe B — C1 : Idempotence incrémentale

```python
# src/bronze/yahoo_ohlcv_bronze.py

def get_last_dates(client) -> dict[str, date]:
    query = f"SELECT isin, MAX(Date) as last_date FROM `{BQ_TABLE_REF}` GROUP BY isin"
    try:
        return {row["isin"]: row["last_date"] for row in client.query(query).result()}
    except Exception:
        return {}  # Première exécution — table absente

def fetch_ohlcv(isin, yf_ticker, start_date=None):
    if start_date is None:
        df = yf.Ticker(yf_ticker).history(period="max")          # Historique complet
    else:
        df = yf.Ticker(yf_ticker).history(start=start_date + timedelta(days=1))
```

### Annexe C — C2 : Nettoyage dbt (silver.yahoo_ohlcv_clean)

```sql
-- dbt/models/silver/yahoo_ohlcv_clean.sql

with raw as (
    select * from {{ source('bronze', 'yfinance_ohlcv') }}
    where isin is not null
      and Close > 0
      and Open > 0
      and Date <= if(
          extract(hour from datetime(current_timestamp(), 'Europe/Paris')) >= 18,
          current_date('Europe/Paris'),
          date_sub(current_date('Europe/Paris'), interval 1 day)
      )
),
deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by isin, Date
        order by ingested_at desc
    ) = 1
)
select Date, Open, High, Low, Close, Volume, isin, yf_ticker from deduped
```

### Annexe D — C2 : Calcul des indicateurs techniques

```python
# src/silver/compute_silver.py

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]
    df["RSI_14"]      = RSIIndicator(close=close, window=14).rsi()
    df["MACD"]        = MACD(close=close).macd()
    df["MACD_signal"] = MACD(close=close).macd_signal()
    df["BB_upper"]    = BollingerBands(close=close, window=20, window_dev=2).bollinger_hband()
    df["BB_lower"]    = BollingerBands(close=close, window=20, window_dev=2).bollinger_lband()
    df["SMA_50"]      = SMAIndicator(close=close, window=50).sma_indicator()
    df["SMA_200"]     = SMAIndicator(close=close, window=200).sma_indicator()
    df["EMA_20"]      = EMAIndicator(close=close, window=20).ema_indicator()
    return df
```

### Annexe E — C3 : Signaux haussier/neutre/baissier

```sql
-- dbt/models/gold/stocks_score.sql (extrait)

case
    when RSI_14 < 35  then 2.0   -- Survente → haussier
    when RSI_14 < 65  then 1.0   -- Zone neutre
    when RSI_14 >= 65 then 0.0   -- Surachat → baissier
    else 1.0                      -- NaN → neutre
end as rsi_signal,

case
    when SMA_50 is null or SMA_200 is null then 1.0
    when SMA_50 > SMA_200                  then 2.0   -- Golden cross
    else 0.0
end as golden_cross_signal,

round(rsi_signal + macd_signal + golden_cross_signal
      + bollinger_signal + trend_signal, 1) as score_technique
```

### Annexe F — C3 : Score composite

```sql
-- dbt/models/gold/company_scores.sql (extrait)

round(coalesce((n.investment_score - 1) / 9.0 * 10, 5.0), 2) as score_news,
round(coalesce(st.score_technique,                  5.0), 2) as score_stock,
round(coalesce((i.score_1_10 - 1) / 9.0 * 10,      5.0), 2) as score_insider,
round(coalesce(f.score_fondamental,                 5.0), 2) as score_financials,

round(
      coalesce((n.investment_score - 1) / 9.0 * 10, 5.0) * 0.25
    + coalesce(st.score_technique,                   5.0) * 0.25
    + coalesce((i.score_1_10 - 1) / 9.0 * 10,       5.0) * 0.25
    + coalesce(f.score_fondamental,                  5.0) * 0.25,
2) as composite_score
```

### Annexe G — C4 : Schémas des tables

**bronze.yfinance_ohlcv** — 11 colonnes, WRITE_APPEND
```
Date DATE | Open FLOAT | High FLOAT | Low FLOAT | Close FLOAT
Volume INTEGER | Dividends FLOAT | Stock Splits FLOAT
isin STRING | yf_ticker STRING | ingested_at TIMESTAMP
```

**silver.yahoo_ohlcv** — 17 colonnes, WRITE_TRUNCATE + APPEND
```
[colonnes Bronze] + RSI_14 | MACD | MACD_signal | BB_upper | BB_lower
                           | SMA_50 | SMA_200 | EMA_20 | computed_at
```

**gold.stocks_score**
```
isin | company_name | yf_ticker | date | Close
rsi_signal | macd_signal | golden_cross_signal | bollinger_signal | trend_signal
score_technique | score_7d_avg | score_14d_avg | is_latest
```

**gold.company_scores** — snapshot quotidien historisé
```
isin | name | ticker_bourso | snapshot_date | score_date
score_news | score_stock | score_insider | score_financials | composite_score
```

### Annexe H — C5 : Endpoint et modèle Pydantic

```python
# src/api/main.py

class StockScore(BaseModel):
    isin: str
    company_name: str
    yf_ticker: str
    date: date
    close: float
    score_technique: float
    score_7d_avg: float | None
    rsi_signal: float
    macd_signal: float
    golden_cross_signal: float
    bollinger_signal: float
    trend_signal: float

@app.get("/gold/stocks-score/latest", response_model=list[StockScore])
def get_latest_scores():
    query = f"""
        SELECT isin, company_name, yf_ticker, date, Close AS close,
               score_technique, score_7d_avg, rsi_signal, macd_signal,
               golden_cross_signal, bollinger_signal, trend_signal
        FROM `{GCP_PROJECT}.gold.stocks_score`
        WHERE is_latest = TRUE
        ORDER BY score_technique DESC
    """
    return [dict(row) for row in get_bq_client().query(query).result()]
```

### Annexe I — C5 : Validation sécurité

```python
# Validation ISIN par regex — protection injection SQL
isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
invalid = [i for i in isins if not isin_re.match(i)]
if invalid:
    raise HTTPException(status_code=422, detail=f"ISINs invalides : {invalid}")
```

### Annexe J — MLD (Modèle Logique des Données)

```dbml
Table yfinance_library {
  yf_ticker varchar
  Date date
  note: "Bibliothèque Python — wrapper Yahoo Finance. Retourne OHLCV par ticker et date."
}
Table bronze_yfinance_ohlcv {
  isin varchar | yf_ticker varchar | Date date | Open float | High float
  Low float | Close float | Volume integer | Dividends float
  "Stock Splits" float | ingested_at timestamp
  note: "Grain : (isin, Date). WRITE_APPEND."
}
Table silver_yahoo_ohlcv_clean {
  isin varchar | Date date | Open float | High float | Low float
  Close float | Volume integer | yf_ticker varchar
  note: "Grain : (isin, Date). dbt incremental insert_overwrite. Partitionné mois, clustered isin."
}
Table silver_yahoo_ohlcv {
  isin varchar | yf_ticker varchar | Date date | Open float | High float
  Low float | Close float | Volume integer | RSI_14 float | MACD float
  MACD_signal float | BB_upper float | BB_lower float | SMA_50 float
  SMA_200 float | EMA_20 float | computed_at datetime
  note: "Grain : (isin, Date). WRITE_TRUNCATE 1er ISIN, WRITE_APPEND suivants."
}
Table silver_companies {
  isin varchar [pk] | name varchar | ticker_bourso varchar | snapshot_date date
  note: "Modèle dbt silver.companies. Source : boursorama_peapme_final.csv (GCS)."
}
Table gold_stocks_score {
  isin varchar | company_name varchar | yf_ticker varchar | date date | Close float
  rsi_signal float | macd_signal float | golden_cross_signal float
  bollinger_signal float | trend_signal float | score_technique float
  score_7d_avg float | score_14d_avg float | is_latest boolean
  note: "Grain : (isin, date). Matérialisé en TABLE — recalcul complet à chaque run."
}
Table gold_company_scores {
  isin varchar | name varchar | ticker_bourso varchar | snapshot_date date
  score_date date | score_news float | score_stock float
  score_insider float | score_financials float | composite_score float
  note: "Grain : (isin, score_date). dbt incremental. Snapshot quotidien historisé."
}

Ref: bronze_yfinance_ohlcv.(yf_ticker, Date) <> yfinance_library.(yf_ticker, Date)
Ref: silver_yahoo_ohlcv_clean.(isin, Date) > bronze_yfinance_ohlcv.(isin, Date)
Ref: silver_yahoo_ohlcv.(isin, Date) - silver_yahoo_ohlcv_clean.(isin, Date)
Ref: gold_stocks_score.(isin, date) - silver_yahoo_ohlcv.(isin, Date)
Ref: gold_stocks_score.isin > silver_companies.isin
Ref: gold_company_scores.isin > gold_stocks_score.isin
```

---

## POINTS D'ATTENTION (NE PAS SE TROMPER)

| Sujet | Ce qu'il ne faut PAS dire | Ce qu'il faut dire |
|---|---|---|
| Score composite | "35% technique, 25% news..." | "25% pour chaque dimension — pondération égale" |
| FastAPI | "FastAPI tourne dans Docker" | "FastAPI tourne en systemd, hors Docker" |
| Dimension absente | "Le score baisse si une dimension manque" | "COALESCE à 5.0 — point médian, ne pénalise pas" |
| NaN indicateurs | "Les NaN sont supprimés" | "NaN propagés en Silver, traités comme neutre (1pt) en Gold" |
| stocks_score | "C'est un modèle incremental" | "Matérialisé en TABLE — recalcul complet à chaque run" |
| investment_score | "Un sentiment de X donne un score de Y" | "C'est un rang relatif parmi toutes les sociétés du jour" |

---

## QUESTIONS JURY — PRÉPARATION

**Q : Pourquoi BigQuery plutôt qu'une base SQL classique ?**
BigQuery est nativement intégré à GCP (ADC, pas de clé JSON), partitionnement et clustering sans configuration, facturation à la requête, zéro serveur à maintenir. PostgreSQL aurait nécessité un serveur dédié et une gestion des credentials plus complexe.

**Q : Comment garantissez-vous l'absence de doublons ?**
Deux mécanismes complémentaires. En Bronze : idempotence incrémentale via `get_last_dates()` — on ne télécharge que les lignes postérieures à la dernière date connue. En Silver dbt : `QUALIFY ROW_NUMBER() OVER (PARTITION BY isin, Date ORDER BY ingested_at DESC) = 1` — une seule ligne par (isin, date).

**Q : Que se passe-t-il si le pipeline échoue à mi-chemin ?**
Prefect rejoue automatiquement. Le prochain run Bronze repart de la dernière date complète par ISIN. Le Silver est entièrement recalculé (WRITE_TRUNCATE sur premier ISIN). Aucune donnée corrompue ne persiste.

**Q : L'API est-elle sécurisée ?**
Les données exposées sont publiques (cours boursiers, actualités). La validation ISIN par regex protège contre les injections SQL. En production, HTTPS via Certbot et une clé API dans l'en-tête HTTP seraient les priorités suivantes.

**Q : Pourquoi yfinance et pas une API officielle ?**
L'API officielle Yahoo Finance a été fermée en 2017. yfinance est le wrapper Python non-officiel de référence, maintenu activement, données de qualité institutionnelle, historique jusqu'à 20 ans.

**Q : Qu'est-ce que ADC ?**
Application Default Credentials. La VM GCP a un Service Account attaché. Le metadata server GCP délivre des tokens temporaires automatiquement — zéro fichier de clé à stocker ou renouveler.

**Q : Pourquoi FastAPI tourne hors Docker ?**
FastAPI accède au metadata server GCP pour ADC. Ce serveur est accessible nativement depuis la VM. Dans Docker, le réseau est isolé et nécessiterait une configuration supplémentaire. En systemd, ça fonctionne sans configuration.
