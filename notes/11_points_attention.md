# 11 — Points d'attention pour la présentation

Incohérences docs/code, bugs connus et dette technique à connaître avant la présentation.

---

## 1. Poids du score composite : README ≠ code dbt

**Documentation existante (`docs/architecture.md`, README) :**
> Score composite = 35% technique + 25% news + 25% insiders + 15% fondamentaux

**Code dbt réel (`dbt/models/gold/company_scores.sql`) :**
```sql
composite_score = 0.25 * score_news
               + 0.25 * score_stock
               + 0.25 * score_insider
               + 0.25 * score_financials
```

**→ Vérité : 25% × 4 dimensions (pondération égale)**

À corriger dans la présentation. Ne pas citer le README sur ce point.

---

## 2. Flows Prefect orphelins ✅ Résolu

`bronze-yfinance-ohlcv` et `silver-yfinance-ohlcv` ont tourné pour la dernière fois le **8 avril 2026** (révocation clé SA `gcp-sa-key`). Remplacés par `yfinance-ohlcv-pipeline`. **Déploiements supprimés du Prefect UI.**

---

## 3. `bronze.yahoo_rss` — 0 nouvelles lignes depuis 2026-03-31

Problème identifié mais non résolu : le flow `bronze-yahoo-rss` tourne (toutes les 4h via `bronze-silver-rss`) mais n'insère plus rien dans `bronze.yahoo_rss` depuis le 31 mars.

Cause probable : problème de fuzzy matching (aucun article RSS Yahoo ne matche les noms du référentiel).  
**Impact :** `silver.rss_articles` est alimenté uniquement par `bronze.google_news_rss` depuis cette date.

---

## 4. FastAPI hors Docker ✅ Résolu

FastAPI tourne en **systemd** (`pea-pme-api`), pas dans Docker. Documenté et corrigé dans `docs/infra.md` (PR #84 mergée).

---

## 5. nginx.conf versionné ≠ configuration réelle sur la VM

Le fichier `infra/nginx.conf` versionné dans le repo montre :
```nginx
location /grafana/ {
    proxy_pass http://grafana:3000/;
    ...
}
```

Mais la configuration appliquée sur la VM pour résoudre la boucle de redirection Grafana 12.x utilise un `rewrite` :
```nginx
location /grafana/ {
    rewrite ^/grafana/(.*) /$1 break;
    proxy_pass http://grafana:3000;
    ...
}
```

Et `GF_SERVER_SERVE_FROM_SUB_PATH=true` a été retiré du docker-compose sur la VM.  
**→ Le fichier versionné ne reflète pas l'état prod.**

---

## 6. docker-compose.prefect.yml : `GF_SERVER_SERVE_FROM_SUB_PATH=true` présent

Le fichier versionné contient encore :
```yaml
grafana:
  environment:
    - GF_SERVER_SERVE_FROM_SUB_PATH=true
```
Cette option provoque une boucle de redirection avec Grafana 12.x + nginx rewrite.  
Elle a été retirée manuellement sur la VM. **Le fichier versionné est incorrect.**

---

## 7. pyproject.toml — Double déclaration dépendances

Le fichier contient à la fois `[project]` (PEP 621, Poetry 2.x) et `[tool.poetry.dependencies]` (legacy Poetry 1.x).  
La section `[tool.poetry.dependencies]` ne contient que `python = "^3.11"` et `pdfplumber = "^0.11.9"`, ce qui peut créer une confusion sur la version de pdfplumber requise.

**Source de vérité : `[project]`**, qui liste `pdfplumber` sans contrainte de version.

---

## 8. Grafana nginx redirect ✅ Résolu

Résolu sur la VM : `rewrite ^/grafana/(.*) /$1 break` + suppression de `GF_SERVER_SERVE_FROM_SUB_PATH=true`. Le fichier versionné `infra/nginx.conf` reste légèrement en décalage mais sera aligné lors du merge de `feat/monitoring-grafana`.

---

## 9. Node Exporter non versionné

Le service `node-exporter` ajouté sur la VM pour les métriques CPU/RAM n'est pas dans `docker-compose.prefect.yml`.  
De même, le job `node` dans `prometheus.yml` n'est pas dans le fichier versionné.

**Ces configurations existent uniquement sur la VM.**

---

## 10. `stocks_scorer.py` vs `stocks_score.sql`

`src/gold/stocks_scorer.py` est une **implémentation miroir en Python** de `dbt/models/gold/stocks_score.sql`.  
**Elle n'est pas utilisée en production** — uniquement comme support de tests unitaires (`tests/gold/test_stocks_score.py`).

La source de vérité pour le scoring technique en production est le **modèle dbt SQL**.  
Ne pas présenter le script Python comme étant la logique de production.

---

## 11. nao — Non finalisé

Le service `nao` (Talk To My Data) tourne sur le port 5005 avec Gemini 2.5 Flash.  
Le semantic layer dbt (`semantic_models`, `metrics`) n'est **pas encore complété**.  
→ Les requêtes en langage naturel peuvent être imprécises ou échouer.

---

## 12. CI ne teste pas les modèles dbt

`ci.yml` exécute `pytest` mais pas `dbt test`.  
Les modèles SQL gold/silver ne sont pas couverts par la CI.  
→ Les régressions dans les transformations BQ ne sont détectées qu'à l'exécution des flows.

---

---

## 13. `investment_score` = PERCENT_RANK, pas une transformation de avg_sentiment

La formule dans `score_news.sql` :
```sql
GREATEST(1, CAST(CEIL(PERCENT_RANK() OVER (ORDER BY avg_sentiment_45d) * 10) AS INT64))
```
→ C'est un **rang relatif** parmi toutes les sociétés du jour, pas une transformation directe du sentiment moyen. Une société avec `avg_sentiment_45d = 6.0` peut obtenir `investment_score = 3` si la plupart des autres sociétés ont un sentiment encore meilleur.

**Implication pour la présentation :** ne pas dire "un sentiment moyen de X donne un score de Y" — le score dépend de l'ensemble de l'univers analysé ce jour-là.

---

## 14. `score_fondamental` = Z-scores pondérés (25/30/25/20), pas simple moyenne

La note initiale sous-estimait la complexité du modèle `financials_score.sql` :
- Poids inégaux : marge_op est pondérée à 30%, les autres à 20–25%
- Le levier dette est **inversé** dans le Z-score (moins de dette = meilleur score)
- Winsorisation avant calcul (élimine les outliers)
- Normalisation finale par PERCENT_RANK (rang parmi toutes les sociétés)
- Score = 0 si moins de 2 métriques disponibles

---

## 15. Monitoring (`feat/monitoring-grafana`) non encore dans main

Les fichiers suivants n'existent pas dans `main` :
- `infra/prometheus/prometheus.yml`
- `infra/grafana/provisioning/datasources/prometheus.yml`
- `/health` et `/metrics` dans `src/api/main.py`

Ils seront ajoutés lors du merge de `feat/monitoring-grafana`. Les notes `09_monitoring.md` documentent l'état de cette branche.

---

## Récapitulatif rapide pour la démo

| Point | État |
|---|---|
| Composite score = 25% × 4 | ✅ Correct dans le code |
| Orphan flows Prefect | ✅ Supprimés |
| bronze.yahoo_rss vide depuis 31/03 | ⚠️ Bug connu, non bloquant |
| FastAPI = systemd (pas Docker) | ✅ Documenté dans docs/infra.md |
| Grafana nginx redirect | ✅ Résolu sur la VM |
| nginx.conf versionné ≠ VM | ⚠️ Sera aligné au merge monitoring PR |
| Node Exporter non versionné | ℹ️ Config sur VM uniquement |
| investment_score = PERCENT_RANK | ℹ️ Formule contre-intuitive, voir point 13 |
| score_fondamental = Z-scores 25/30/25/20 | ℹ️ Plus complexe que documenté initialement |
| Monitoring dans feat/monitoring-grafana | ⚠️ Pas encore dans main |
