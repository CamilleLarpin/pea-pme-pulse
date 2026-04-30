# 01 — Vue d'ensemble du projet

## Identité

| | |
|---|---|
| **Nom** | PEA-PME Pulse |
| **Version** | 0.1.0 |
| **Repo** | https://github.com/CamilleLarpin/pea-pme-pulse |
| **Statut** | En production |
| **Environnement prod** | VM GCP `35.241.252.5`, europe-west1-b |
| **GCP Project** | `bootcamp-project-pea-pme` |

---

## Objectif

Produire chaque jour ouvré un **classement des PME françaises éligibles au PEA-PME** par score composite d'attractivité [0–10], à partir de 4 dimensions :

- 📈 **Technique** — Indicateurs boursiers (RSI, MACD, Golden Cross, Bollinger, EMA)
- 📰 **Actualités** — Sentiment presse analysé par LLM (fenêtre 45 jours)
- 🏦 **Insiders** — Achats/ventes de dirigeants déclarés à l'AMF
- 📊 **Fondamentaux** — Croissance CA, marge opérationnelle, levier dette, FCF yield

---

## Périmètre

- **Univers analysé :** 571 sociétés (référentiel `referentiel/boursorama_peapme_final.csv`)
- **Données entièrement gratuites** — aucune API payante
- **Score composite quotidien** recalculé chaque soir en semaine (sauf fondamentaux, annuels)

---

## Sources de données

| Source | Type | Historique | Fréquence |
|---|---|---|---|
| yfinance + ta | Library Python | 20 ans | Lun-Ven 19h30 |
| AMF flux-amf-new-prod | API REST JSON v2 | Complet | Toutes les 4h |
| Yahoo Finance FR RSS | Flux XML RSS | ~50 articles | Toutes les 4h |
| Google News RSS | Flux XML RSS (2 flux) | ~30 articles | Toutes les 4h |
| ABCBourse RSS | Flux XML RSS | ~30 articles | Lun-Ven 08h |
| Boursorama | Scraping Web | Snapshot mensuel | 1er du mois |

---

## Stack technologique

| Catégorie | Outil | Notes |
|---|---|---|
| Langage | Python 3.10–3.11 | CI sur 3.11 |
| Orchestration | Prefect 3 (≥3.1.0) | Self-hosted sur VM GCP |
| Transformation SQL | dbt-bigquery | Modèles Silver + Gold |
| Entrepôt de données | BigQuery (EU) | projet `bootcamp-project-pea-pme` |
| Data Lake | Google Cloud Storage | Bucket `project-pea-pme` |
| LLM | Groq API | Llama 3.1 8B (RSS), Llama 3.3 70B (PDFs AMF) |
| API | FastAPI + uvicorn | Port 8000, systemd sur VM |
| Dashboard | Streamlit (≥1.56.0) | + Plotly (≥6.6.0) |
| Monitoring | Prometheus + Grafana 12.4.2 | Self-hosted sur VM |
| Conteneurisation | Docker + docker-compose | 5 services sur VM |
| Reverse proxy | nginx | Port 80, basic auth Prefect UI |
| Talk To My Data | nao (getnao.io) | Gemini 2.5 Flash, port 5005 |
| Qualité code | Ruff | line-length=100, py311 |
| Tests | pytest | Unit tests Python, pas de dbt test en CI |
| CI/CD | GitHub Actions | PR + push main/develop |
| Gestion dépendances | Poetry 2.3.2 | poetry.lock commité |

---

## Livrables

1. **Pipeline de données** — Bronze→Silver→Gold automatisé, 10 flows Prefect
2. **API REST** — 11 endpoints FastAPI (read-only, BigQuery)
3. **Dashboard** — 4 onglets Streamlit (Composite, Actualités, Insiders/Fondamentaux, Technique)
4. **Monitoring** — Grafana (API health, VM CPU/RAM, fraîcheur pipeline, latence)
5. **Documentation** — dbt docs statiques, README, docs/

---

## Accès en production

| Service | URL | Auth |
|---|---|---|
| Prefect UI | http://35.241.252.5/ | Basic auth |
| Prefect API | http://35.241.252.5/api | — |
| FastAPI docs | http://35.241.252.5/docs | — |
| FastAPI health | http://35.241.252.5/health | — |
| Grafana | http://35.241.252.5/grafana/ | Login/password |
| dbt docs | http://35.241.252.5/dbt-docs/ | — |
| nao | http://35.241.252.5:5005 | — |
