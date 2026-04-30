# Notes de présentation — PEA-PME Pulse

> Créées le 2026-04-13. Source : lecture complète du code source + docs officielles.
> Ces notes corrigent les approximations des fichiers `docs/` quand nécessaire.

## Résumé du projet

PEA-PME Pulse est un système de scoring quotidien automatisé des PME éligibles au PEA-PME.
Il ingère des données 100% gratuites (yfinance, AMF API, flux RSS, Boursorama), les transforme via une architecture Medallion Bronze→Silver→Gold sur BigQuery, et produit un score composite [0–10] par société. Une API FastAPI et un dashboard Streamlit exposent les résultats.

- **Univers :** 571 sociétés françaises cotées
- **Production :** VM GCP e2-small (35.241.252.5), europe-west1-b
- **Pipeline :** 10 flows Prefect, orchestration automatique
- **Score final :** composite = 0.25 × technique + 0.25 × news + 0.25 × insider + 0.25 × financials

---

## Index

| Fichier | Contenu |
|---|---|
| [01_vue_ensemble.md](01_vue_ensemble.md) | Projet, objectifs, périmètre, stack, équipe |
| [02_architecture.md](02_architecture.md) | Architecture Medallion, conventions, tech stack complet |
| [03_pipeline_donnees.md](03_pipeline_donnees.md) | Couches Bronze/Silver/Gold — tables, sources, transformations |
| [04_flows_prefect.md](04_flows_prefect.md) | Inventaire des 10 flows Prefect avec schedules et étapes |
| [05_scoring.md](05_scoring.md) | Formules de scoring détaillées (technique, news, insider, financials, composite) |
| [06_api.md](06_api.md) | Tous les endpoints FastAPI (routes, schémas, requêtes BQ) |
| [07_dashboard.md](07_dashboard.md) | Streamlit — structure, onglets, graphiques, cache |
| [08_infra.md](08_infra.md) | VM GCP, Docker Compose, nginx, secrets, ADC |
| [09_monitoring.md](09_monitoring.md) | Prometheus + Grafana — métriques, datasources, panels |
| [10_ci_qualite.md](10_ci_qualite.md) | CI/CD, Ruff, pytest, Makefile |
| [11_points_attention.md](11_points_attention.md) | Incohérences docs/code, bugs connus, points à clarifier |
