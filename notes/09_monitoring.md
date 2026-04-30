# 09 — Monitoring (Prometheus + Grafana)

> ⚠️ **Statut :** Cette configuration est dans la branche `feat/monitoring-grafana` (non encore mergée dans `main`).
> Les fichiers `infra/prometheus/prometheus.yml` et `infra/grafana/provisioning/datasources/prometheus.yml`
> n'existent pas dans `main`. Les endpoints `/health` et `/metrics` de l'API sont aussi dans cette branche.

## Architecture monitoring

```
FastAPI (systemd :8000)
  └─ /metrics  ←── Prometheus (scrape 30s)
                        └─ Grafana (datasource Prometheus)

Node Exporter (:9100)  ←── Prometheus (scrape 30s)
                                └─ Grafana (CPU, RAM, disque VM)

BigQuery gold.*        ←── Grafana (datasource BigQuery via GCE ADC)

Prefect API (:4200)    ←── Grafana (datasource Infinity HTTP)
```

---

## Prometheus — Ce qu'il collecte et comment

Prometheus est une **base de données de séries temporelles** spécialisée dans les métriques. Son fonctionnement est inverse à ce qu'on pourrait attendre : ce ne sont pas les services qui envoient leurs métriques à Prometheus — c'est Prometheus qui va **les chercher (scrape)** à intervalles réguliers sur des endpoints HTTP exposés par chaque service.

**Deux sources :**

1. **FastAPI `/metrics`** — exposé par `prometheus-fastapi-instrumentator` : compteurs de requêtes, histogrammes de latence, disponibilité.
2. **Node Exporter `:9100`** — lit directement les fichiers `/proc` et `/sys` du noyau Linux : CPU, RAM, disque, réseau.

Chaque valeur est stockée avec un timestamp et des labels (ex: `handler="/gold/stocks-score/latest"`). Grafana interroge Prometheus en PromQL pour construire des graphiques.

---

## Prometheus

**Fichier config :** `infra/prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: pea-pme-api
    static_configs:
      - targets: ['host.docker.internal:8000']
    metrics_path: /metrics
```

- `host.docker.internal` → résolu vers la VM hôte (via `extra_hosts: host.docker.internal:host-gateway` dans docker-compose)
- Le container prometheus peut donc atteindre FastAPI qui tourne hors Docker sur le port 8000

**Node Exporter** (ajouté sur la VM) :
```yaml
  - job_name: node
    static_configs:
      - targets: ['node-exporter:9100']
```
Container node-exporter monté avec `/proc`, `/sys`, `/rootfs` pour métriques OS.

**Métriques FastAPI exposées par prometheus-fastapi-instrumentator :**
- `http_requests_total{handler, method, status}` — Compteur de requêtes
- `http_request_duration_seconds{handler}` — Histogramme latences
- `http_requests_in_progress` — Requêtes en cours
- `up{job="pea-pme-api"}` — Disponibilité (1=up, 0=down)

---

## Grafana

- **Version :** grafana/grafana:latest (12.x sur la VM)
- **URL :** `http://35.241.252.5/grafana/`
- **Admin :** `admin` / voir `.env.monitoring`
- **Plugin :** `grafana-bigquery-datasource` (auto-installé au démarrage du container)

### Datasources configurées

| Nom | Type | URL / Auth | Statut |
|---|---|---|---|
| Prometheus | prometheus | `http://prometheus:9090` | Provisionnée via YAML |
| BigQuery | grafana-bigquery-datasource | GCE Default SA (ADC) | Configurée manuellement |
| Infinity (Prefect) | yesoreyeram-infinity-datasource | `http://prefect-server:4200/api` | Configurée manuellement |

**BigQuery datasource :**
- Authentication : "GCE Default Service Account" → utilise l'ADC de la VM (aucune clé JSON)
- Project : `bootcamp-project-pea-pme`
- Dataset par défaut : `gold`

**Infinity datasource (Prefect) :**
- URL de base : `http://prefect-server:4200/api`
- Endpoints utiles :
  - `GET /flow_runs/filter` → Runs récents (POST avec body JSON)
  - `GET /flows/filter` → Noms des flows
  - `GET /health` → État du serveur Prefect

---

## Panels du dashboard "PEA-PME Pulse"

### Row 1 — Disponibilité API

| Panel | Type | PromQL / Source |
|---|---|---|
| API Status | stat | `up{job="pea-pme-api"}` → "UP" (vert) / "DOWN" (rouge) |
| Taux erreur API (5min) | stat | `rate(http_requests_total{job="pea-pme-api",status=~"5.."}[5m]) / rate(http_requests_total{job="pea-pme-api"}[5m]) * 100` |

### Row 2 — VM (Node Exporter)

| Panel | Type | PromQL |
|---|---|---|
| CPU VM | gauge | `100 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100` |
| RAM VM | gauge | `(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100` |

**Ce qu'affiche la jauge :**

La jauge CPU/RAM affiche un **pourcentage d'utilisation instantané** (moyenne sur les 5 dernières minutes) avec des seuils de couleur :
- 🟢 Vert → utilisation normale (< ~60%)
- 🟡 Jaune → charge élevée
- 🔴 Rouge → saturation

**Formule CPU détaillée :** Le noyau Linux comptabilise le temps CPU par mode (`idle`, `user`, `system`, `iowait`…). On mesure le taux d'évolution du mode `idle` sur 5 min et on le soustrait de 100% — ce qui donne le % de CPU effectivement utilisé. Résultat : si la jauge affiche 25%, la VM est occupée à 25% de sa capacité CPU.

**Formule RAM :** `MemAvailable` (RAM libre + récupérable) divisé par `MemTotal`. On prend le complément pour obtenir la RAM utilisée en pourcentage.

### Row 3 — Latence API

| Panel | Type | PromQL |
|---|---|---|
| Latence p95 par endpoint | time series | `histogram_quantile(0.95, sum by (le, handler) (rate(http_request_duration_seconds_bucket{job="pea-pme-api"}[5m])))` |

### Row 4 — Fraîcheur pipeline

| Panel | Type | Source |
|---|---|---|
| Fraîcheur pipeline | table | BigQuery — requête SQL sur `gold.*` |

**Requête SQL fraîcheur :**
```sql
SELECT 'Composite' AS Dimension, MAX(score_date) AS Derniere_MAJ,
  DATE_DIFF(CURRENT_DATE(), MAX(score_date), DAY) AS Jours_retard
FROM `bootcamp-project-pea-pme.gold.company_scores`
UNION ALL
SELECT 'Technique', MAX(date), DATE_DIFF(CURRENT_DATE(), MAX(date), DAY)
FROM `bootcamp-project-pea-pme.gold.stocks_score`
UNION ALL
SELECT 'Actualites', MAX(score_date), DATE_DIFF(CURRENT_DATE(), MAX(score_date), DAY)
FROM `bootcamp-project-pea-pme.gold.score_news`
UNION ALL
SELECT 'Insiders', MAX(signal_date), DATE_DIFF(CURRENT_DATE(), MAX(signal_date), DAY)
FROM `bootcamp-project-pea-pme.gold.score_insider`
UNION ALL
SELECT 'Fondamentaux', MAX(date_cloture_exercice), DATE_DIFF(CURRENT_DATE(), MAX(date_cloture_exercice), DAY)
FROM `bootcamp-project-pea-pme.gold.financials_score`
```
Note : pas d'accents dans les alias (BigQuery rejette l'encodage UTF-8 en certains contextes).

### Row 5 — Flows Prefect (Infinity)

Panel table : Runs récents avec flow_id, state_name, start_time
- Source : Infinity datasource, `POST /flow_runs/filter`
- Limitation : l'API retourne `flow_id` (UUID) et non le nom — un JOIN avec `/flows/filter` est nécessaire pour afficher le nom du flow

---

## PromQL de référence

```promql
# Disponibilité (1 = UP)
up{job="pea-pme-api"}

# CPU VM (%)
100 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100

# RAM utilisée (%)
(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100

# Taux d'erreurs 5xx (%)
rate(http_requests_total{job="pea-pme-api",status=~"5.."}[5m])
/ rate(http_requests_total{job="pea-pme-api"}[5m]) * 100

# Latence p95 par handler
histogram_quantile(0.95,
  sum by (le, handler) (
    rate(http_request_duration_seconds_bucket{job="pea-pme-api"}[5m])
  )
)

# Nombre de requêtes par handler (5min)
sum by (handler) (rate(http_requests_total{job="pea-pme-api"}[5m]))
```

---

## Provisioning automatique

Les fichiers dans `infra/grafana/provisioning/` sont chargés au démarrage de Grafana :
- `datasources/prometheus.yml` → Datasource Prometheus provisionnée (non modifiable via UI)
- `dashboards/provider.yml` → Scanne `/var/lib/grafana/dashboards/*.json` → folder `PEA-PME`

Les datasources BigQuery et Infinity sont configurées manuellement via l'UI (non versionnées à ce jour).

---

## Ajout Node Exporter (post-déploiement initial)

Le node-exporter n'est pas dans `docker-compose.prefect.yml` versionné.
Sur la VM, il a été ajouté manuellement :

```yaml
  node-exporter:
    image: prom/node-exporter:latest
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--path.rootfs=/rootfs'
    restart: unless-stopped
```

Et `prometheus.yml` mis à jour avec le job `node`.
