# 08 — Infrastructure

## VM GCP

| Attribut | Valeur |
|---|---|
| Machine type | e2-small |
| IP externe | 35.241.252.5 |
| Zone | europe-west1-b |
| OS | Linux (Debian) |
| Service Account | pea-pme-ingestor |
| Repo cloné (Mathias) | `/home/mathias/pea-pme-pulse` |
| Repo cloné (Camille) | `/home/camillelarpin/prefect` |

**Application Default Credentials (ADC) — pourquoi zéro clé secrète sur la machine :**

Chaque VM GCP a un Service Account attaché (`pea-pme-ingestor`). Le **metadata server** GCP tourne en permanence à l'adresse `http://169.254.169.254` et délivre des tokens d'accès temporaires pour ce SA — sans aucun fichier. Le SDK Google Cloud (BigQuery, GCS…) interroge automatiquement ce metadata server quand il ne trouve pas de clé JSON : c'est ADC.

Conséquences :
- Pas de clé JSON à stocker, à sécuriser, à renouveler → zéro risque de fuite
- Les tokens sont éphémères (expiration automatique) → si la VM est compromise, pas de credentials à voler
- En local : `gcloud auth application-default login` joue le même rôle (token via navigateur)
- La clé SA `gcp-sa-key` stockée dans Prefect Secrets est révoquée depuis 2026-04-08 — legacy uniquement

---

## Docker Compose (production)

Deux fichiers Compose coexistent :

### `infra/docker-compose.prefect.yml` (déployé sur la VM)
5 services dans un seul réseau Docker :

| Service | Image | Rôle | Port exposé |
|---|---|---|---|
| `prefect-server` | `prefecthq/prefect:3-latest` | Prefect server (API + UI) | Interne 4200 |
| `prefect-worker` | `prefecthq/prefect:3-latest` | Worker pool `bronze-pool`, type `process` | — |
| `nginx` | `nginx:alpine` | Reverse proxy HTTP port 80 | **80:80** |
| `grafana` | `grafana/grafana:latest` | Dashboards monitoring | Interne 3000 |
| `prometheus` | `prom/prometheus:latest` | Collecte métriques | Interne 9090 |

Volumes nommés : `prefect-db`, `grafana-data`, `prometheus-data`

**Démarrage :**
```bash
sudo docker compose -p prefect -f docker-compose.prefect.yml up -d
```
→ Containers nommés `prefect-<service>-1`

### `infra/docker-compose.monitoring.yml` (dev local uniquement)
Grafana (port 3000) + Prometheus (port 9090) sans nginx.

---

## FastAPI — Service systemd

**Pourquoi systemd plutôt que Docker ?**

`systemd` est le gestionnaire de services Linux : il démarre les processus au boot, les redémarre automatiquement en cas de crash, et centralise les logs (`journalctl`). C'est l'équivalent d'un "supervisor" natif à l'OS.

FastAPI tourne hors Docker pour une raison principale : **l'accès au metadata server GCP (ADC)**. Un processus qui tourne directement sur la VM atteint `http://169.254.169.254` sans configuration supplémentaire. Dans un container Docker, la résolution réseau est isolée — il faudrait ajouter `--network=host` ou une configuration `extra_hosts`, ce qui complexifie le setup. En dehors de Docker, ça marche out of the box.

FastAPI tourne **hors Docker** en tant que service systemd `pea-pme-api` :

- **WorkingDirectory :** `/home/mathias/pea-pme-pulse`
- **Venv :** `/home/mathias/api-venv/`
- **Port :** 8000 (interface loopback + IP interne `10.132.0.2`)
- **Commande :** `uvicorn src.api.main:app --host 0.0.0.0 --port 8000`

```bash
sudo systemctl status pea-pme-api
sudo systemctl restart pea-pme-api
sudo journalctl -u pea-pme-api -f
```

---

## nginx — Table de routage complète

**Ce qu'est nginx et pourquoi on en a besoin :**

nginx est un **reverse proxy** : un aiguilleur HTTP qui se place devant tous les services. La VM a une seule adresse IP publique (`35.241.252.5`) mais plusieurs services qui écoutent chacun sur un port différent (Prefect sur 4200, FastAPI sur 8000, Grafana sur 3000). Sans nginx, les utilisateurs devraient connaître et taper le bon port dans l'URL — et les ports seraient tous exposés publiquement.

nginx écoute sur le port 80, lit le **chemin de l'URL**, et redirige vers le bon service interne. Il joue aussi le rôle de pare-feu : seul le port 80 est ouvert au public, les autres restent internes au réseau Docker.

Fichier : `infra/nginx.conf` (monté dans le container nginx)

| Location | Règle | Destination |
|---|---|---|
| `/api` | proxy_pass | `http://prefect-server:4200` (WebSocket upgrade inclus) |
| `/dbt-docs/` | alias statique | `/var/www/dbt-docs/` |
| `~ ^/(gold\|overview\|health\|metrics\|docs\|openapi\.json)` | regex proxy | `http://10.132.0.2:8000` (FastAPI systemd) |
| `/grafana/` | proxy_pass | `http://grafana:3000/` |
| `/` | proxy_pass + auth_basic | `http://prefect-server:4200` (Prefect UI, htpasswd) |

**Points importants :**
- `/api` sans auth → accès direct à l'API Prefect (pour les workers et webhooks)
- `/` avec auth basic `"Prefect - PEA-PME"` → `.htpasswd` monté en volume
- FastAPI atteint via IP interne `10.132.0.2:8000` (pas `localhost` car nginx est dans Docker)
- Grafana : proxy simple `proxy_pass http://grafana:3000/` (trailing slash = nginx strip prefix)
- Note : La configuration Grafana sur la VM réelle peut différer du fichier versionné (rewrite vs trailing slash — voir `11_points_attention.md`)

---

## Volumes et données persistantes

| Volume Docker | Contenu |
|---|---|
| `prefect-db` | SQLite Prefect (flow runs, deployments, blocks) |
| `grafana-data` | Dashboards, users, datasources Grafana |
| `prometheus-data` | Séries temporelles Prometheus (TSDB) |

**GCS Bucket : `project-pea-pme`**

| Chemin GCS | Contenu |
|---|---|
| `rss_yahoo/` | Dumps JSON horodatés Yahoo RSS |
| `rss_google_news/` | Dumps JSON horodatés Google News RSS |
| `amf/` | PDFs, XMLs, JSONL AMF (auditabilité) |
| `boursorama_peapme_final.csv` | Référentiel 571 sociétés (table externe BQ) |

---

## Provisioning Grafana

Grafana est configuré via des fichiers montés en lecture seule :

```
infra/grafana/provisioning/
  datasources/prometheus.yml   → Datasource Prometheus auto-configurée
  dashboards/provider.yml      → Provider : charge JSON depuis /var/lib/grafana/dashboards
infra/grafana/dashboards/      → Fichiers JSON de dashboards (auto-chargés)
```

**Datasource Prometheus (auto-provisioned) :**
```yaml
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
```

**Dashboard provider :**
```yaml
providers:
  - name: PEA-PME Pulse
    folder: PEA-PME
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

---

## Variables d'environnement Grafana

Fichier : `infra/.env.monitoring` (non commité, à partir de `.env.monitoring.example`)

```
GRAFANA_ADMIN_PASSWORD=<valeur secrète>
```

Sur la VM (docker-compose.prefect.yml) :
- `GF_INSTALL_PLUGINS=grafana-bigquery-datasource` → Plugin BigQuery auto-installé
- `GF_SERVER_ROOT_URL=http://35.241.252.5/grafana/`
- `GF_SERVER_SERVE_FROM_SUB_PATH=true`

---

## URLs publiques

| Service | URL |
|---|---|
| Prefect UI | `http://35.241.252.5/` (auth basic) |
| FastAPI docs | `http://35.241.252.5/docs` |
| FastAPI /health | `http://35.241.252.5/health` |
| Grafana | `http://35.241.252.5/grafana/` |
| dbt docs | `http://35.241.252.5/dbt-docs/` |

---

## Commandes utiles sur la VM

```bash
# Statut services Docker
sudo docker compose -p prefect ps

# Logs nginx
sudo docker logs prefect-nginx-1 -f

# Logs Grafana
sudo docker logs prefect-grafana-1 -f

# Redémarrer un container
sudo docker restart prefect-grafana-1

# Statut FastAPI
sudo systemctl status pea-pme-api

# Logs FastAPI
sudo journalctl -u pea-pme-api -f --since "1 hour ago"
```
