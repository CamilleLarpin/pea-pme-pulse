# Run manuel — pipelines Bronze RSS

> Commandes pour lancer les ingestions manuellement (dev/debug) ou via Prefect (production).
> Prérequis : être dans le repo local, sur `main`, GCP auth OK.

---

## Prérequis

```bash
# 1. Se placer dans le repo
cd ~/projects/pea-pme-pulse

# 2. Vérifier l'auth GCP
gcloud auth application-default print-access-token | head -c 20

# 3. Vérifier le bon Python (pyenv 3.12)
pyenv which python
# → doit afficher ~/.pyenv/versions/3.12.5/bin/python
```

---

## Yahoo Finance FR RSS

**Output :** GCS `gs://project-pea-pme/rss_yahoo/` · BQ `bronze.yahoo_rss`

```bash
pyenv exec python scripts/run_yahoo_rss.py
```

**Vérifier le résultat :**

```bash
# GCS — lister les dumps
gsutil ls gs://project-pea-pme/rss_yahoo/

# BQ — voir les dernières lignes chargées
bq query --nouse_legacy_sql \
  'SELECT title, matched_name, isin, published
   FROM `bootcamp-project-pea-pme.bronze.yahoo_rss`
   ORDER BY fetched_at DESC LIMIT 10'
```

---

## Google News RSS (Euronext Growth + PME Bourse FR)

**Output :** GCS `gs://project-pea-pme/rss_google_news/` · BQ `bronze.google_news_rss`

```bash
pyenv exec python scripts/run_google_news_rss.py
```

**Vérifier le résultat :**

```bash
# GCS — lister les dumps
gsutil ls gs://project-pea-pme/rss_google_news/

# BQ — voir les dernières lignes chargées
bq query --nouse_legacy_sql \
  'SELECT title, matched_name, isin, feed_name, published
   FROM `bootcamp-project-pea-pme.bronze.google_news_rss`
   ORDER BY fetched_at DESC LIMIT 10'
```

---

## Lancer les deux d'affilée (manuel)

```bash
pyenv exec python scripts/run_yahoo_rss.py
pyenv exec python scripts/run_google_news_rss.py
```

---

## Via Prefect (production)

Flows déployés sous le work pool `bronze-rss-pool` · schedule automatique 17:30 UTC lun–ven.

**Lancer manuellement un run Prefect :**
```bash
prefect deployment run 'bronze-rss-daily/bronze-rss-daily'
```

**Voir les runs récents :**
```bash
prefect flow-run ls
```

**Démarrer le worker (sur GCP e2-small) :**
```bash
prefect worker start --pool bronze-rss-pool
```
