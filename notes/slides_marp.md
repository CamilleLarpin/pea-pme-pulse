---
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Segoe UI', sans-serif;
    font-size: 28px;
  }
  h1 { color: #1a73e8; font-size: 40px; }
  h2 { color: #1a73e8; }
  table { font-size: 22px; }
  .columns { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
---

# Mon flow
## Données de prix boursiers

- Chaque soir **lundi–vendredi à 19h30**, après la clôture des marchés
- **462 sociétés** françaises éligibles PEA-PME
- Source : **yfinance** — bibliothèque Python, données Yahoo Finance gratuites
- Données collectées : Ouverture · Plus haut · Plus bas · **Clôture** · Volume (**OHLCV**)

<!-- 
Mon rôle dans l'équipe c'était les données de prix. Chaque soir en semaine à 19h30 — juste après la clôture des marchés — un pipeline automatique va chercher les cours boursiers de 462 sociétés françaises via yfinance, une bibliothèque Python qui expose gratuitement les données Yahoo Finance. On récupère ce qu'on appelle les données OHLCV : Ouverture, le cours le plus Haut, le plus Bas, la Clôture, et le Volume échangé.
-->

---

# De la donnée brute au score technique

| Étape | Ce qui se passe |
|---|---|
| 🥉 **Bronze** | Stockage brut dans BigQuery — 462 ISINs, APPEND quotidien |
| 🥈 **Silver dbt** | Nettoyage : dédoublonnage + validation OHLCV |
| 🥈 **Silver Python** | 7 indicateurs techniques (RSI, MACD, Bollinger, SMA 50/200, EMA) |
| 🥇 **Gold dbt** | 5 signaux haussier/neutre/baissier → score **[0 – 10]** |

<!-- 
Le pipeline suit l'architecture Medallion du projet. Bronze : on stocke brut. Premier Silver : un modèle dbt nettoie et dédoublonne. Deuxième Silver : on calcule 7 indicateurs techniques avec la bibliothèque Python `ta` — le RSI mesure si une action est surachetée ou survendue, le MACD mesure le momentum, les moyennes mobiles 50 et 200 jours, les Bollinger Bands, et l'EMA 20 jours. Gold : un modèle dbt traduit ces indicateurs en 5 signaux valant 0, 1 ou 2 points — score technique de 0 à 10.

La séquence est critique : le nettoyage dbt doit tourner avant le Python, et le scoring dbt tourne après. C'est Prefect qui orchestre tout ça.
-->

---

# Orchestration & infrastructure

- **1 VM GCP** e2-small — sobre, suffisante pour nos volumes
- **Prefect 3** auto-hébergé — chef d'orchestre des 10 pipelines
- **Docker Compose** — 5 services : Prefect server · worker · nginx · Grafana · Prometheus
- **FastAPI** tourne hors Docker (service systemd)
- **Zéro clé secrète** stockée — authentification via métadonnées GCP (ADC)

<!-- 
Tout tourne sur une seule machine virtuelle chez Google Cloud — une e2-small, configuration modeste mais suffisante pour nos volumes.

Prefect c'est notre chef d'orchestre : il planifie les pipelines, les lance au bon moment, les rejoue en cas d'échec, et garde un historique. On l'héberge nous-mêmes plutôt que d'utiliser leur offre cloud payante.

Les services infrastructure tournent dans des containers Docker. L'API FastAPI, elle, tourne directement sur la machine via systemd : choix délibéré pour simplifier l'authentification Google Cloud.

Zéro fichier de clé secrète sur la machine — l'authentification GCP passe par les métadonnées de la VM, c'est le mécanisme ADC, Application Default Credentials.
-->

---

# Comment Prefect exécute les pipelines

- À chaque déclenchement : **clone du repo GitHub** (branche `main`) + pip install
- Pas d'image Docker par pipeline — simple et maintenable
- **nginx** aiguille les requêtes : Prefect UI · API FastAPI · Grafana

**10 pipelines déployés, 4 fréquences :**

| Schedule | Pipeline |
|---|---|
| Lun–Ven **19h30** | **yfinance OHLCV** (mon pipeline) |
| Toutes les **4h** | RSS actualités |
| **1er du mois** | Boursorama (référentiel) |
| **2x/an** | AMF fondamentaux (avril + octobre) |

<!-- 
À chaque exécution, le worker Prefect télécharge la dernière version du code depuis GitHub et installe les dépendances. On n'a pas d'image Docker par pipeline — ça simplifie beaucoup la maintenance.

10 pipelines avec des fréquences très différentes selon la nature des données : les cours boursiers tous les soirs, les actualités toutes les 4 heures, les fondamentaux seulement 2 fois par an quand les rapports AMF sortent.

nginx joue le rôle d'aiguilleur : selon l'URL demandée, il redirige vers Prefect, l'API, ou Grafana.
-->

---

# Prefect UI — Démonstration live

`http://35.241.252.5/prefect`

- **Deployments** → 10 pipelines, schedules visibles
- **Flow Runs** → historique, durée, état de chaque étape
- **Work Pools** → worker connecté en temps réel

<!-- 
CHEMIN DE DÉMO :
1. Dashboard → derniers runs, succès en vert
2. Deployments → liste des 10, montrer le schedule yfinance "30 19 * * 1-5"
3. Clic sur yfinance-ohlcv-pipeline → bouton Run manuel
4. Flow Runs → historique, durées
5. Clic sur un run récent → détail des 6 tasks dans l'ordre (bronze → dbt clean → Python indicators → dbt score → dbt company_scores)
6. Work Pools → worker connecté

"Si un pipeline casse à 19h45, on voit exactement à quelle étape, avec les logs complets — sans avoir à se connecter en SSH sur la machine."
-->

---

# Dashboard Streamlit

`http://35.241.252.5`

- Interface de scoring des **462 sociétés** PEA-PME
- Données via **API FastAPI** → cache 1h
- Code couleur : 🟢 ≥ 6 · 🟡 4–6 · 🔴 < 4

**4 onglets :**
Vue Composite · Actualités · Insiders & Fondamentaux · Analyse Technique

<!-- 
Tout ce qu'on vient de voir — les pipelines, les scores — c'est ici que ça devient visible. Le dashboard consomme notre API FastAPI qui interroge BigQuery. Les données sont mises en cache 1 heure pour ne pas surcharger BigQuery à chaque clic. Le bouton Actualiser vide ce cache.
-->

---

# Démonstration — Tour des 4 onglets

<!-- 
CHEMIN DE DÉMO :
1. Sidebar → légende couleurs, bouton Actualiser
2. Tab Composite → tableau Top 25, histogramme distribution, radar chart Top 5
   "Le score composite c'est une synthèse égale des 4 dimensions — 25% chacune."
3. Tab Actualités → Top 20 score news, scatter volume/sentiment
   "Le score news c'est un rang relatif parmi toutes les sociétés du jour — pas une transformation directe du sentiment moyen."
4. Tab Insiders → signaux AMF, montants d'achats par les dirigeants
   "Quand un PDG rachète ses propres actions, c'est en général un signal positif."
5. Tab Technique → heatmap signaux RSI/MACD, historique score_7d_avg
   "C'est la vue issue directement de mon pipeline."
-->

---

# Monitoring — Prometheus & Grafana

- FastAPI expose `/metrics` : latence · erreurs · volume de requêtes
- **Prometheus** collecte toutes les **30 secondes**
- **Grafana** visualise :
  - API status (UP/DOWN)
  - CPU & RAM de la VM
  - Fraîcheur des données par dimension
  - Runs Prefect récents
- Branche `feat/monitoring-grafana` — **merge imminent**

<!-- 
En bonus, j'ai ajouté une couche de monitoring. L'API FastAPI expose ses métriques de performance dans un format standard que Prometheus collecte toutes les 30 secondes. Grafana les visualise : est-ce que l'API répond ? En combien de temps ? La machine est-elle saturée ? Depuis quand les données n'ont pas été mises à jour ?

C'est dans une PR qui sera mergée juste après la présentation.
-->
