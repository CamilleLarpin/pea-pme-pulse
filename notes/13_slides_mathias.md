# 13 — Slides Google Slides (Mathias)

Structure slide par slide avec notes orateur.  
Chaque bloc = 1 slide à créer dans Google Slides.

---

## Slide 1 — Mon flow : récupérer les prix boursiers

**Titre :** Mon flow — Données de prix boursiers

**Contenu :**
- Chaque soir en semaine à **19h30**, après la clôture des marchés
- **462 sociétés** françaises éligibles PEA-PME
- Source : **yfinance** (bibliothèque Python, données Yahoo Finance — gratuites)
- Données collectées : Ouverture, Plus haut, Plus bas, **Clôture**, Volume (OHLCV)

**Notes orateur :**
> "Mon rôle dans l'équipe c'était les données de prix. Chaque soir en semaine à 19h30 — juste après la clôture des marchés — un pipeline automatique va chercher les cours boursiers de 462 sociétés françaises via yfinance, une bibliothèque Python qui expose gratuitement les données Yahoo Finance. On récupère ce qu'on appelle les données OHLCV : Ouverture, le cours le plus Haut, le plus Bas, la Clôture, et le Volume échangé."

---

## Slide 2 — Pipeline Bronze → Silver → Gold

**Titre :** De la donnée brute au score technique

**Contenu (schéma vertical ou tableau 4 lignes) :**

| Étape | Ce qui se passe |
|---|---|
| 🥉 Bronze | Stockage brut dans BigQuery — 462 ISINs, APPEND quotidien |
| 🥈 Silver dbt | Nettoyage : dédoublonnage + validation OHLCV |
| 🥈 Silver Python | Calcul de 7 indicateurs techniques (RSI, MACD, Bollinger, SMA 50/200, EMA) |
| 🥇 Gold dbt | 5 signaux → score technique **[0 – 10]** |

**Notes orateur :**
> "Le pipeline suit l'architecture Medallion du projet. Bronze : on stocke brut. Premier Silver : un modèle dbt nettoie et dédoublonne — si une journée est déjà en base, on ne la réinsère pas. Deuxième Silver : on calcule 7 indicateurs techniques avec la bibliothèque Python `ta` — le RSI qui mesure si une action est surachetée ou survendue, le MACD qui mesure le momentum, les moyennes mobiles 50 et 200 jours, les Bollinger Bands, et l'EMA 20 jours. Gold : un modèle dbt traduit ces indicateurs en 5 signaux haussier / neutre / baissier, chacun valant 0, 1 ou 2 points — ce qui donne un score technique de 0 à 10."
>
> "La séquence est critique : le nettoyage dbt doit tourner avant le Python, et le scoring dbt tourne après. C'est Prefect qui s'assure que tout s'enchaîne dans le bon ordre."

---

## Slide 3 — Orchestration : une seule machine, tout un système

**Titre :** Orchestration & infrastructure — Vue d'ensemble

**Contenu :**
- **1 VM GCP** e2-small — sobre, suffisante pour nos volumes
- **Prefect 3** auto-hébergé — chef d'orchestre des 10 pipelines
- **Docker Compose** — 5 services : Prefect server, Prefect worker, nginx, Grafana, Prometheus
- **FastAPI** tourne en dehors de Docker (service systemd)
- **Zéro clé secrète** sur la machine — authentification via métadonnées GCP (ADC)

**Notes orateur :**
> "Tout tourne sur une seule machine virtuelle chez Google Cloud — une e2-small, configuration modeste mais suffisante pour nos volumes. Configuration sobre, coût maîtrisé."
>
> "Prefect c'est notre chef d'orchestre : il planifie les pipelines, les lance au bon moment, les rejoue en cas d'échec, et garde un historique de chaque exécution. On l'héberge nous-mêmes plutôt que d'utiliser leur offre cloud payante."
>
> "Les services infrastructure tournent dans des containers Docker — des boîtes isolées. L'API FastAPI, elle, tourne directement sur la machine via systemd : choix délibéré pour simplifier l'authentification Google Cloud."
>
> "Et zéro fichier de clé secrète stocké sur la machine — l'authentification GCP passe par les métadonnées de la VM. C'est le mécanisme ADC, Application Default Credentials."

---

## Slide 4 — Pattern d'exécution des pipelines

**Titre :** Comment Prefect exécute les pipelines

**Contenu :**
- À chaque déclenchement : **clone du repo GitHub** (branche `main`) + pip install
- Pas d'image Docker par pipeline — simple et maintenable
- **10 déploiements** avec schedules variés :
  - RSS actualités → toutes les 4h
  - **yfinance OHLCV** → lundi–vendredi 19h30
  - AMF (fondamentaux) → 2 fois par an
  - Boursorama (référentiel) → 1er du mois
- **nginx** aiguille les requêtes : Prefect UI, API FastAPI, Grafana

**Notes orateur :**
> "À chaque exécution, le worker Prefect télécharge la dernière version du code depuis GitHub et installe les dépendances. On n'a pas d'image Docker par pipeline — ça simplifie beaucoup la maintenance et les mises à jour."
>
> "On a 10 pipelines déployés avec des fréquences très différentes selon la nature des données : les cours boursiers tous les soirs en semaine, les actualités toutes les 4 heures, les fondamentaux financiers seulement 2 fois par an quand les rapports AMF sortent."
>
> "nginx joue le rôle d'aiguilleur : selon l'URL demandée, il redirige vers Prefect, l'API, ou Grafana."

---

## Slide 5 — Prefect UI : centre de contrôle (slide de démo)

**Titre :** Prefect UI — Démonstration live

**Contenu :**
- `http://35.241.252.5/prefect`
- Deployments → 10 pipelines, schedules visibles
- Flow Runs → historique, durée, état de chaque étape
- Work Pools → worker connecté en temps réel

**Notes orateur :**
> "Je vous montre en direct."
>
> **Chemin de démo :**
> 1. Dashboard → derniers runs, succès en vert
> 2. Deployments → liste des 10, montrer le schedule yfinance `30 19 * * 1-5`
> 3. Clic sur `yfinance-ohlcv-pipeline` → bouton Run manuel
> 4. Flow Runs → historique, durées
> 5. Clic sur un run récent → détail des 6 tasks dans l'ordre (bronze → dbt clean → Python indicators → dbt score → dbt company_scores)
> 6. Work Pools → worker connecté
>
> "Si un pipeline casse à 19h45, on voit exactement à quelle étape, avec les logs complets — sans avoir à se connecter en SSH sur la machine."

---

## Slide 6 — Dashboard Streamlit : la vitrine du projet

**Titre :** Dashboard Streamlit — Ce que voit l'utilisateur

**Contenu :**
- Interface de scoring des 462 sociétés PEA-PME
- **4 onglets** : Vue Composite · Actualités · Insiders & Fondamentaux · Analyse Technique
- Données via **API FastAPI** → mise en cache 1h
- Code couleur : 🟢 ≥ 6 | 🟡 4–6 | 🔴 < 4

**Notes orateur :**
> "Tout ce qu'on vient de voir — les pipelines, les scores — c'est ici que ça devient visible et utilisable. Le dashboard consomme notre API FastAPI qui interroge BigQuery."
>
> "Les données sont mises en cache 1 heure côté Streamlit pour ne pas surcharger BigQuery à chaque clic. Le bouton Actualiser vide ce cache."

---

## Slide 7 — Tour des 4 onglets (slide de démo)

**Titre :** Démonstration live — `http://35.241.252.5`

**Contenu :**
- **Vue Composite** → classement 462 sociétés, radar chart Top 5
- **Actualités** → score de sentiment 45 jours, scatter volume/sentiment
- **Insiders & Fondamentaux** → transactions dirigeants, métriques financières AMF
- **Analyse Technique** → heatmap signaux, historique scores

**Notes orateur :**
> **Chemin de démo :**
> 1. Sidebar → légende couleurs, bouton Actualiser
> 2. Tab Composite → tableau Top 25, histogramme distribution, radar chart Top 5
>    "Le score composite c'est une synthèse égale des 4 dimensions — 25% chacune."
> 3. Tab Actualités → Top 20 score news, scatter volume/sentiment
>    "Le score news c'est un rang relatif parmi toutes les sociétés du jour, pas une transformation directe du sentiment moyen."
> 4. Tab Insiders → signaux AMF, montants d'achats par les dirigeants
>    "Quand un PDG rachète ses propres actions, c'est en général un signal positif."
> 5. Tab Technique → heatmap signaux RSI/MACD/etc., historique score_7d_avg
>    "C'est la vue issue directement de mon pipeline."

---

## Slide 8 — Monitoring Prometheus + Grafana

**Titre :** Monitoring — Prometheus & Grafana

**Contenu :**
- FastAPI expose `/metrics` — latence, erreurs, volume de requêtes
- **Prometheus** collecte toutes les 30 secondes
- **Grafana** visualise : API status, CPU/RAM VM, fraîcheur des données, runs Prefect
- Branche `feat/monitoring-grafana` — merge imminent

**Notes orateur :**
> "En bonus, j'ai ajouté une couche de monitoring. L'API FastAPI expose ses métriques de performance dans un format standard que Prometheus collecte toutes les 30 secondes. Grafana les visualise : est-ce que l'API répond ? En combien de temps ? La machine est-elle saturée ? Depuis quand les données n'ont pas été mises à jour ?"
>
> "C'est dans une PR qui sera mergée juste après la présentation."

---

## Récap structure des slides

| # | Titre court | Durée cible | Type |
|---|---|---|---|
| 1 | Mon flow — Prix boursiers | 30s | Slide texte |
| 2 | Bronze → Silver → Gold | 30s | Tableau / schéma |
| 3 | Orchestration — Vue d'ensemble | 45s | Slide texte |
| 4 | Pattern d'exécution Prefect | 45s | Slide texte |
| 5 | Prefect UI | 2min | Démo live |
| 6 | Dashboard Streamlit | 30s | Slide texte |
| 7 | Tour des 4 onglets | 1min30s | Démo live |
| 8 | Monitoring | 30s | Slide texte |

**Total : ~7 min** (flow 1min + infra 2min + Prefect UI 2min + Streamlit 2min)
