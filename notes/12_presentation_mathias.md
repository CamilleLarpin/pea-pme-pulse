# 12 — Script présentation Mathias

Présentation de fin de bootcamp — vulgarisation pour une audience technique qui n'a pas suivi le projet.  
**Principe :** nommer le problème concret avant la solution technique. Chaque acronyme est défini la première fois.

---

## Mon flow — Pipeline prix boursiers yfinance (1 min)

**À dire :**

> "Mon rôle dans l'équipe, c'était les données de prix. Chaque soir en semaine à 19h30 — juste après la clôture des marchés — un pipeline automatique va chercher les cours boursiers de 462 sociétés françaises et les transforme en signaux d'analyse technique."

**Expliquer la chaîne simplement :**

1. **Collecte (Bronze)** — on interroge yfinance, une bibliothèque Python gratuite qui expose les données Yahoo Finance. On récupère pour chaque société : l'Ouverture, le Cours le plus Haut, le plus Bas, la Clôture et le Volume — abrégé OHLCV. Tout est stocké brut dans BigQuery.

2. **Nettoyage (Silver dbt)** — un modèle dbt supprime les éventuels doublons et valide les données. Si une journée est déjà en base, on ne la réinsère pas.

3. **Indicateurs techniques (Silver Python)** — on calcule 7 indicateurs avec la bibliothèque Python `ta` : RSI (mesure de survente/surachat), MACD (momentum), moyennes mobiles 50 et 200 jours, Bollinger Bands, et EMA 20 jours. Ces indicateurs sont écrits dans une deuxième table Silver.

4. **Scoring (Gold dbt)** — un modèle dbt transforme ces indicateurs en 5 signaux haussier/neutre/baissier, chacun valant 0, 1 ou 2 points. La somme donne un score technique de 0 à 10.

> "La séquence est critique : le nettoyage dbt doit tourner avant le Python, et le scoring dbt tourne après. C'est Prefect qui s'assure que tout s'enchaîne dans le bon ordre."

**Transition :**
> "Justement, parlons de Prefect et de l'infrastructure."

---

## #4 — Orchestration et infrastructure (2 min)

**Accroche :**
> "Maintenant que vous voyez ce que font les pipelines, je vais vous expliquer où et comment ils tournent."

**Points à couvrir dans l'ordre :**

**1. La VM**
> "Tout tourne sur une seule machine virtuelle chez Google Cloud — une e2-small, configuration modeste mais suffisante pour nos volumes. C'est sobre et ça coûte peu."

**2. Prefect**
> "Prefect c'est notre chef d'orchestre : il planifie les pipelines, les lance au bon moment, les rejoue en cas d'échec, et garde un historique de chaque exécution. On l'héberge nous-mêmes sur la VM plutôt que d'utiliser leur offre cloud payante — deux containers Docker : un serveur et un worker."

**3. Docker**
> "Les services infrastructure (Prefect, le reverse proxy nginx, Grafana, Prometheus) tournent dans des containers Docker — des boîtes isolées avec chacune leurs dépendances."

**4. Pattern clone-and-run**
> "À chaque exécution d'un pipeline, le worker Prefect clone la dernière version du code depuis GitHub et installe les dépendances. On n'a pas d'image Docker par pipeline — ça simplifie beaucoup la maintenance."

**5. FastAPI en dehors de Docker**
> "Notre API de données FastAPI tourne directement sur la machine, pas dans une boîte Docker. Elle est gérée par systemd — c'est le gestionnaire de services du système Linux, comme les services Windows en gros : il s'assure que FastAPI démarre automatiquement quand la machine reboot, et la redémarre si elle plante. On a fait ce choix pour une raison simple : un service qui tourne directement sur la machine peut s'identifier auprès de Google Cloud sans aucune configuration — Google le reconnaît automatiquement via des métadonnées de la VM."

**6. nginx**
> "nginx joue le rôle d'aiguilleur : selon l'URL demandée, il redirige vers Prefect, vers l'API FastAPI, ou vers Grafana."

**7. Sécurité**
> "Zéro fichier de clé secrète sur la machine. L'authentification Google Cloud passe par les métadonnées de la VM — c'est le mécanisme ADC, Application Default Credentials."

**Transition :**
> "Je vous montre l'interface Prefect en direct."

---

## #7 — Prefect UI (2 min)

**Accroche :**
> "Voici le centre de contrôle de tous nos pipelines. Tout ce qu'on vient de décrire, on peut le voir et le piloter ici."

**Chemin de démo — dans l'ordre :**

1. Ouvrir `http://35.241.252.5/prefect`
   > "La page d'accueil : les derniers runs, les succès en vert, les éventuels échecs en rouge."

2. **Deployments**
   > "Nos 10 pipelines déployés avec leurs schedules. Mon pipeline yfinance tourne du lundi au vendredi à 19h30."

3. Clic sur `yfinance-ohlcv-pipeline`
   > "Le détail : schedule cron, paramètres, et le bouton Run pour déclencher manuellement."

4. **Flow Runs**
   > "L'historique complet : durée de chaque exécution, état. On voit les 6 étapes s'enchaîner."

5. Clic sur un run récent → détail des tasks
   > "Bronze, nettoyage dbt, indicateurs Python, scoring dbt — chaque étape avec ses logs et sa durée. Si quelque chose casse à 19h45, on sait exactement où sans avoir à se connecter en SSH sur la machine."

6. **Work Pools**
   > "Le worker est connecté et en attente. S'il tombe, Prefect le détecte immédiatement."

**Transition :**
> "Le résultat de tout ça, c'est ce que les utilisateurs voient dans le dashboard Streamlit."

---

## #5 — Dashboard Streamlit (2 min)

**Accroche :**
> "Tout ce qu'on vient de voir — les pipelines, les scores — c'est ici que ça devient visible et utilisable."

**Tour du dashboard :**

1. **Sidebar**
   > "À gauche : la légende de lecture. Vert = score ≥ 6, opportunité. Jaune = neutre. Rouge = prudence. Et un bouton Actualiser pour vider le cache et recharger les données."

2. **Onglet Vue Composite**
   > "Le classement des 462 sociétés par score composite — une synthèse égale des 4 dimensions : prix, actualités, transactions dirigeants, et fondamentaux. Le graphique radar montre en un coup d'œil les forces et faiblesses d'une société."

3. **Onglet Actualités**
   > "Le score de sentiment des 45 derniers jours : combien d'articles ont été publiés sur cette société, et quel est leur ton moyen ?"

4. **Onglet Insiders & Fondamentaux**
   > "Les transactions boursières des dirigeants : quand un PDG rachète ses propres actions, c'est en général un signal positif. Et les métriques financières extraites des rapports AMF."

5. **Onglet Analyse Technique**
   > "La vue qui vient de mes données. Le heatmap montre en un coup d'œil quels signaux sont haussiers pour quelles sociétés ce jour-là."

**Détail technique :**
> "Les requêtes vers l'API FastAPI sont mises en cache une heure — pas de requête BigQuery à chaque clic. Le bouton Actualiser vide ce cache. Et au premier chargement, les ballons 🎈."

**Transition :**
> "Pour finir, une couche de monitoring que j'ai ajoutée."

---

## Monitoring (30 secondes)

> "En bonus, j'ai mis en place un monitoring avec Prometheus et Grafana."

> "L'API FastAPI expose ses métriques — temps de réponse, taux d'erreurs, nombre de requêtes — dans un format standard que Prometheus collecte toutes les 30 secondes. Grafana les visualise : est-ce que l'API répond ? La VM sature-t-elle ? Depuis quand les données n'ont pas été mises à jour ?"

> "C'est dans une PR qui sera mergée juste après la présentation."

---

## Points d'attention (ne pas se tromper)

| Sujet | Ce qu'il ne faut PAS dire | Ce qu'il faut dire |
|---|---|---|
| Score composite | "35% technique, 25% news..." | "25% pour chaque dimension — pondération égale" |
| FastAPI | "FastAPI tourne dans Docker" | "FastAPI tourne en systemd, hors Docker" |
| investment_score | "Un sentiment de X donne un score de Y" | "C'est un rang relatif parmi toutes les sociétés du jour" |
| bronze.yahoo_rss | Présenter comme fonctionnel | "Alimenté uniquement par Google News depuis fin mars, bug connu" |
| nao | Présenter comme finalisé | "Talk To My Data en cours de développement" |
