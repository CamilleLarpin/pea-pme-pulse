# Rapport professionnel
## RNCP37827 Bloc de compétences 01
## Collecte, stockage et mise à disposition des données d'un projet en IA

**Candidat :** Mathias Pinault
**Projet :** PEA-PME Pulse
**Formation :** Data Engineer / AI Engineer, Artefact School of Data
**Date :** Avril 2026

---

## Sommaire

1. Présentation du projet
   - 1.1. Contexte métier
   - 1.2. Architecture technique
   - 1.3. Mon périmètre

2. Compétence 1 : Collecte automatisée de données
   - 2.1. Source de données : Yahoo Finance via yfinance
   - 2.2. Référentiel des sociétés
   - 2.3. Résolution des identifiants (ISIN vers ticker)
   - 2.4. Mécanique d'idempotence incrémentale
   - 2.5. Téléchargement OHLCV et valeur de l'historique long
   - 2.6. Écriture en base de données
   - 2.7. Orchestration séquentielle avec Prefect

3. Compétence 2 : Préparation et nettoyage des données
   - 3.1. Nettoyage structurel par dbt
   - 3.2. Enrichissement par indicateurs techniques

4. Compétence 3 : Règles d'agrégation
   - 4.1. Score technique par société
   - 4.2. Score composite multi-dimensions

5. Compétence 4 : Base de données
   - 5.1. Choix de BigQuery
   - 5.2. Modèle de données en trois couches
   - 5.3. Schémas des tables principales
   - 5.4. Authentification et gestion des accès

6. Compétence 5 : Exposition via une API REST
   - 6.1. Choix technologiques (FastAPI, Uvicorn, nginx)
   - 6.2. Déploiement et architecture réseau
   - 6.3. Contrats de données et endpoints
   - 6.4. Sécurisation de l'API
     - 6.4.1. Ce qui est en place
     - 6.4.2. Limites actuelles
     - 6.4.3. Recommandations pour la mise en production
     - 6.4.4. Monitoring de l'API

7. Conclusion

Glossaire

---

## 1. Présentation du projet

### 1.1. Contexte métier

PEA-PME Pulse est un système d'aide à la décision pour les investisseurs particuliers souhaitant identifier des opportunités parmi les petites et moyennes entreprises cotées sur Euronext Growth et éligibles au dispositif fiscal PEA-PME. L'univers d'investissement couvert compte 462 sociétés françaises.

L'objectif est de croiser quatre dimensions d'analyse pour produire chaque jour un score composite par société, sur une échelle de 0 à 10 :

- la dimension **technique**, basée sur les cours boursiers (prix, volumes, indicateurs d'analyse chartiste),
- la dimension **éditoriale**, basée sur le sentiment des articles de presse financière des 45 derniers jours,
- la dimension **comportementale**, basée sur les transactions boursières réalisées par les dirigeants de chaque société,
- la dimension **fondamentale**, basée sur les résultats financiers publiés auprès de l'AMF (Autorité des Marchés Financiers).

### 1.2. Architecture technique

L'ensemble du projet repose sur une architecture en trois couches, appelée architecture médaillon, hébergée dans Google BigQuery (voir Glossaire) :

- La couche **Bronze** stocke les données brutes telles qu'elles arrivent des sources, sans aucune transformation.
- La couche **Silver** contient les données nettoyées, dédupliquées et enrichies (indicateurs calculés, textes normalisés).
- La couche **Gold** regroupe les scores métier finaux, directement consommés par l'API et le dashboard utilisateur.

Les transformations SQL sont gérées par dbt (voir Glossaire). L'orchestration des pipelines est assurée par Prefect 3 (voir Glossaire), hébergé sur une machine virtuelle GCP de type e2-small en zone europe-west1-b. L'API de données est développée avec FastAPI (voir Glossaire) et exposée via nginx (voir Glossaire).

### 1.3. Mon périmètre

Mon périmètre dans l'équipe couvrait trois domaines : le pipeline complet de données de prix boursiers (de la collecte brute au score technique), l'infrastructure d'hébergement partagée (VM, Prefect, nginx, monitoring), et l'API REST commune à toutes les dimensions du projet.

---

## 2. Compétence 1 : Collecte automatisée de données

### 2.1. Source de données : Yahoo Finance via yfinance

La source de données pour le pipeline de prix boursiers est Yahoo Finance, accessible via la bibliothèque Python `yfinance`. Yahoo Finance a mis fin à son API officielle en 2017. Depuis cette date, `yfinance` fonctionne en interrogeant des endpoints internes de Yahoo Finance non documentés et sans contrat de service officiel. Concrètement, la bibliothèque simule les requêtes d'un navigateur web pour récupérer les données de cours. Ce mode de fonctionnement implique deux risques à connaître : Yahoo peut modifier ses endpoints à tout moment sans préavis, et un volume trop élevé de requêtes peut déclencher un blocage temporaire (rate limiting). Dans le cadre de ce projet, ces risques sont acceptables. Le volume quotidien est de l'ordre de 500 appels, espacés de 0,5 seconde pour éviter de déclencher un éventuel blocage côté Yahoo (le seuil exact n'est pas documenté publiquement). Et dans un contexte académique, une interruption occasionnelle de la source de données ne constitue pas un risque critique. En environnement de production avec des contraintes de SLA (niveau de service garanti), il faudrait se tourner vers un fournisseur avec une API officielle et contractuelle, comme Alpha Vantage ou Refinitiv.

### 2.2. Référentiel des sociétés

Le pipeline traite uniquement les sociétés répertoriées dans le fichier `referentiel/boursorama_peapme_final.csv`. Ce fichier est produit par un pipeline dédié qui extrait automatiquement chaque mois, par scraping (lecture et analyse du code HTML de la page), la liste PEA-PME publiée par Boursorama. Ce scraping mensuel est nécessaire parce que l'univers des sociétés éligibles PEA-PME n'est pas figé : des introductions en bourse ont lieu régulièrement sur Euronext Growth, des sociétés peuvent être radiées ou passer d'un compartiment à l'autre, et Boursorama met à jour sa liste en conséquence. Un fichier statique figé en début de projet deviendrait rapidement incomplet.

Le fichier contient 571 entrées. Au chargement, la fonction `load_referentiel()` effectue trois opérations de nettoyage : suppression des lignes sans ISIN (voir Glossaire), vérification que chaque ISIN fait exactement 12 caractères (format international normalisé), et déduplication sur l'ISIN. Cette dernière étape est nécessaire car Boursorama peut lister une même société plusieurs fois avec des tickers différents (par exemple sur plusieurs marchés), mais un seul ISIN. On ne veut télécharger les données qu'une fois par société. On obtient ainsi 462 sociétés uniques.

Le code utilise pandas, une bibliothèque Python standard pour la manipulation de données tabulaires. pandas représente les données sous forme de DataFrames, c'est-à-dire des tableaux structurés en lignes et colonnes avec des types associés, comparables à des feuilles Excel en mémoire. Dans ce pipeline, pandas est utilisé à chaque étape intermédiaire : chargement des CSV, filtrage, et préparation des données avant leur écriture dans BigQuery.

```python
# src/bronze/yahoo_ohlcv_bronze.py
# Fonction : chargement et nettoyage du référentiel des sociétés
# pd est l'alias standard de la bibliothèque pandas

def load_referentiel(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["name", "isin"])
    df = df.dropna(subset=["isin"])
    df = df[df["isin"].str.len() == 12]
    df = df.drop_duplicates(subset=["isin"])
    return df.reset_index(drop=True)
```

### 2.3. Résolution des identifiants (ISIN vers ticker Yahoo)

Yahoo Finance indexe ses données par ticker boursier (par exemple "SOP.PA" pour Sopra Steria) et non par ISIN. La conversion entre les deux est nécessaire. La fonction `isin_to_yf_ticker()` procède en deux étapes. Elle consulte d'abord un fichier de surcharge manuelle `referentiel/ticker_overrides.json` qui recense les cas où la résolution automatique échoue : tickers renommés après une fusion ou un changement de marché, sociétés non référencées dans la base Yahoo avec leur ISIN. Si l'ISIN n'y figure pas, elle interroge directement Yahoo Finance en passant l'ISIN comme identifiant et en récupérant le ticker retourné.

```python
# src/bronze/yahoo_ohlcv_bronze.py
# Fonction : résolution ISIN vers ticker Yahoo Finance

def isin_to_yf_ticker(isin: str, overrides: dict) -> str | None:
    # Étape 1 : consultation des surcharges manuelles
    if isin in overrides:
        return overrides[isin]
    # Étape 2 : résolution automatique via Yahoo
    try:
        info = yf.Ticker(isin).info
        return info.get("symbol") or None
    except Exception as e:
        logger.warning(f"Erreur résolution ISIN {isin} : {e}")
        return None
```

Si aucun ticker ne peut être trouvé (société inconnue de Yahoo, data non disponible), l'ISIN est logué comme échec et le pipeline passe au suivant sans s'interrompre.

### 2.4. Mécanique d'idempotence incrémentale

Le pipeline est conçu pour être relançable à tout moment sans créer de doublons. Avant tout téléchargement, la fonction `get_last_dates()` interroge BigQuery pour récupérer la dernière date de séance déjà enregistrée pour chaque ISIN. Cette information permet au pipeline de ne télécharger que les séances postérieures à cette date.

```python
# src/bronze/yahoo_ohlcv_bronze.py
# Fonction : récupération des dernières dates déjà ingérées par ISIN

def get_last_dates(client: bigquery.Client) -> dict[str, date]:
    query = f"SELECT isin, MAX(Date) as last_date FROM `{BQ_TABLE_REF}` GROUP BY isin"
    try:
        result = client.query(query).result()
        return {row["isin"]: row["last_date"] for row in result}
    except Exception:
        return {}  # table absente lors de la première exécution
```

Si la table n'existe pas encore (première exécution du pipeline), la fonction retourne un dictionnaire vide et le téléchargement intégral est déclenché pour chaque société. Lors des runs quotidiens suivants, seules les nouvelles séances sont récupérées, ce qui réduit fortement le temps d'exécution et la charge réseau.

### 2.5. Téléchargement OHLCV et valeur de l'historique long

La fonction `fetch_ohlcv()` distingue deux cas selon qu'une date de départ est connue ou non.

```python
# src/bronze/yahoo_ohlcv_bronze.py
# Fonction : téléchargement de l'historique OHLCV depuis Yahoo Finance

def fetch_ohlcv(isin: str, yf_ticker: str, start_date: date | None = None) -> pd.DataFrame | None:
    if start_date is None:
        # Première ingestion : historique maximal disponible
        df = yf.Ticker(yf_ticker).history(period="max")
    else:
        # Runs quotidiens : uniquement les séances postérieures à la dernière connue
        df = yf.Ticker(yf_ticker).history(start=start_date + timedelta(days=1))

    if df.empty:
        return None

    df["Date"] = df["Date"].dt.date  # conversion en date pure (sans heure)
    df["isin"] = isin
    df["yf_ticker"] = yf_ticker
    df["ingested_at"] = datetime.now(UTC)  # horodatage d'ingestion pour la traçabilité
    return df
```

Lors de la première ingestion, on récupère tout l'historique disponible (`period="max"`), ce qui peut représenter jusqu'à 20 ans de données selon la société. La question se pose : 200 jours suffisent pour calculer les indicateurs techniques du jour, pourquoi stocker davantage ? C'est vrai pour le scoring quotidien, mais l'historique long présente d'autres avantages qui justifient ce choix.

D'abord, la profondeur historique permet d'analyser l'évolution des scores dans le temps : observer comment une société a réagi pendant la crise de 2020, identifier des patterns saisonniers, ou vérifier rétrospectivement si les signaux générés par le modèle étaient pertinents. Ensuite, le projet intègre une fonctionnalité "Talk to My Data" (Nao) qui permet aux utilisateurs d'interroger la base en langage naturel ; une base historique riche rend ce type d'analyse bien plus utile. Enfin, le coût de stockage reste marginal : la table `bronze.yfinance_ohlcv` compte aujourd'hui 2 007 629 lignes, ce qui représente quelques centaines de mégaoctets dans BigQuery. Le téléchargement intégral est un coût ponctuel, exécuté une seule fois lors de l'initialisation.

Les colonnes récupérées par yfinance constituent l'ensemble OHLCV (Open, High, Low, Close, Volume), complété par les dividendes et les opérations sur titres (splits), ainsi que l'ISIN, le ticker Yahoo et l'horodatage d'ingestion.

### 2.6. Écriture en base de données

L'écriture dans BigQuery se fait en mode `WRITE_APPEND` : les nouvelles lignes s'ajoutent à celles déjà présentes sans écraser l'existant. Le schéma de la table est défini explicitement dans le code Python sous forme d'une liste d'objets `SchemaField`, plutôt que de laisser BigQuery inférer les types automatiquement. Cette précaution est nécessaire car le mécanisme d'inférence automatique de BigQuery (appelé autodetect, voir Glossaire) peut mal interpréter certains types Python, notamment les objets `date` et `datetime`, et les convertir en chaîne de caractères au lieu de les stocker en tant que type `DATE` ou `TIMESTAMP` natif BigQuery. Un schéma explicite garantit la stabilité du contrat de données.

```python
# src/bronze/yahoo_ohlcv_bronze.py
# Schéma explicite de la table bronze.yfinance_ohlcv

BRONZE_SCHEMA = [
    bigquery.SchemaField("Date",         "DATE"),
    bigquery.SchemaField("Open",         "FLOAT"),
    bigquery.SchemaField("High",         "FLOAT"),
    bigquery.SchemaField("Low",          "FLOAT"),
    bigquery.SchemaField("Close",        "FLOAT"),
    bigquery.SchemaField("Volume",       "INTEGER"),
    bigquery.SchemaField("Dividends",    "FLOAT"),
    bigquery.SchemaField("Stock Splits", "FLOAT"),
    bigquery.SchemaField("isin",         "STRING"),
    bigquery.SchemaField("yf_ticker",    "STRING"),
    bigquery.SchemaField("ingested_at",  "TIMESTAMP"),
]
```

Une pause de 0,5 seconde est appliquée entre chaque appel Yahoo pour ne pas déclencher de blocage. Les erreurs par ISIN (ticker introuvable, données vides) sont logguées individuellement sans interrompre le pipeline. Un bilan final indique le nombre de succès, d'ISINs déjà à jour et d'échecs.

### 2.7. Orchestration séquentielle avec Prefect

Le pipeline complet est défini comme un "flow" Prefect dans `src/flows/yfinance_pipeline.py`. En Python, le `@flow` est un décorateur (voir Glossaire) qui indique à Prefect que cette fonction est un point d'entrée orchestrable : Prefect peut la planifier, l'exécuter, en conserver les logs et la rejouer en cas d'échec, sans modifier le code Python lui-même.

```python
# src/flows/yfinance_pipeline.py
# Flow Prefect : pipeline complet yfinance (Bronze -> Silver -> Gold)
# Planification : lundi au vendredi à 19h30 (cron : 30 19 * * 1-5)

@flow(name="yfinance-ohlcv-pipeline")
def yfinance_ohlcv_pipeline_flow(full_refresh: bool = False) -> None:
    yfinance_ohlcv_flow()                    # 1. Bronze  : téléchargement OHLCV
    dbt_deps()                               # 2. dbt     : vérification des dépendances
    dbt_run_yahoo_ohlcv_clean(full_refresh)  # 3. Silver  : nettoyage dbt
    yfinance_silver_compute()               # 4. Silver  : calcul des indicateurs Python
    dbt_run_stocks_score()                  # 5. Gold    : scoring technique dbt
    dbt_run_company_scores()               # 6. Gold    : score composite dbt
```

La séquence est strictement linéaire et intentionnelle : chaque étape consomme le résultat de la précédente. Le calcul des indicateurs Python (étape 4) ne peut pas démarrer avant que le nettoyage dbt (étape 3) ait terminé, et le scoring Gold (étape 5) ne peut pas s'exécuter sans les indicateurs Silver. Prefect garantit cet ordre et détecte immédiatement si une étape échoue, ce qui évite de propager une erreur silencieuse dans les couches suivantes.

Ce pipeline est déployé avec une planification de type cron `30 19 * * 1-5`. Cron est la syntaxe Unix standard pour exprimer des tâches récurrentes : ici, "à la 30ème minute de la 19ème heure, du lundi au vendredi". Cela déclenche le pipeline chaque soir de semaine à 19h30 heure de Paris, soit deux heures après la clôture des marchés européens à 17h30, laissant le temps aux données de se stabiliser chez Yahoo Finance.

#### Clone-and-run : simplicité vs reproductibilité

À chaque exécution, le worker Prefect clone la dernière version du dépôt GitHub et installe les dépendances Python via pip (le gestionnaire de paquets Python, qui télécharge et installe les bibliothèques depuis PyPI, le dépôt officiel Python) avant de lancer le flow.

Cette approche, dite "clone-and-run", se distingue de l'alternative classique qui consiste à packager le code dans une image Docker. Docker est une plateforme qui permet d'empaqueter une application avec l'intégralité de son environnement d'exécution (version de Python, bibliothèques, variables d'environnement) dans une unité portative et isolée appelée conteneur. Une image Docker est le modèle figé de ce conteneur, construite une fois (lors d'une étape appelée "build") et réutilisée à l'identique à chaque exécution. Cette approche est généralement recommandée en production car elle garantit que le code s'exécute toujours dans exactement le même environnement, quel que soit le moment ou la machine : les versions de toutes les bibliothèques sont figées dans l'image.

Le clone-and-run ne bénéficie pas de cette garantie : si pip installe une version légèrement différente d'une bibliothèque d'un run à l'autre (parce qu'une nouvelle version a été publiée entre-temps), le comportement peut changer sans que ce soit immédiatement visible. En pratique, ce risque est limité si les versions sont épinglées dans le fichier `requirements.txt`, mais il subsiste. Dans le cadre de ce projet, ce compromis est acceptable : pas de pipeline d'intégration et déploiement continus (CI/CD) à maintenir pour rebuilder les images à chaque modification du code, pas de référentiel centralisé d'images Docker (registry) à administrer. En contexte d'entreprise avec plusieurs équipes et des exigences de reproductibilité strictes, des images Docker versionnées seraient préférables.

---

## 3. Compétence 2 : Préparation et nettoyage des données

### 3.1. Nettoyage structurel par dbt (silver.yahoo_ohlcv_clean)

La première transformation est assurée par le modèle dbt `dbt/models/silver/yahoo_ohlcv_clean.sql`. Ce modèle ne calcule rien : son seul rôle est d'assainir les données brutes du Bronze avant de les transmettre à l'étape de calcul.

Le modèle est configuré en matérialisation incrémentale avec une stratégie `insert_overwrite` sur une partition mensuelle. Concrètement, BigQuery divise la table en partitions d'un mois chacune. À chaque run, dbt réécrit uniquement la partition du mois en cours (et éventuellement celle du mois précédent en début de mois, pour couvrir les arrivées tardives). Cette approche est bien plus efficace que de recalculer la table entière à chaque fois, surtout quand l'historique atteint plusieurs années.

```sql
-- dbt/models/silver/yahoo_ohlcv_clean.sql
-- Modèle dbt : nettoyage et déduplication des données OHLCV Bronze
-- Matérialisation : incrémentale, partition mensuelle sur la colonne Date

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'Date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['isin'],
) }}
```

Trois règles de nettoyage sont appliquées, toutes dans la CTE nommée `raw` :

```sql
-- Extrait de dbt/models/silver/yahoo_ohlcv_clean.sql
-- CTE "raw" : sélection depuis la source Bronze avec filtres de validité

with raw as (
    select Date, Open, High, Low, Close, Volume, isin, yf_ticker, ingested_at
    from {{ source('bronze', 'yfinance_ohlcv') }}
    where
        isin is not null
        and Close > 0          -- exclut les cours aberrants (suspensions, erreurs source)
        and Open > 0
        -- Exclut la séance en cours si le marché n'est pas encore fermé (avant 18h Paris)
        and Date <= if(
            extract(hour from datetime(current_timestamp(), 'Europe/Paris')) >= 18,
            current_date('Europe/Paris'),
            date_sub(current_date('Europe/Paris'), interval 1 day)
        )
)
```

La première règle filtre les prix aberrants : tout cours de clôture ou d'ouverture nul ou négatif est exclu. Ces valeurs peuvent apparaître lors de suspensions de cotation ou d'erreurs de source. La deuxième exclut la séance en cours si les marchés ne sont pas encore fermés (avant 18h heure de Paris), pour éviter d'insérer des données incomplètes dans Silver. La troisième est la déduplication sur le grain (voir Glossaire) (isin, Date).

Le grain d'une table est la définition de ce qu'une ligne représente : ici, une ligne représente une séance de bourse pour une société donnée. Deux lignes ne peuvent donc pas avoir le même couple (isin, Date). Si plusieurs lignes existent pour ce couple (parce que le pipeline a tourné plusieurs fois dans la même journée), on ne conserve que la plus récente selon l'horodatage d'ingestion. La clause `QUALIFY` utilisée dans le SQL est spécifique à BigQuery : elle permet de filtrer sur le résultat d'une fonction de fenêtrage comme `ROW_NUMBER()`. Un `WHERE` ordinaire ne fonctionnerait pas ici car, dans l'ordre d'exécution SQL, `WHERE` est évalué avant que les fonctions de fenêtrage soient calculées. `QUALIFY` s'applique après, ce qui évite d'avoir à imbriquer la requête dans une sous-requête supplémentaire.

```sql
-- Extrait de dbt/models/silver/yahoo_ohlcv_clean.sql
-- Sous-requête nommée "deduped" (CTE, voir Glossaire) : conservation de la ligne la plus récente par (isin, Date)
-- ROW_NUMBER() numérote les lignes par groupe (isin, Date), ordre décroissant sur ingested_at
-- QUALIFY filtre ensuite sur ce numéro (clause BigQuery, voir explication dans le texte)

deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by isin, Date
        order by ingested_at desc
    ) = 1
)
```


### 3.2. Enrichissement par indicateurs techniques (compute_silver.py)

Une fois les données nettoyées, le script Python `src/silver/compute_silver.py` calcule huit indicateurs techniques par société en utilisant la bibliothèque `ta` (Technical Analysis). Ces indicateurs sont calculés sur la série chronologique des cours de clôture, triée du plus ancien au plus récent.

Chaque indicateur est un calcul glissant sur une fenêtre de N jours. Les N premières lignes d'historique, insuffisantes pour remplir la fenêtre, produisent des valeurs `NaN` (Not a Number, c'est-à-dire "donnée absente"). C'est le comportement standard de la bibliothèque `ta`, appelé période de warmup : il faut attendre que la fenêtre de calcul soit pleine pour obtenir une valeur significative. Le warmup minimal varie selon les indicateurs (14 jours pour le RSI, 200 jours pour la SMA_200). Ces `NaN` sont conservés tels quels en Silver. La couche Gold les traitera comme des signaux neutres lors du scoring.

```python
# src/silver/compute_silver.py
# Fonction : calcul des indicateurs techniques sur un historique OHLCV trié par date
# Bibliothèque : ta (Technical Analysis Library for Python)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]  # tous les indicateurs sont calculés sur le cours de clôture

    df["RSI_14"]      = RSIIndicator(close=close, window=14).rsi()
    macd              = MACD(close=close)          # EMA12 - EMA26
    df["MACD"]        = macd.macd()
    df["MACD_signal"] = macd.macd_signal()         # EMA9 du MACD
    bb                = BollingerBands(close=close, window=20, window_dev=2)
    df["BB_upper"]    = bb.bollinger_hband()       # bande haute : moyenne 20j + 2 écarts-types
    df["BB_lower"]    = bb.bollinger_lband()       # bande basse : moyenne 20j - 2 écarts-types
    df["SMA_50"]      = SMAIndicator(close=close, window=50).sma_indicator()
    df["SMA_200"]     = SMAIndicator(close=close, window=200).sma_indicator()
    df["EMA_20"]      = EMAIndicator(close=close, window=20).ema_indicator()
    df["computed_at"] = datetime.now(UTC).replace(tzinfo=None)
    return df
```

Les huit indicateurs couvrent trois familles d'analyse technique :

- les indicateurs de **momentum** (RSI, MACD) évaluent si les acheteurs ou les vendeurs ont dominé les dernières séances. Le RSI compare les jours de hausse aux jours de baisse sur 14 séances : un RSI bas signale que les vendeurs ont pris le dessus de façon répétée (potentiel rebond), un RSI élevé que les acheteurs dominent depuis plusieurs jours (potentiel essoufflement). Le MACD compare une moyenne récente des cours à une moyenne plus ancienne ; quand la tendance courte dépasse la tendance longue, le titre reprend généralement de l'élan.
- les indicateurs de **volatilité** (bandes de Bollinger) tracent une fourchette autour de la moyenne des cours des 20 dernières séances. Quand le cours descend sous la bande basse, il s'est fortement écarté de sa valeur habituelle récente, ce qui précède souvent un rebond.
- les indicateurs de **suivi de tendance** (SMA 50 et 200 jours, EMA 20 jours) lissent les cours sur différentes fenêtres temporelles pour dégager la direction générale du marché, indépendamment des fluctuations quotidiennes.

#### Stratégie d'écriture : analyse critique

`compute_silver.py` lit la table `silver.yahoo_ohlcv_clean` (la table produite par le modèle dbt décrit en section 3.1) et calcule les indicateurs techniques pour chacun des 462 ISINs. Les résultats sont écrits dans une deuxième table Silver, `silver.yahoo_ohlcv`. Pour chaque ISIN, toutes ses lignes historiques (l'ensemble des couples ISIN + date) sont réécrites à chaque exécution.

La stratégie adoptée est la suivante : le premier ISIN traité déclenche un `WRITE_TRUNCATE` qui vide la table `silver.yahoo_ohlcv` dans son intégralité. Les 461 ISINs suivants utilisent `WRITE_APPEND` qui ajoute leurs lignes à la table. À la fin d'une exécution, la table contient les indicateurs calculés pour les 462 sociétés, sans aucune ligne résiduelle d'une exécution précédente.

Cette approche présente un avantage principal : sa simplicité. Il n'y a pas de logique d'upsert (mise à jour des lignes existantes + insertion des nouvelles), pas de gestion de clé d'unicité, et le résultat est identique quel que soit le nombre de fois où le pipeline est relancé dans la journée.

Son inconvénient est l'inefficacité : à chaque exécution, l'intégralité de l'historique de chaque ISIN est recalculée et réécrite, alors qu'une seule nouvelle date a été ajoutée depuis la veille. Sur 2 millions de lignes, ce retraitement complet représente un calcul et un volume d'écriture sans commune mesure avec le delta réel du jour.

Une alternative plus efficace consisterait à ne calculer et écrire que les nouvelles séances dans `silver.yahoo_ohlcv`, en s'appuyant sur `WRITE_APPEND` avec un filtre sur les dates déjà présentes, à l'image de la logique incrémentale appliquée en Bronze. Cette approche nécessiterait cependant de lire quand même un historique suffisant pour initialiser les indicateurs à longue fenêtre (200 jours pour SMA_200), ce qui complexifie la requête de lecture. Dans le contexte de ce projet, où les volumes restent gérables et les exécutions quotidiennes ne durent que quelques minutes, le compromis simplicité/performance a été fait en faveur de la lisibilité du code.

---

## 4. Compétence 3 : Règles d'agrégation

### 4.1. Score technique par société (gold.stocks_score)

Le modèle dbt `dbt/models/gold/stocks_score.sql` transforme les huit indicateurs calculés en Silver en cinq signaux binaires, puis additionne ces signaux pour produire le `score_technique`, compris entre 0 et 10.

Chaque signal vaut 0 (signal baissier), 1 (signal neutre ou données absentes) ou 2 (signal haussier). Le tableau suivant résume les règles :

| Signal | Condition haussière (2 pts) | Neutre (1 pt) | Baissier (0 pt) |
|---|---|---|---|
| RSI | RSI < 35 (survente) | 35 ≤ RSI < 65 ou NaN | RSI ≥ 65 (surachat) |
| MACD | MACD > MACD_signal | NaN | MACD ≤ MACD_signal |
| Golden Cross | SMA_50 > SMA_200 | NaN | SMA_50 ≤ SMA_200 |
| Bollinger (%B) | %B < 0,2 (bas de bande) | 0,2 ≤ %B ≤ 0,8 ou NaN | %B > 0,8 (haut de bande) |
| EMA Trend | Close > EMA_20 | NaN | Close ≤ EMA_20 |

Le %B de Bollinger se calcule selon la formule `(Close - BB_lower) / (BB_upper - BB_lower)`. Une valeur proche de 0 signifie que le cours est en bas de la bande de Bollinger (zone de survente potentielle), une valeur proche de 1 qu'il est en haut (zone de surachat potentielle). La division est protégée contre le cas où les deux bandes sont confondues (`NULLIF(BB_upper - BB_lower, 0)`) pour éviter une division par zéro.

```sql
-- dbt/models/gold/stocks_score.sql
-- CTE "scored" : calcul des 5 signaux techniques binaires depuis silver.yahoo_ohlcv

case
    when RSI_14 < 35  then 2.0
    when RSI_14 < 65  then 1.0
    when RSI_14 >= 65 then 0.0
    else 1.0  -- valeur NaN (warmup insuffisant) -> signal neutre
end as rsi_signal,

case
    when BB_lower is null or BB_upper is null                       then 1.0
    when (Close - BB_lower) / nullif(BB_upper - BB_lower, 0) < 0.2 then 2.0
    when (Close - BB_lower) / nullif(BB_upper - BB_lower, 0) > 0.8 then 0.0
    else 1.0
end as bollinger_signal,

-- score_technique : somme des 5 signaux, arrondi à 1 décimale, plage [0, 10]
round(
    rsi_signal + macd_signal + golden_cross_signal + bollinger_signal + trend_signal,
1) as score_technique
```

Le modèle produit également deux moyennes mobiles du score sur 7 et 14 jours (`score_7d_avg` et `score_14d_avg`), calculées par fenêtre glissante sur l'historique de chaque société. Ces colonnes permettent de lisser les signaux ponctuels et d'identifier les tendances de moyen terme, plutôt que de réagir à chaque fluctuation quotidienne.

Un drapeau booléen (voir Glossaire) `is_latest` est calculé sur chaque ligne : il vaut `TRUE` uniquement pour la séance la plus récente disponible par société. Ce drapeau est utilisé par l'API pour servir uniquement les données du jour sans avoir à faire de sous-requête `MAX(Date)` sur l'ensemble de la table.

### 4.2. Score composite multi-dimensions (gold.company_scores)

Le modèle `dbt/models/gold/company_scores.sql` agrège les quatre dimensions en un score composite journalier. Chaque dimension contribue à hauteur de 25%.

Une difficulté d'implémentation vient du fait que les scores ne partagent pas tous la même échelle : `score_news` et `score_insider` sont sur [1, 10], tandis que `score_technique` et `score_fondamental` sont sur [0, 10]. Avant pondération, les scores [1, 10] sont normalisés vers [0, 10] par la transformation `(x - 1) / 9,0 * 10`.

```sql
-- dbt/models/gold/company_scores.sql
-- Calcul du score composite : 25% par dimension, fallback 5.0 si dimension absente

round(
      coalesce((n.investment_score  - 1) / 9.0 * 10, 5.0) * 0.25   -- news
    + coalesce(st.score_technique,                   5.0) * 0.25   -- technique
    + coalesce((i.score_1_10       - 1) / 9.0 * 10, 5.0) * 0.25   -- insider
    + coalesce(f.score_fondamental,                  5.0) * 0.25,  -- fondamental
2) as composite_score
```

La fonction `COALESCE(valeur, 5.0)` remplace une dimension absente par 5,0 (le point médian de l'échelle [0, 10]). Ce choix est délibéré : une petite société peu couverte médiatiquement ou dont les dirigeants n'ont pas effectué de transaction récente ne doit pas être pénalisée dans son score composite. L'absence d'information est traitée comme une information neutre.

Ce modèle est matérialisé en mode incrémental avec `unique_key=['isin', 'score_date']`, ce qui garantit l'unicité d'une ligne par société par jour. Chaque run du pipeline yfinance du soir renouvelle le score composite pour l'ensemble des 462 sociétés.

---

## 5. Compétence 4 : Base de données

### 5.1. Choix de BigQuery

Le projet utilise Google BigQuery comme unique base de données analytique. Ce choix repose sur plusieurs critères. BigQuery est un service entièrement managé : il n'y a pas de serveur à administrer, pas de sauvegardes à configurer, et la mise à l'échelle est automatique. Il s'intègre nativement à l'écosystème Google Cloud utilisé dans le projet : la même bibliothèque Python `google-cloud-bigquery` est utilisée depuis les scripts de collecte, les flows Prefect et l'API FastAPI, avec la même mécanique d'authentification (décrite en section 5.4). dbt dispose d'un adaptateur BigQuery officiel et bien maintenu.

BigQuery propose également des mécanismes de partitionnement et de clustering. Le partitionnement consiste à diviser physiquement une table en tranches selon une colonne (ici la date, par mois). Quand on interroge une période récente, BigQuery ne lit que les partitions concernées plutôt que toute la table. Le clustering complète ce mécanisme en triant les lignes à l'intérieur de chaque partition selon une colonne donnée (ici l'ISIN) : BigQuery peut alors ignorer les blocs qui ne correspondent pas à l'ISIN recherché. À l'échelle de ce projet (2 millions de lignes), le gain est avant tout en temps de réponse (quelques secondes contre plusieurs dizaines sans optimisation), davantage qu'en coût ; l'impact financier sur BigQuery devient significatif à des volumes beaucoup plus importants.

Le pipeline yfinance écrit directement dans BigQuery sans passer par un stockage intermédiaire. D'autres pipelines du projet utilisent GCS (Google Cloud Storage, un système de fichiers cloud similaire à Amazon S3) : les dumps RSS et les PDF AMF y sont archivés pour l'auditabilité, et la table externe du référentiel Boursorama est hébergée dans un fichier CSV sur GCS que BigQuery lit directement comme si c'était une table.

Le choix de BigQuery est celui d'une base de données relationnelle (SQL) plutôt que d'une base orientée documents (NoSQL, comme MongoDB ou Elasticsearch). Les données de prix boursiers ont une structure tabulaire stable et bien définie : une ligne, une séance, des colonnes typées. Ce modèle s'y prête parfaitement. Les bases NoSQL seraient mieux adaptées à des données dont la structure varie d'un enregistrement à l'autre (documents JSON imbriqués, schémas flexibles) ou à des cas d'usage de recherche plein texte. Ce n'est pas la problématique ici.

Une alternative SQL envisageable aurait été PostgreSQL (relationnel, bon support dbt) ou Snowflake (entrepôt de données analytique comparable à BigQuery). Ces options auraient nécessité plus de configuration et d'infrastructure, sans avantage décisif pour un projet dont les pipelines de collecte, les secrets et la machine virtuelle sont déjà dans l'écosystème GCP.

### 5.2. Modèle de données en trois couches

BigQuery est un entrepôt de données analytique qui ne dispose pas de clés étrangères ni de mécanismes d'intégrité référentielle au sens des bases relationnelles classiques. Les dépendances entre tables sont garanties par l'ordre d'exécution imposé par Prefect, et non par des contraintes dans la base de données elle-même.

Le tableau suivant représente l'ensemble des flux de données du projet, de la collecte jusqu'à l'exposition. Les lignes marquées ★ correspondent à mon périmètre.

| Source externe | Bronze | Silver | Gold |
|---|---|---|---|
| ★ Yahoo Finance (yfinance) | `bronze.yfinance_ohlcv` | `silver.yahoo_ohlcv_clean` (dbt) puis `silver.yahoo_ohlcv` ★ (Python) | `gold.stocks_score` ★ |
| Google News RSS, Yahoo RSS, ABC Bourse RSS | `bronze.google_news_rss`, `bronze.yahoo_rss`, `bronze.abcbourse_*` | `silver.rss_articles` (dbt) + scoring Groq | `gold.article_sentiment` puis `gold.score_news` |
| AMF — transactions dirigeants | `bronze.amf_insiders` | `silver.amf_insider_signals` | `gold.score_insider` |
| AMF — rapports financiers | `bronze.amf_financials` (via GCS) | `silver.amf_financial_signal` (dbt) | `gold.financials_score` |
| Boursorama (scraping mensuel) | `bronze.boursorama` (via GCS/CSV) | `silver.companies` (référentiel 462 ISINs) | utilisé en JOIN dans tous les modèles Gold |

Les quatre scores Gold convergent dans `gold.company_scores` ★ (dbt), qui est ensuite exposé par l'API FastAPI ★ et consommé par le dashboard Streamlit.

La base est organisée en trois datasets distincts dans le projet `bootcamp-project-pea-pme`, correspondant aux trois couches du modèle médaillon :

| Dataset | Rôle | Accès |
|---|---|---|
| `bronze` | Données brutes, immutables, tracées | Scripts Python (écriture), dbt (lecture) |
| `silver` | Données nettoyées et enrichies | dbt (écriture), Python Silver (écriture), dbt Gold (lecture) |
| `gold` | Scores métier | dbt (écriture), API FastAPI (lecture) |

### 5.3. Schémas des tables principales

**Table `bronze.yfinance_ohlcv`**

| Colonne | Type BigQuery | Description |
|---|---|---|
| Date | DATE | Date de la séance boursière |
| Open | FLOAT | Cours d'ouverture |
| High | FLOAT | Plus haut de séance |
| Low | FLOAT | Plus bas de séance |
| Close | FLOAT | Cours de clôture |
| Volume | INTEGER | Nombre de titres échangés |
| Dividends | FLOAT | Dividende détaché (si applicable) |
| Stock Splits | FLOAT | Ratio de division d'actions |
| isin | STRING | Identifiant international de la valeur (12 caractères) |
| yf_ticker | STRING | Ticker utilisé pour l'appel Yahoo Finance |
| ingested_at | TIMESTAMP | Horodatage UTC d'ingestion |

**Table `silver.yahoo_ohlcv`**

Le schéma complet est défini dans le code Python sous forme de liste de `SchemaField`, à l'image du Bronze :

```python
# src/silver/compute_silver.py
# Schéma explicite de la table silver.yahoo_ohlcv
# Colonnes OHLCV + isin + yf_ticker reprises du Bronze, indicateurs ajoutés par compute_silver.py

SILVER_SCHEMA = [
    bigquery.SchemaField("Date",        "DATE"),
    bigquery.SchemaField("Open",        "FLOAT"),
    bigquery.SchemaField("High",        "FLOAT"),
    bigquery.SchemaField("Low",         "FLOAT"),
    bigquery.SchemaField("Close",       "FLOAT"),
    bigquery.SchemaField("Volume",      "INTEGER"),
    bigquery.SchemaField("isin",        "STRING"),
    bigquery.SchemaField("yf_ticker",   "STRING"),
    bigquery.SchemaField("RSI_14",      "FLOAT"),
    bigquery.SchemaField("MACD",        "FLOAT"),
    bigquery.SchemaField("MACD_signal", "FLOAT"),
    bigquery.SchemaField("BB_upper",    "FLOAT"),
    bigquery.SchemaField("BB_lower",    "FLOAT"),
    bigquery.SchemaField("SMA_50",      "FLOAT"),
    bigquery.SchemaField("SMA_200",     "FLOAT"),
    bigquery.SchemaField("EMA_20",      "FLOAT"),
    bigquery.SchemaField("computed_at", "DATETIME"),
]
```

Les colonnes `Dividends`, `Stock Splits` et `ingested_at` présentes dans le Bronze ne sont pas propagées en Silver : elles sont spécifiques à la couche de collecte brute et n'ont pas de valeur analytique pour le calcul des indicateurs.

| Colonne ajoutée par rapport au Bronze | Type BigQuery | Description |
|---|---|---|
| RSI_14 | FLOAT | Relative Strength Index sur 14 jours |
| MACD | FLOAT | Moving Average Convergence Divergence |
| MACD_signal | FLOAT | Ligne de signal du MACD (EMA 9 du MACD) |
| BB_upper | FLOAT | Bande de Bollinger supérieure (20j, 2 écarts-types) |
| BB_lower | FLOAT | Bande de Bollinger inférieure |
| SMA_50 | FLOAT | Moyenne mobile simple 50 jours |
| SMA_200 | FLOAT | Moyenne mobile simple 200 jours |
| EMA_20 | FLOAT | Moyenne mobile exponentielle 20 jours |
| computed_at | DATETIME | Horodatage de calcul des indicateurs |

### 5.4. Authentification et gestion des accès

Aucune clé JSON de compte de service n'est stockée sur la machine virtuelle. L'authentification Google Cloud repose sur le mécanisme ADC (Application Default Credentials, voir Glossaire) : la VM GCP dispose d'un compte de service attaché nommé `pea-pme-ingestor`. Le serveur de métadonnées de la VM (un service interne GCP accessible à l'adresse fixe `169.254.169.254`, injoignable depuis l'extérieur) délivre des tokens d'accès temporaires à la demande, sans aucune intervention manuelle. Le SDK Python `google-cloud-bigquery` interroge automatiquement ce serveur quand il ne trouve pas de fichier de clé.

Ce mécanisme présente plusieurs avantages : pas de secret à stocker ni à renouveler, pas de risque de fuite de credentials persistants, et les tokens ont une durée de vie courte (expiration automatique après une heure). En pratique, même si la machine virtuelle était compromise, un attaquant ne trouverait aucun fichier de credentials à exfiltrer.

---

## 6. Compétence 5 : Exposition via une API REST

### 6.1. Choix technologiques

#### 6.1.1. FastAPI

FastAPI est un framework Python pour la création d'API web. Son avantage principal dans ce projet est la génération automatique de documentation interactive à partir des types Python définis dans le code, sans aucune configuration manuelle. Cette documentation est accessible sur `/docs` sous la forme d'une interface Swagger UI : une page web qui liste tous les endpoints disponibles avec leur format de données attendu, et qui permet de les tester directement depuis le navigateur sans aucun outil externe. Il supporte nativement la validation des données d'entrée et de sortie via Pydantic (voir Glossaire), ce qui rend les contrats de données explicites et vérifiables automatiquement. Pour une équipe qui développe à la fois le backend de données et le dashboard consommateur, cette transparence est précieuse.

#### 6.1.2. Uvicorn

Uvicorn est le serveur qui fait réellement tourner FastAPI. HTTP (HyperText Transfer Protocol) est le protocole de communication du web : c'est lui qui définit comment un client (navigateur, script Python, application mobile) formule une requête et comment le serveur y répond. Uvicorn se charge de recevoir ces requêtes HTTP entrantes et de les transmettre à FastAPI pour traitement. Il est léger, rapide et bien adapté à une API de données principalement en lecture. Pour des charges plus élevées, on l'associe généralement à Gunicorn, un superviseur de processus qui démarre plusieurs instances Uvicorn en parallèle pour traiter davantage de requêtes simultanées. Pour ce projet, une seule instance suffit largement.

#### 6.1.3. nginx comme reverse proxy

nginx est un serveur web qui joue ici le rôle de reverse proxy (voir Glossaire). Un port est un numéro entre 0 et 65 535 qui identifie un service particulier sur une machine. Le port 80 est le port web standard : les navigateurs l'utilisent automatiquement pour HTTP, sans qu'on ait besoin de le saisir dans l'URL. Les ports 4200 (Prefect), 8000 (FastAPI) et 3000 (Grafana) sont les ports par défaut de ces applications ou les ports configurés au déploiement, sans caractère standardisé.

La machine virtuelle dispose d'une seule adresse IP publique (`35.241.252.5`), mais plusieurs services y sont actifs chacun sur leur propre port. Sans nginx, pour accéder à Prefect il faudrait taper `http://35.241.252.5:4200`, pour l'API `http://35.241.252.5:8000`, pour Grafana `http://35.241.252.5:3000`. Cela oblige l'utilisateur à connaître la topologie interne, et expose autant de points d'entrée réseau à sécuriser séparément. Avec nginx, tout passe par `http://35.241.252.5` (port 80, le seul port ouvert publiquement) : nginx lit le chemin de l'URL et redirige vers le bon service interne. C'est le point d'entrée unique pour la sécurité réseau.

### 6.2. Déploiement et architecture réseau

FastAPI est déployé comme service systemd (voir Glossaire) `pea-pme-api` directement sur la VM, hors Docker. Ce choix est lié à l'ADC : un processus qui tourne directement sur la VM atteint le serveur de métadonnées GCP sans configuration supplémentaire. Dans un container Docker, le réseau est isolé par défaut et nécessiterait une configuration spécifique (`network_mode: host` ou `extra_hosts: host.docker.internal:host-gateway`) pour atteindre le même réseau que la VM hôte.

nginx est configuré pour router les requêtes selon leur chemin :

| Chemin | Destination |
|---|---|
| `/` | Prefect UI (avec authentification par mot de passe) |
| `/gold/...`, `/health`, `/docs`, `/metrics` | FastAPI sur `10.132.0.2:8000` (IP interne de la VM) |
| `/grafana/` | Grafana sur le port 3000 |
| `/dbt-docs/` | Documentation dbt statique |

La racine `/` redirige vers l'interface Prefect avec une authentification par nom d'utilisateur et mot de passe. Concrètement, quand un navigateur accède à cette URL, nginx lui renvoie un code 401 qui lui demande d'afficher une fenêtre de saisie d'identifiant et de mot de passe. Le navigateur envoie les credentials encodés dans l'en-tête HTTP de la requête suivante. nginx les compare à un fichier `.htpasswd` (fichier texte contenant la liste des utilisateurs autorisés avec leurs mots de passe stockés sous forme de condensés cryptographiques) : si les credentials correspondent, la requête est transmise à Prefect. C'est le mécanisme HTTP Basic Auth, configuré entièrement côté nginx sans modifier le code de Prefect. La documentation de l'API FastAPI est accessible séparément sur `http://35.241.252.5/docs`, sans authentification.

### 6.3. Contrats de données et endpoints

Un endpoint est une URL spécifique à laquelle l'API répond à des requêtes HTTP. Chaque endpoint du projet correspond à une table Gold de BigQuery et retourne ses données au format JSON. L'ensemble des endpoints suit le même pattern : `GET /gold/<nom-de-la-ressource>/latest`.

#### 6.3.1. Modèles Pydantic : typage des réponses

Pydantic est une bibliothèque Python de validation et de typage des données. Dans FastAPI, chaque endpoint déclare le type exact de ses données de réponse sous forme d'un modèle Pydantic. FastAPI utilise cette déclaration pour valider automatiquement les données renvoyées par BigQuery, convertir les types si nécessaire, et générer la documentation OpenAPI (standard ouvert de description d'API REST, rendu sous forme interactive sur `/docs` via Swagger UI). Par exemple, le modèle `StockScore` décrit précisément ce que retourne l'endpoint `/gold/stocks-score/latest` :

```python
# src/api/main.py
# Modèle Pydantic : contrat de données pour l'endpoint /gold/stocks-score/latest

class StockScore(BaseModel):
    isin: str
    company_name: str
    yf_ticker: str
    date: date
    close: float
    score_technique: float
    score_7d_avg: float | None    # peut être absent (pas assez d'historique)
    rsi_signal: float
    macd_signal: float
    golden_cross_signal: float
    bollinger_signal: float
    trend_signal: float
```

#### 6.3.2. Endpoint principal : /gold/stocks-score/latest

Cet endpoint retourne le score technique du jour pour toutes les sociétés, en filtrant sur `is_latest = TRUE` et en triant par score décroissant. C'est lui qui est consommé par le tableau de bord Streamlit pour afficher le heatmap des signaux techniques.

```python
# src/api/main.py
# Endpoint GET /gold/stocks-score/latest
# Retourne : liste de StockScore, triée par score_technique décroissant

@app.get("/gold/stocks-score/latest", response_model=list[StockScore])
def get_latest_scores():
    query = f"""
        SELECT isin, company_name, yf_ticker, date, Close AS close,
               score_technique, score_7d_avg,
               rsi_signal, macd_signal, golden_cross_signal,
               bollinger_signal, trend_signal
        FROM `{GCP_PROJECT}.{GOLD_DATASET}.stocks_score`
        WHERE is_latest = TRUE
        ORDER BY score_technique DESC, score_7d_avg DESC
    """
    return [dict(row) for row in get_bq_client().query(query).result()]
```

L'endpoint `/gold/stocks-score/history` retourne l'historique du score sur une fenêtre paramétrable (1 à 365 jours) pour une liste d'ISINs fournie en paramètre. La validation des ISINs par expression régulière est décrite dans la section sécurité ci-dessous.

#### 6.3.3. Client BigQuery partagé

Pour éviter de créer une nouvelle connexion BigQuery à chaque requête HTTP, le client est instancié une seule fois et mis en cache par `@lru_cache` (voir Glossaire), un décorateur Python qui mémorise le résultat du premier appel et le réutilise sans ré-exécuter la fonction.

```python
# src/api/main.py
# Client BigQuery instancié une seule fois pour toute la durée de vie du process

@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)
```

### 6.4. Sécurisation de l'API

Ce point mérite une analyse honnête de ce qui a été mis en place, de ses limites et de ce qu'un contexte de production nécessiterait.

#### 6.4.1. Ce qui est en place

L'accès aux interfaces de Prefect et de Grafana est protégé par une authentification par identifiant et mot de passe via nginx (mécanisme HTTP Basic Auth décrit en section 6.2). Les credentials GCP ne sont pas stockés sur le serveur, grâce au mécanisme ADC décrit en section 5.4. Par ailleurs, FastAPI valide le format des ISINs passés en paramètre. Une injection SQL est une attaque où un utilisateur malveillant insère du code SQL dans un paramètre d'URL pour manipuler la requête exécutée par la base de données. Dans ce contexte, le risque concret est limité : le compte de service utilisé par l'API n'a que des droits en lecture sur BigQuery, et les données exposées sont des scores calculés depuis des cours boursiers publics. La validation reste une bonne pratique : elle utilise une regex (regular expression, ou expression régulière), c'est-à-dire un motif qui décrit le format attendu ("2 lettres majuscules + 10 caractères alphanumériques majuscules"), et refuse toute valeur qui ne respecte pas exactement ce format avant qu'elle n'atteigne la requête SQL.

```python
# src/api/main.py
# Validation des ISINs en paramètre avant injection dans la requête SQL

isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
invalid = [i for i in isins if not isin_re.match(i)]
if invalid:
    raise HTTPException(status_code=422, detail=f"ISINs invalides : {invalid}")
```

#### 6.4.2. Limites actuelles

Les endpoints FastAPI (`/gold/...`, `/docs`) sont accessibles sans aucune authentification depuis Internet. N'importe qui connaissant l'adresse IP et les URLs peut interroger la base de données. Le service tourne en HTTP (port 80), sans chiffrement : les données transitent en clair sur le réseau. Il n'y a pas de rate limiting (limitation du nombre de requêtes acceptées par période) sur l'API, ce qui laisse ouverte la possibilité d'une surcharge volontaire.

#### 6.4.3. Recommandations pour la mise en production

Avant d'examiner les mesures à prendre, il faut calibrer le niveau de risque réel. L'API expose des scores calculés à partir de cours boursiers publics, elle est en lecture seule, et elle ne traite ni données personnelles ni informations financièrement sensibles. Le profil de risque est donc très différent d'une API bancaire ou d'un service de santé.

La première priorité serait le chiffrement des communications. Aujourd'hui, les échanges entre le client et le serveur circulent en clair sur le réseau : n'importe qui capable d'intercepter le trafic entre les deux pourrait lire les données échangées. HTTPS (voir Glossaire) est la version sécurisée d'HTTP : nginx s'interpose entre le réseau et FastAPI et chiffre toutes les communications grâce au protocole TLS. La mise en place se résume à obtenir un certificat TLS via Let's Encrypt (une autorité de certification reconnue, gérée par une fondation à but non lucratif) et son outil Certbot, puis à ajouter quelques lignes dans la configuration nginx pour l'activer sur le port 443 (le port standard HTTPS).

Ensuite, les endpoints de l'API pourraient être protégés par une authentification. La solution la plus simple est la clé d'API : le client joint à chaque requête un en-tête HTTP spécial (`X-API-Key: ma_cle_secrete`) que FastAPI vérifie avant de traiter la requête. En FastAPI, cette vérification s'implémente sous forme d'une dépendance injectée dans chaque endpoint :

```python
# src/api/main.py — exemple d'implémentation d'une clé d'API
# La clé attendue est lue depuis une variable d'environnement (jamais en dur dans le code)

import os
from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    expected = os.environ.get("API_KEY")
    if not api_key or api_key != expected:
        raise HTTPException(status_code=401, detail="Clé d'API absente ou invalide")

# Protéger un endpoint : ajouter dependencies=[Depends(verify_api_key)]
@app.get("/gold/stocks-score/latest", response_model=list[StockScore],
         dependencies=[Depends(verify_api_key)])
def get_latest_scores():
    ...
```

Cette implémentation n'est pas déployée à ce jour (limite identifiée en section 6.4.2), mais le mécanisme FastAPI est standard et son ajout ne nécessiterait que quelques lignes de code. Pour des intégrations plus avancées entre systèmes, on utilise parfois OAuth2, un protocole d'autorisation où l'utilisateur s'authentifie une fois auprès d'un serveur dédié et obtient un jeton temporaire signé (token JWT) lui permettant d'accéder à l'API sans retransmettre ses credentials à chaque requête. OAuth2 est plus robuste mais aussi nettement plus complexe à déployer.

Il serait également utile d'ajouter du rate limiting : une limite du nombre de requêtes acceptées par adresse IP sur une période donnée. nginx le supporte nativement et protège contre un client mal configuré qui ferait des appels en boucle ou contre une tentative délibérée de saturation.

Le niveau de protection adapté à ce projet reste HTTPS et une clé d'API simple. Des solutions plus lourdes comme une API Gateway (service centralisé qui gère les droits d'accès, les quotas par client et l'audit de toutes les requêtes) se justifieraient pour une API commerciale exposée à de nombreux clients différents, mais sont disproportionnées ici.

#### 6.4.4. Monitoring de l'API

La bibliothèque `prometheus-fastapi-instrumentator` instrumente automatiquement FastAPI pour exposer des métriques de performance sur `/metrics` : nombre de requêtes par endpoint, latences (p50 est le temps de réponse médian, p95 signifie que 95% des requêtes ont répondu en moins de X millisecondes), taux d'erreurs HTTP.

Ces métriques sont collectées par Prometheus, une base de données spécialisée dans les données de performance. Prometheus fonctionne à rebours de ce qu'on pourrait attendre : ce ne sont pas les services qui lui envoient leurs métriques, c'est lui qui va les chercher ("scraper") toutes les 30 secondes sur l'endpoint `/metrics` de FastAPI. Il stocke chaque valeur avec son horodatage, ce qui permet d'observer les tendances dans le temps.

Grafana est l'outil de visualisation qui se connecte à Prometheus (et à BigQuery) pour afficher des graphiques et tableaux de bord en temps réel. Le dashboard du projet est accessible sur `http://35.241.252.5/grafana/` et regroupe l'état de l'API, la charge de la VM et la fraîcheur des données dans chaque table Gold. Un endpoint `/health` permet à des outils de supervision externes de vérifier la disponibilité du service en quelques millisecondes.

---

## 7. Conclusion

PEA-PME Pulse est un projet réalisé en deux semaines en équipe de quatre personnes, chacune responsable d'une dimension du score composite : données de prix (mon périmètre), actualités financières, transactions des dirigeants, et résultats financiers AMF. Ce travail collaboratif a été géré avec Git et GitHub : chaque développement a eu lieu sur une branche dédiée, avec des pull requests relues par l'équipe avant intégration dans la branche principale. Cette organisation a permis de travailler en parallèle sur des périmètres distincts tout en maintenant un historique clair des modifications et une base de code stable. Le dépôt du projet est consultable à l'adresse `https://github.com/Mathias-PP/pea-pme-pulse`.

Sur le plan technique, ce projet m'a permis de mettre en oeuvre l'ensemble des compétences du bloc BC01 dans un contexte de production réel. La collecte automatisée de cours boursiers pour 462 sociétés, leur transformation en indicateurs analytiques, la conception d'un schéma adapté aux besoins de performance, et l'exposition des résultats via une API REST avec contrat de données explicite (structure et types des réponses définis par des modèles Pydantic, documentation générée automatiquement) sont des problèmes concrets que j'ai résolus de bout en bout. Chaque choix technique (yfinance plutôt qu'une API payante, BigQuery pour l'intégration GCP, FastAPI pour la documentation automatique, ADC pour la sécurité des credentials) a été motivé par des contraintes réelles de temps, de coût et de maintenabilité.

Ce projet m'a également confronté aux limites d'un prototype réalisé en deux semaines, que j'ai cherché à documenter honnêtement plutôt qu'à minimiser : une API sans authentification ni HTTPS, un pattern clone-and-run moins reproductible que des images Docker versionnées, une bibliothèque yfinance sans garantie de service. Ces limites sont connues et les pistes d'amélioration sont claires. Leur identification fait partie intégrante du travail d'ingénierie.

---

## Glossaire

**ADC (Application Default Credentials).** Mécanisme d'authentification Google Cloud qui évite de stocker des fichiers de clés secrets. Sur une machine virtuelle GCP, le SDK Google interroge automatiquement le serveur de métadonnées interne de la VM (accessible à `169.254.169.254`) pour obtenir un token d'accès temporaire.

**GCS (Google Cloud Storage).** Service de stockage de fichiers cloud de Google, équivalent d'Amazon S3. Permet de stocker des fichiers de n'importe quel format (CSV, JSON, PDF) dans des "buckets" accessibles via des URLs. Dans ce projet, utilisé pour archiver les dumps RSS, les PDFs AMF, et pour héberger le fichier CSV du référentiel Boursorama lu par BigQuery comme table externe.

**Grafana.** Outil de visualisation de données qui se connecte à des sources comme Prometheus ou BigQuery pour afficher des graphiques et tableaux de bord en temps réel. Permet de créer des dashboards interactifs sans écrire de code front-end.

**HTTP (HyperText Transfer Protocol).** Protocole de communication du web qui définit comment un client (navigateur, application) formule une requête vers un serveur et comment ce dernier y répond. HTTP transmet les données en clair. HTTPS en est la version chiffrée (voir entrée HTTPS / TLS).

**CI/CD (Continuous Integration / Continuous Deployment).** Pratique consistant à automatiser la vérification et la mise en production du code à chaque modification. Un pipeline CI/CD peut, par exemple, lancer les tests automatiquement à chaque commit, puis reconstruire et déployer une image Docker si les tests passent.

**CTE (Common Table Expression).** Sous-requête nommée dans une requête SQL, introduite par `WITH nom AS (...)`. Permet de décomposer une requête complexe en étapes lisibles et réutilisables au sein de la même requête (dans ce projet : `raw`, `deduped` dans `yahoo_ohlcv_clean.sql`).

**API REST.** Interface de programmation exposée via le protocole HTTP. REST (Representational State Transfer) est un style d'architecture qui définit des conventions : les ressources sont identifiées par des URLs (`/gold/stocks-score/latest`), les opérations utilisent les verbes HTTP standards (GET pour lire, POST pour créer, etc.), et les réponses sont généralement au format JSON.

**Autodetect BigQuery.** Option de chargement qui demande à BigQuery d'inférer automatiquement les types de colonnes à partir des premières lignes des données. Pratique mais risqué pour les types ambigus (une colonne de dates Python stockée en `object` peut être interprétée comme `STRING` au lieu de `DATE`).

**BigQuery.** Service de base de données analytique managé par Google Cloud. Orienté requêtes SQL sur de grands volumes, il stocke les données en colonnes (format adapté à l'agrégation et à l'analyse) et facture à la quantité de données lues lors des requêtes.


**dbt (data build tool).** Outil de transformation de données qui permet d'écrire des transformations SQL sous forme de fichiers `.sql` versionnés, avec gestion des dépendances entre modèles, tests de données et documentation automatique.

**Docker.** Plateforme logicielle qui permet d'empaqueter une application avec tout son environnement d'exécution (version de Python, bibliothèques, configuration) dans une unité portable appelée conteneur. Une image Docker est le modèle figé de ce conteneur, construite lors d'une étape de "build" et réutilisée à l'identique à chaque lancement. Docker garantit que le code se comporte de la même façon quelle que soit la machine sur laquelle il tourne.

**Décorateur Python.** Syntaxe `@nom_decorateur` placée au-dessus d'une définition de fonction. Un décorateur enveloppe la fonction pour lui ajouter un comportement sans modifier son code. Exemples dans ce projet : `@flow` (signale à Prefect qu'une fonction est un pipeline orchestrable), `@app.get(...)` (enregistre une fonction comme handler d'un endpoint HTTP), `@lru_cache` (met en cache le résultat d'une fonction).

**pandas.** Bibliothèque Python standard pour la manipulation de données tabulaires. Elle fournit la structure de données DataFrame : un tableau en mémoire organisé en lignes et colonnes, avec des types associés. Importée sous l'alias `pd` par convention, elle est utilisée dans quasiment tous les scripts de collecte et de transformation de ce projet.

**Drapeau booléen (flag).** Colonne ou variable qui ne peut prendre que deux valeurs : `TRUE` ou `FALSE`. Dans ce projet, `is_latest` est un drapeau qui vaut `TRUE` uniquement pour la séance boursière la plus récente par société, permettant à l'API de filtrer rapidement les données du jour.

**Endpoint.** Point d'entrée d'une API, défini par une URL et un verbe HTTP. Chaque endpoint répond à un type de requête précis et retourne des données dans un format défini.

**FastAPI.** Framework Python pour la création d'API web. Il génère automatiquement la documentation interactive (Swagger UI sur `/docs`) à partir des types Python définis dans le code, et valide les données d'entrée et de sortie via Pydantic. Déployé avec le serveur Uvicorn.

**HTTPS / TLS.** HTTPS est la version sécurisée du protocole HTTP : les données échangées entre le client et le serveur sont chiffrées, ce qui empêche leur lecture en cas d'interception. Le chiffrement repose sur TLS (Transport Layer Security), un protocole cryptographique standard. Un certificat numérique (fourni par une autorité de certification comme Let's Encrypt) est nécessaire pour activer HTTPS.

**Grain.** En modélisation de données, le grain d'une table est la définition de ce qu'une ligne représente. Dans `bronze.yfinance_ohlcv`, le grain est (isin, Date) : une ligne = une séance boursière pour une société.

**ISIN.** International Securities Identification Number. Identifiant international normalisé pour les valeurs mobilières, composé de 12 caractères (2 lettres de code pays + 9 caractères alphanumériques + 1 chiffre de contrôle). Exemple : `FR0000130577` pour TotalEnergies.

**@lru_cache.** Décorateur Python de la bibliothèque standard qui mémorise le résultat d'un appel à une fonction. `maxsize=1` signifie qu'un seul résultat est mis en cache. Utilisé ici pour partager une instance unique du client BigQuery entre toutes les requêtes HTTP sans recréer une connexion à chaque fois.

**Métadonnées de la VM.** Service interne GCP accessible uniquement depuis la machine virtuelle elle-même, à l'adresse `169.254.169.254`. Il expose des informations sur la VM (nom, zone, compte de service attaché) et délivre des tokens d'authentification temporaires, sans aucun fichier de clé.

**nginx.** Serveur web polyvalent utilisé ici comme reverse proxy : il reçoit toutes les requêtes HTTP entrantes sur le port 80 et les redirige vers le service interne approprié selon le chemin de l'URL.

**OHLCV.** Acronyme désignant les cinq données fondamentales d'une séance boursière : Open (ouverture), High (plus haut), Low (plus bas), Close (clôture), Volume (nombre de titres échangés).

**Port réseau.** Numéro entre 0 et 65 535 qui identifie un service particulier sur une machine. Le port 80 est le port HTTP standard (les navigateurs l'utilisent automatiquement), le port 443 est le port HTTPS standard. Les autres numéros (4200 pour Prefect, 8000 pour FastAPI, 3000 pour Grafana) sont les ports par défaut ou configurés de ces applications.

**Prefect.** Outil d'orchestration de pipelines de données. Il planifie les exécutions, enregistre les logs, rejoue les étapes en échec et fournit une interface web pour surveiller l'ensemble des pipelines.

**Prometheus.** Base de données spécialisée dans le stockage et la requête de métriques de performance. Fonctionne en "scraping" : Prometheus va chercher les métriques toutes les X secondes sur les endpoints HTTP des services qu'il surveille, plutôt que d'attendre que ces services les lui envoient. Chaque valeur est stockée avec un horodatage, ce qui permet d'observer l'évolution dans le temps.

**Pydantic.** Bibliothèque Python de validation et de typage des données à l'exécution. Permet de définir des "modèles" de données (classes Python avec types annotés) que FastAPI utilise pour valider les entrées et les sorties de chaque endpoint.

**Reverse proxy.** Serveur intermédiaire qui reçoit les requêtes à la place des services finaux, les redirige selon des règles, et transmet les réponses. Permet de masquer la topologie interne du réseau et de centraliser la sécurité.

**systemd.** Gestionnaire de services du système Linux. Il démarre les processus au démarrage de la machine, les redémarre automatiquement en cas de crash, et centralise leurs logs. Équivalent des "Services Windows" dans l'environnement Linux.

**Ticker.** Symbole boursier abrégé identifiant une valeur sur une place de marché spécifique. Contrairement à l'ISIN qui est universel, un ticker dépend de la place de marché : la même société peut avoir des tickers différents selon qu'elle est cotée à Paris, Londres ou New York.

**VM (Machine Virtuelle).** Serveur émulé logiciellement sur une infrastructure physique Cloud. Dans ce projet, une VM GCP de type `e2-small` (1 vCPU partagé, 2 Go de RAM) héberge l'ensemble des services infrastructure.

**Warmup.** Période initiale pendant laquelle un indicateur technique ne peut pas encore être calculé faute d'un historique suffisant. Par exemple, une moyenne mobile à 200 jours (SMA_200) produit des valeurs `NaN` pour les 199 premières séances de données d'une société. Ce n'est qu'à partir de la 200ème séance que la fenêtre de calcul est pleine et que l'indicateur devient significatif.
