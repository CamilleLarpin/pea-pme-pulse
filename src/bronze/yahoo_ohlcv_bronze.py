# src/bronze/yahoo_ohlcv_bronze.py
#
# Pipeline Bronze — OHLCV PEA-PME
# Source      : Yahoo Finance (via yfinance)
# Référentiel : referentiel/boursorama_peapme_final.csv
# Destination : BigQuery → dataset bronze, table yfinance_ohlcv
#
# Architecture Bronze/Silver :
#   Bronze (ce script) = données brutes OHLCV sans transformation
#   Silver (étape suivante) = indicateurs techniques calculés via ta
#     → RSI_14, MACD, MACD_signal, BB_upper/lower, EMA_20, SMA_50, SMA_200
#
# Flux :
#   boursorama_peapme_final.csv
#       → load_referentiel()        (nettoyage + déduplication ISIN)
#       → load_overrides()          (overrides manuels ISIN → ticker)
#       → get_last_dates()          (dernière date ingérée par ISIN — idempotence)
#       → isin_to_yf_ticker()       (résolution ISIN → ticker Yahoo)
#       → fetch_ohlcv()             (historique complet ou incrémental selon last_date)
#       → write_to_bigquery()       (écriture directe BQ, sans fichier local)

import json
import time
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from loguru import logger

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parents[2]

REFERENTIEL_PATH = ROOT / "referentiel" / "boursorama_peapme_final.csv"
OVERRIDES_PATH   = ROOT / "referentiel" / "ticker_overrides.json"

GCP_PROJECT_ID = "bootcamp-project-pea-pme"
BQ_DATASET     = "bronze"
BQ_TABLE       = "yfinance_ohlcv"
BQ_TABLE_REF   = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

HISTORY_PERIOD = "max"

# Pause entre chaque appel Yahoo pour éviter le rate-limiting
# yfinance ne publie pas de limite officielle — 0.5s est un compromis
# sûr pour ~560 ISINs. Remonter à 1.0 si des erreurs 429 apparaissent.
SLEEP_BETWEEN_CALLS = 0.5

# Schéma BQ explicite — garantit DATE et TIMESTAMP corrects
# sans dépendre de l'autodetect (qui peut mal interpréter les date objects pandas)
BRONZE_SCHEMA = [
    bigquery.SchemaField("Date",          "DATE"),
    bigquery.SchemaField("Open",          "FLOAT"),
    bigquery.SchemaField("High",          "FLOAT"),
    bigquery.SchemaField("Low",           "FLOAT"),
    bigquery.SchemaField("Close",         "FLOAT"),
    bigquery.SchemaField("Volume",        "INTEGER"),
    bigquery.SchemaField("Dividends",     "FLOAT"),
    bigquery.SchemaField("Stock Splits",  "FLOAT"),
    bigquery.SchemaField("isin",          "STRING"),
    bigquery.SchemaField("yf_ticker",     "STRING"),
    bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# loguru : pas de configuration nécessaire — format et niveau INFO par défaut

# ---------------------------------------------------------------------------
# Fonctions
# ---------------------------------------------------------------------------

def load_referentiel(path: Path) -> pd.DataFrame:
    """
    Charge le référentiel Boursorama.

    On garde uniquement name + isin — les autres colonnes
    (last, variation, high...) sont des snapshots temps réel
    qui ne servent pas ici.

    Filtre :
    - ISIN manquants (NaN)
    - ISIN de longueur incorrecte (un ISIN = exactement 12 caractères)

    Déduplication sur ISIN : certaines entreprises ont plusieurs
    tickers Boursorama (marchés différents) mais un seul ISIN —
    un seul téléchargement yfinance est nécessaire.
    """
    df = pd.read_csv(path, usecols=["name", "isin"])

    df = df.dropna(subset=["isin"])
    df = df[df["isin"].str.len() == 12]

    before = len(df)
    df = df.drop_duplicates(subset=["isin"])
    after  = len(df)

    logger.info(
        f"Référentiel : {after} entreprises uniques "
        f"({before - after} doublons ISIN retirés)"
    )
    return df.reset_index(drop=True)


def load_overrides(path: Path) -> dict:
    """
    Charge les overrides manuels ISIN → ticker Yahoo.

    Utile pour les cas où Yahoo Finance ne résout pas l'ISIN
    automatiquement (delisted partiel, ticker non standard...).

    Retourne un dict vide si le fichier est absent — le script
    fonctionne normalement sans overrides.

    Exemple de contenu ticker_overrides.json :
    {
        "FR0013295789": "TFF.PA"
    }
    """
    if not path.exists():
        logger.info("Pas de fichier ticker_overrides.json — aucun override actif")
        return {}
    with open(path) as f:
        overrides = json.load(f)
    logger.info(f"Overrides chargés : {len(overrides)} entrée(s)")
    return overrides


def isin_to_yf_ticker(isin: str, overrides: dict) -> str | None:
    """
    Résout un ISIN en ticker Yahoo Finance.

    Ordre de résolution :
    1. Override manuel (ticker_overrides.json) → prioritaire, pas d'appel réseau
    2. Résolution automatique via Yahoo Finance

    Yahoo Finance accepte les ISIN directement dans yf.Ticker().
    .info['symbol'] retourne le ticker natif YF (ex: ABSc.PA).
    Retourne None si l'ISIN est inconnu de Yahoo (delisted, illiquide).
    """
    # 1. Override manuel
    if isin in overrides:
        logger.info(f"  → override manuel : {overrides[isin]}")
        return overrides[isin]

    # 2. Résolution automatique Yahoo
    try:
        info = yf.Ticker(isin).info
        return info.get("symbol") or None
    except Exception as e:
        logger.warning(f"Erreur résolution ISIN {isin} : {e}")
        return None


def fetch_ohlcv(isin: str, yf_ticker: str, start_date: date | None = None) -> pd.DataFrame | None:
    """
    Télécharge l'historique OHLCV depuis Yahoo Finance.

    Première ingestion (start_date=None) :
      period="max" → tout l'historique disponible.
      Nécessaire pour le warmup des indicateurs Silver :
      - SMA_200     : 200 jours minimum
      - MACD_signal : 34 jours minimum
      - RSI_14      : 14 jours minimum

    Runs quotidiens Prefect (start_date fourni) :
      start=last_date+1 → uniquement les nouvelles lignes.
      Retourne None si aucune nouvelle donnée (ISIN déjà à jour).

    Colonnes retournées par yfinance :
      Open, High, Low, Close, Volume, Dividends, Stock Splits

    Colonnes ajoutées :
      isin        → clé de jointure avec le référentiel
      yf_ticker   → ticker utilisé (utile pour debug et Silver)
      ingested_at → timestamp UTC d'ingestion (traçabilité Bronze)
    """
    try:
        if start_date is None:
            df = yf.Ticker(yf_ticker).history(period=HISTORY_PERIOD)
        else:
            fetch_from = start_date + timedelta(days=1)
            df = yf.Ticker(yf_ticker).history(start=fetch_from)

        if df.empty:
            return None

        df = df.reset_index()

        # Conversion en DATE pure : la granularité est journalière,
        # l'heure retournée par yfinance est toujours 00:00:00 → inutile.
        # BQ stockera le type DATE (ex: 2024-01-15) au lieu de DATETIME.
        df["Date"] = df["Date"].dt.date

        df["isin"]        = isin
        df["yf_ticker"]   = yf_ticker
        df["ingested_at"] = datetime.now(timezone.utc)

        return df

    except Exception as e:
        logger.warning(f"Erreur fetch OHLCV {yf_ticker} ({isin}) : {e}")
        return None


def get_last_dates(client: bigquery.Client) -> dict[str, date]:
    """
    Récupère la dernière date ingérée par ISIN depuis BQ.

    Utilisé pour l'idempotence incrémentale : au lieu de skipper
    un ISIN entier (ancienne logique), on ne télécharge que les
    lignes postérieures à la dernière date connue.

    Avantage vs l'ancienne approche "ISIN présent → skip" :
    - Les runs quotidiens Prefect n'ajoutent que les nouvelles lignes
    - Un crash partiel est récupérable : le prochain run repart
      de la dernière date complète

    Si la table n'existe pas encore (première exécution), retourne
    un dict vide sans lever d'erreur.
    """
    query = f"SELECT isin, MAX(Date) as last_date FROM `{BQ_TABLE_REF}` GROUP BY isin"
    try:
        result = client.query(query).result()
        dates  = {row["isin"]: row["last_date"] for row in result}
        logger.info(f"BQ : {len(dates)} ISIN déjà présents")
        return dates
    except Exception:
        logger.info(f"Table {BQ_TABLE_REF} absente → première ingestion")
        return {}


def write_to_bigquery(client: bigquery.Client, df: pd.DataFrame) -> None:
    """
    Écrit un DataFrame dans BigQuery en mode APPEND.

    WRITE_APPEND : ajoute sans écraser — Bronze accumule les données dans le temps.
    Schéma explicite pour garantir DATE et TIMESTAMP corrects (vs autodetect
    qui peut mal interpréter les date objects Python stockés en object dtype pandas).
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=BRONZE_SCHEMA,
    )
    job = client.load_table_from_dataframe(df, BQ_TABLE_REF, job_config=job_config)
    job.result()  # bloquant — attend confirmation avant de passer au suivant


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run() -> dict:
    logger.info("=== Démarrage pipeline Bronze OHLCV ===")
    logger.info(f"Historique : {HISTORY_PERIOD} | Destination : {BQ_TABLE_REF}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    df_ref     = load_referentiel(REFERENTIEL_PATH)
    last_dates = get_last_dates(client)
    overrides  = load_overrides(OVERRIDES_PATH)

    success, skipped, failed = [], [], []
    total = len(df_ref)

    for i, row in df_ref.iterrows():
        isin = row["isin"]
        name = row["name"]

        logger.info(f"[{i+1}/{total}] {name} ({isin})")

        # Résolution ticker
        yf_ticker = isin_to_yf_ticker(isin, overrides)
        if not yf_ticker:
            logger.warning("  → ticker Yahoo introuvable")
            failed.append(isin)
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        logger.info(f"  → ticker Yahoo : {yf_ticker}")

        # Idempotence incrémentale : ne fetch que les lignes nouvelles
        start_date = last_dates.get(isin)
        if start_date:
            logger.info(f"  → dernière date en BQ : {start_date} — fetch depuis {start_date + timedelta(days=1)}")

        # Téléchargement OHLCV
        df_ohlcv = fetch_ohlcv(isin, yf_ticker, start_date=start_date)
        if df_ohlcv is None:
            if start_date is not None:
                logger.info("  → déjà à jour, skip")
                skipped.append(isin)
            else:
                logger.warning("  → OHLCV vide")
                failed.append(isin)
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        # Filtre défensif : exclut les dates déjà présentes en BQ
        # Protège contre les doublons si get_last_dates() retourne une valeur périmée
        if start_date is not None:
            df_ohlcv = df_ohlcv[df_ohlcv["Date"] > start_date]
            if df_ohlcv.empty:
                logger.info("  → déjà à jour après filtrage, skip")
                skipped.append(isin)
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

        # Écriture BQ
        try:
            write_to_bigquery(client, df_ohlcv)
            logger.info(f"  → {len(df_ohlcv)} lignes écrites ({df_ohlcv['Date'].min()} → {df_ohlcv['Date'].max()})")
            success.append(isin)
        except Exception as e:
            logger.error(f"  → Erreur écriture BQ : {e}")
            failed.append(isin)

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Bilan
    logger.info("=" * 60)
    logger.info(f"✅ Succès  : {len(success)}/{total}")
    logger.info(f"⏭️  Skippés : {len(skipped)}/{total}")
    logger.info(f"❌ Échecs  : {len(failed)}/{total}")
    if failed:
        logger.warning("ISIN en échec :")
        for isin in failed:
            logger.warning(f"  - {isin}")
    logger.info("=== Pipeline terminé ===")

    return {"success": len(success), "skipped": len(skipped), "failed": len(failed), "total": total}


if __name__ == "__main__":
    run()
