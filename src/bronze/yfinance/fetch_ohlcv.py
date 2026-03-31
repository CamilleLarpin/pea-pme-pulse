# src/bronze/fetch_ohlcv.py
#
# Pipeline Bronze — OHLCV PEA-PME
# Source      : Yahoo Finance (via yfinance)
# Référentiel : referentiel/boursorama_peapme_final.csv
# Destination : BigQuery → dataset bronze, table yahoo_ohlcv
#
# Architecture Bronze/Silver :
#   Bronze (ce script) = données brutes OHLCV sans transformation
#   Silver (étape suivante) = indicateurs techniques calculés via ta
#     → RSI_14, MACD, MACD_signal, BB_upper/lower, EMA_20, EMA_50
#
# Flux :
#   boursorama_peapme_final.csv
#       → load_referentiel()        (nettoyage + déduplication ISIN)
#       → load_overrides()          (overrides manuels ISIN → ticker)
#       → isin_to_yf_ticker()       (résolution ISIN → ticker Yahoo)
#       → fetch_ohlcv()             (téléchargement historique complet)
#       → write_to_bigquery()       (écriture directe BQ, sans fichier local)

import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parents[3]

REFERENTIEL_PATH = ROOT / "referentiel" / "boursorama_peapme_final.csv"
OVERRIDES_PATH   = ROOT / "referentiel" / "ticker_overrides.json"

GCP_PROJECT_ID = "bootcamp-project-pea-pme"
BQ_DATASET     = "yfinance_bronze"
BQ_TABLE       = "yahoo_ohlcv"
BQ_TABLE_REF   = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

HISTORY_PERIOD = "max"

# Pause entre chaque appel Yahoo pour éviter le rate-limiting
# yfinance ne publie pas de limite officielle — 1s est conservateur et suffisant
SLEEP_BETWEEN_CALLS = 1.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

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

    log.info(
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
        log.info("Pas de fichier ticker_overrides.json — aucun override actif")
        return {}
    with open(path) as f:
        overrides = json.load(f)
    log.info(f"Overrides chargés : {len(overrides)} entrée(s)")
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
        log.info(f"  → override manuel : {overrides[isin]}")
        return overrides[isin]

    # 2. Résolution automatique Yahoo
    try:
        info = yf.Ticker(isin).info
        return info.get("symbol") or None
    except Exception as e:
        log.debug(f"Erreur résolution ISIN {isin} : {e}")
        return None


def fetch_ohlcv(isin: str, yf_ticker: str) -> pd.DataFrame | None:
    """
    Télécharge l'historique OHLCV complet depuis Yahoo Finance.

    period="max" : tout l'historique disponible pour ce ticker.
    Nécessaire pour garantir le warmup des indicateurs Silver :
    - EMA_50      : 50 jours minimum
    - MACD_signal : 33 jours minimum
    - RSI_14      : 14 jours minimum

    Colonnes retournées par yfinance :
      Open, High, Low, Close, Volume, Dividends, Stock Splits

    Colonnes ajoutées :
      isin        → clé de jointure avec le référentiel
      yf_ticker   → ticker utilisé (utile pour debug et Silver)
      ingested_at → timestamp UTC d'ingestion (traçabilité Bronze)
    """
    try:
        df = yf.Ticker(yf_ticker).history(period=HISTORY_PERIOD)

        if df.empty:
            return None

        df = df.reset_index()

        # Suppression du timezone : BQ attend DATETIME sans tz info
        # Le timezone Europe/Paris est implicitement conservé dans les valeurs
        df["Date"] = df["Date"].dt.tz_localize(None)

        df["isin"]        = isin
        df["yf_ticker"]   = yf_ticker
        df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

        return df

    except Exception as e:
        log.debug(f"Erreur fetch OHLCV {yf_ticker} ({isin}) : {e}")
        return None


def get_already_ingested_isins(client: bigquery.Client) -> set[str]:
    """
    Récupère les ISIN déjà présents dans BQ pour l'idempotence.

    Permet de relancer le script après un crash sans re-télécharger
    les entreprises déjà ingérées.

    Si la table n'existe pas encore (première exécution), retourne
    un set vide sans lever d'erreur.
    """
    query = f"SELECT DISTINCT isin FROM `{BQ_TABLE_REF}`"
    try:
        result = client.query(query).result()
        isins  = {row["isin"] for row in result}
        log.info(f"BQ : {len(isins)} ISIN déjà présents")
        return isins
    except Exception:
        log.info(f"Table {BQ_TABLE_REF} absente → première ingestion")
        return set()


def write_to_bigquery(client: bigquery.Client, df: pd.DataFrame) -> None:
    """
    Écrit un DataFrame dans BigQuery en mode APPEND.

    WRITE_APPEND : ajoute sans écraser — mode standard pour Bronze
    qui accumule les données dans le temps.

    autodetect=True : BQ infère les types depuis le DataFrame.
    Acceptable en Bronze. En Silver on définira un schéma explicite.
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, BQ_TABLE_REF, job_config=job_config)
    job.result()  # bloquant — attend confirmation avant de passer au suivant


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run() -> None:
    log.info("=== Démarrage pipeline Bronze OHLCV ===")
    log.info(f"Historique : {HISTORY_PERIOD} | Destination : {BQ_TABLE_REF}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    df_ref       = load_referentiel(REFERENTIEL_PATH)
    already_done = get_already_ingested_isins(client)
    overrides    = load_overrides(OVERRIDES_PATH)

    success, skipped, failed = [], [], []
    total = len(df_ref)

    for i, row in df_ref.iterrows():
        isin = row["isin"]
        name = row["name"]

        log.info(f"[{i+1}/{total}] {name} ({isin})")

        # Idempotence
        if isin in already_done:
            log.info("  → déjà ingéré, skip")
            skipped.append(isin)
            continue

        # Résolution ticker
        yf_ticker = isin_to_yf_ticker(isin, overrides)
        if not yf_ticker:
            log.warning("  → ticker Yahoo introuvable")
            failed.append(isin)
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        log.info(f"  → ticker Yahoo : {yf_ticker}")

        # Téléchargement OHLCV
        df_ohlcv = fetch_ohlcv(isin, yf_ticker)
        if df_ohlcv is None:
            log.warning("  → OHLCV vide")
            failed.append(isin)
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        # Écriture BQ
        try:
            write_to_bigquery(client, df_ohlcv)
            log.info(f"  → {len(df_ohlcv)} lignes écrites ({df_ohlcv['Date'].min().date()} → {df_ohlcv['Date'].max().date()})")
            success.append(isin)
        except Exception as e:
            log.error(f"  → Erreur écriture BQ : {e}")
            failed.append(isin)

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Bilan
    log.info("=" * 60)
    log.info(f"✅ Succès  : {len(success)}/{total}")
    log.info(f"⏭️  Skippés : {len(skipped)}/{total}")
    log.info(f"❌ Échecs  : {len(failed)}/{total}")
    if failed:
        log.warning("ISIN en échec :")
        for isin in failed:
            log.warning(f"  - {isin}")
    log.info("=== Pipeline terminé ===")


if __name__ == "__main__":
    run()
