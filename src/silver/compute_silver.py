# src/silver/compute_silver.py
#
# Pipeline Silver — Indicateurs techniques OHLCV PEA-PME
# Source      : BigQuery → bronze.yfinance_ohlcv
# Destination : BigQuery → yfinance_silver.yahoo_ohlcv
#
# Indicateurs calculés via `ta` :
#   RSI_14       : Relative Strength Index (14 jours)
#   MACD         : Moving Average Convergence Divergence (EMA12 - EMA26)
#   MACD_signal  : Signal line du MACD (EMA 9 du MACD)
#   BB_upper     : Bande de Bollinger supérieure (20j, 2σ)
#   BB_lower     : Bande de Bollinger inférieure (20j, 2σ)
#   SMA_50       : Simple Moving Average 50 jours
#   SMA_200      : Simple Moving Average 200 jours
#   EMA_20       : Exponential Moving Average 20 jours
#
# Flux :
#   bronze.yfinance_ohlcv (BQ)
#       → load_bronze_isins()      (liste des ISIN distincts en Bronze)
#       → fetch_bronze_ohlcv()     (OHLCV brut d'un ISIN depuis BQ)
#       → compute_indicators()     (calcul indicateurs via ta)
#       → write_to_bigquery()      (écriture Silver, schéma explicite)
#
# Idempotence : WRITE_TRUNCATE sur le premier ISIN écrit (vide la table),
# puis WRITE_APPEND pour les suivants. Chaque run recompute l'intégralité
# depuis le Bronze — safe à relancer à tout moment.

import logging
from datetime import UTC, datetime

import pandas as pd
from google.cloud import bigquery
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator, SMAIndicator
from ta.volatility import BollingerBands

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GCP_PROJECT_ID = "bootcamp-project-pea-pme"

BQ_BRONZE_TABLE = f"{GCP_PROJECT_ID}.bronze.yfinance_ohlcv"
BQ_SILVER_TABLE = f"{GCP_PROJECT_ID}.yfinance_silver.yahoo_ohlcv"

# Schéma explicite Silver — contrairement au Bronze (autodetect),
# on fixe les types ici pour garantir la stabilité du contrat de données
# vers la couche Gold.
SILVER_SCHEMA = [
    bigquery.SchemaField("Date", "DATETIME"),
    bigquery.SchemaField("Open", "FLOAT"),
    bigquery.SchemaField("High", "FLOAT"),
    bigquery.SchemaField("Low", "FLOAT"),
    bigquery.SchemaField("Close", "FLOAT"),
    bigquery.SchemaField("Volume", "INTEGER"),
    bigquery.SchemaField("isin", "STRING"),
    bigquery.SchemaField("yf_ticker", "STRING"),
    bigquery.SchemaField("RSI_14", "FLOAT"),
    bigquery.SchemaField("MACD", "FLOAT"),
    bigquery.SchemaField("MACD_signal", "FLOAT"),
    bigquery.SchemaField("BB_upper", "FLOAT"),
    bigquery.SchemaField("BB_lower", "FLOAT"),
    bigquery.SchemaField("SMA_50", "FLOAT"),
    bigquery.SchemaField("SMA_200", "FLOAT"),
    bigquery.SchemaField("EMA_20", "FLOAT"),
    bigquery.SchemaField("computed_at", "DATETIME"),
]

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


def load_bronze_isins(client: bigquery.Client) -> list[str]:
    """
    Récupère la liste des ISIN distincts présents dans le Bronze.

    Point de départ du pipeline Silver : on ne traite que les ISIN
    effectivement disponibles en Bronze (459 sur 561 au lancement).
    """
    query = f"SELECT DISTINCT isin FROM `{BQ_BRONZE_TABLE}` ORDER BY isin"
    result = client.query(query).result()
    isins = [row["isin"] for row in result]
    log.info(f"Bronze : {len(isins)} ISIN distincts à traiter")
    return isins


def fetch_bronze_ohlcv(client: bigquery.Client, isin: str) -> pd.DataFrame:
    """
    Charge l'historique OHLCV complet d'un ISIN depuis le Bronze BQ.

    Tri par Date ascendant obligatoire : les indicateurs techniques
    (EMA, MACD, RSI...) sont des calculs glissants sensibles à l'ordre.

    On conserve uniquement les colonnes utiles au Silver —
    Dividends, Stock Splits et ingested_at sont des métadonnées Bronze
    qui ne font pas partie du contrat Silver.
    """
    query = f"""
        SELECT Date, Open, High, Low, Close, Volume, isin, yf_ticker
        FROM `{BQ_BRONZE_TABLE}`
        WHERE isin = '{isin}'
        ORDER BY Date ASC
    """
    df = client.query(query).to_dataframe()
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les indicateurs techniques sur un DataFrame OHLCV trié par Date.

    Tous les indicateurs sont calculés sur la colonne Close.
    Les N premières lignes sans historique suffisant retournent NaN —
    comportement standard de la lib `ta`, acceptable en Silver.

    Warmup minimum requis :
    - RSI_14      : 14 jours
    - MACD_signal : 34 jours (EMA26 + EMA9)
    - SMA_50      : 50 jours
    - BB_upper/lower : 20 jours
    - EMA_20      : 20 jours
    - SMA_200     : 200 jours  ← warmup le plus long
    """
    close = df["Close"]

    df["RSI_14"] = RSIIndicator(close=close, window=14).rsi()

    macd = MACD(close=close)
    df["MACD"] = macd.macd()
    df["MACD_signal"] = macd.macd_signal()

    bb = BollingerBands(close=close, window=20, window_dev=2)
    df["BB_upper"] = bb.bollinger_hband()
    df["BB_lower"] = bb.bollinger_lband()

    df["SMA_50"] = SMAIndicator(close=close, window=50).sma_indicator()
    df["SMA_200"] = SMAIndicator(close=close, window=200).sma_indicator()
    df["EMA_20"] = EMAIndicator(close=close, window=20).ema_indicator()

    df["computed_at"] = datetime.now(UTC).replace(tzinfo=None)

    return df


def write_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    first_write: bool,
) -> None:
    """
    Écrit un DataFrame Silver dans BigQuery.

    Premier ISIN (first_write=True) : WRITE_TRUNCATE — vide la table Silver
    avant d'écrire, garantissant qu'un run complet repart de zéro.

    ISINs suivants (first_write=False) : WRITE_APPEND — accumule.

    Le dataset `yfinance_silver` doit exister dans BQ avant le premier run
    (créer manuellement via la console GCP ou `bq mk`).
    """
    disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if first_write
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        schema=SILVER_SCHEMA,
    )
    job = client.load_table_from_dataframe(df, BQ_SILVER_TABLE, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run() -> None:
    log.info("=== Démarrage pipeline Silver OHLCV ===")
    log.info(f"Source : {BQ_BRONZE_TABLE}")
    log.info(f"Destination : {BQ_SILVER_TABLE}")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    isins = load_bronze_isins(client)

    success, failed = [], []
    total = len(isins)

    for i, isin in enumerate(isins):
        log.info(f"[{i + 1}/{total}] {isin}")

        try:
            df = fetch_bronze_ohlcv(client, isin)

            if df.empty:
                log.warning("  → Bronze vide pour cet ISIN, skip")
                failed.append(isin)
                continue

            df = compute_indicators(df)
            write_to_bigquery(client, df, first_write=(i == 0))

            log.info(f"  → {len(df)} lignes écrites ({len(df.columns)} colonnes)")
            success.append(isin)

        except Exception as e:
            log.error(f"  → Erreur : {e}")
            failed.append(isin)

    log.info("=" * 60)
    log.info(f"Succès  : {len(success)}/{total}")
    log.info(f"Échecs  : {len(failed)}/{total}")
    if failed:
        log.warning("ISIN en échec :")
        for isin in failed:
            log.warning(f"  - {isin}")
    log.info("=== Pipeline terminé ===")


if __name__ == "__main__":
    run()
