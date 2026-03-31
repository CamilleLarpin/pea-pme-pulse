"""
fetch_ohlcv.py — Ingestion Bronze : données de marché OHLCV.

Responsabilité unique : pour chaque entreprise du référentiel,
récupérer l'historique de cours via yfinance et sauvegarder
en CSV dans data/bronze/ohlcv/.

Conventions :
  - Un fichier par entreprise : {ISIN}.csv
  - Toujours les mêmes colonnes : date, open, high, low, close, volume, isin
  - Les colonnes Dividends et Stock Splits sont exclues ici
    (elles ont leur propre table en silver)
  - Les dates sont normalisées en UTC puis converties date simple
    (on retire le timezone pour éviter les problèmes de sérialisation)

Pourquoi un CSV par ISIN et pas un seul fichier ?
  → Chaque entreprise a son propre cycle de mise à jour.
  → Plus simple à re-ingérer partiellement sans tout écraser.
  → Correspond à la structure BigQuery cible (partition par ISIN).
"""

import logging
from pathlib import Path

import pandas as pd
import yfinance as yf

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Chemins ───────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]           # racine du projet
REF_PATH = ROOT / "referentiel" / "companies_draft.csv"
OUTPUT_DIR = ROOT / "data" / "bronze" / "ohlcv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)        # crée les dossiers si absents

# ── Paramètres ────────────────────────────────────────────────────────────────
PERIOD = "1y"          # profondeur historique demandée à yfinance
INTERVAL = "1d"        # granularité journalière

# Colonnes qu'on garde — on exclut Dividends et Stock Splits
COLS_KEEP = ["Open", "High", "Low", "Close", "Volume"]


def fetch_ohlcv(isin: str, period: str = PERIOD) -> pd.DataFrame | None:
    """
    Récupère l'historique OHLCV pour un ISIN via yfinance.

    Retourne un DataFrame propre ou None si échec / données vides.

    Nettoyages appliqués :
      - Suppression colonnes Dividends et Stock Splits
      - Ajout colonne 'isin' pour traçabilité
      - Index date normalisé (timezone supprimé, format date simple)
      - Colonnes renommées en snake_case
    """
    log.info(f"Fetching OHLCV  {isin}  period={period}")

    try:
        ticker = yf.Ticker(isin)
        df = ticker.history(period=period, interval=INTERVAL)
    except Exception as e:
        log.error(f"  ❌ yfinance erreur pour {isin} : {e}")
        return None

    if df.empty:
        log.warning(f"  ⚠️  DataFrame vide pour {isin} — ISIN non reconnu ?")
        return None

    # ── Nettoyage ─────────────────────────────────────────────────────────────
    df = df[COLS_KEEP].copy()

    # Normalisation timezone → date simple
    # yfinance retourne un DatetimeIndex timezone-aware (Europe/Paris)
    # BigQuery et pandas préfèrent des dates sans timezone pour les partitions
    df.index = df.index.tz_localize(None).normalize()
    df.index.name = "date"

    # Renommage snake_case
    df.columns = [c.lower() for c in df.columns]

    # Ajout ISIN pour traçabilité (utile quand on concatène en silver)
    df.insert(0, "isin", isin)

    log.info(f"  ✅ {len(df)} lignes  |  {df.index.min().date()} → {df.index.max().date()}")
    return df


def run():
    """Point d'entrée principal — itère sur le référentiel et sauvegarde."""
    log.info(f"Chargement référentiel : {REF_PATH}")
    ref = pd.read_csv(REF_PATH)
    log.info(f"{len(ref)} entreprises à traiter\n")

    results = {}

    for _, row in ref.iterrows():
        isin = row["isin"]
        nom = row["nom"]

        df = fetch_ohlcv(isin)

        if df is None:
            results[isin] = "❌ ÉCHEC"
            continue

        # Sauvegarde CSV
        out_path = OUTPUT_DIR / f"{isin}.csv"
        df.to_csv(out_path)
        log.info(f"  💾 Sauvegardé → {out_path.relative_to(ROOT)}\n")
        results[isin] = f"✅ {len(df)} lignes"

    # ── Récapitulatif ─────────────────────────────────────────────────────────
    log.info("─" * 55)
    log.info("RÉCAPITULATIF")
    log.info("─" * 55)
    for isin, status in results.items():
        nom = ref.loc[ref["isin"] == isin, "nom"].values[0]
        log.info(f"  {nom:<25} {isin}   {status}")


if __name__ == "__main__":
    run()
