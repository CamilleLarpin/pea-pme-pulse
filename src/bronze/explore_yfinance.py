"""
Exploration yfinance — phase découverte.

On ne sauvegarde rien. On observe ce que yfinance retourne
pour nos 5 ISIN français avant de construire le vrai pipeline.

Questions auxquelles ce script répond :
  1. yfinance accepte-t-il les ISIN directement ?
  2. Quels champs .info sont disponibles et fiables ?
  3. .history() retourne-t-il des données OHLCV complètes ?
  4. Y a-t-il des trous dans les données ?
"""

import pandas as pd
import yfinance as yf

# ── 1. Chargement référentiel ─────────────────────────────────────────────────
# On part du CSV draft — 5 entreprises suffisent pour explorer.
# L'ISIN est l'identifiant universel (même code partout dans le monde).
# On va tester si yfinance l'accepte directement, ce qui nous éviterait
# de gérer la conversion ISIN → ticker Yahoo (FR0013333432 → THR.PA).

ref = pd.read_csv("referentiel/companies_draft.csv")
print(f"Référentiel chargé : {len(ref)} entreprises\n")
print(ref.to_string())
print()

# ── 2. Boucle d'exploration par ISIN ─────────────────────────────────────────
for _, row in ref.iterrows():
    isin = row["isin"]
    nom = row["nom"]
    ticker_bourso = row["ticker_bourso"]

    print(f"\n{'═'*65}")
    print(f"  {nom}")
    print(f"  ISIN          : {isin}")
    print(f"  Ticker Bourso : {ticker_bourso}")
    print(f"{'═'*65}")

    t = yf.Ticker(isin)

    # ── 2a. .info ────────────────────────────────────────────────────────────
    # .info est un dict ~100 clés : nom, secteur, pays, ratios financiers,
    # prix actuel, market cap, etc.
    # C'est la source principale pour le Financials Scorer (gold layer).
    # ATTENTION : yfinance peut retourner un dict vide ou avec des None
    # si l'ISIN n'est pas reconnu. On vérifie ça ici.
    print("\n── .info ──────────────────────────────────────────────────")
    try:
        info = t.info
        # Champs qu'on utilisera dans le pipeline
        champs_cles = [
            "shortName", "longName", "symbol",
            "exchange", "currency", "country",
            "sector", "industry",
            "marketCap", "currentPrice",
            "trailingPE", "forwardPE",
            "revenueGrowth", "grossMargins", "operatingMargins",
            "totalDebt", "freeCashflow",
            "fiftyDayAverage", "twoHundredDayAverage",
        ]
        print(f"  Nombre total de champs : {len(info)}")
        print(f"\n  Champs clés pipeline :")
        for k in champs_cles:
            v = info.get(k, "⚠️  ABSENT")
            print(f"    {k:<30} : {v}")

    except Exception as e:
        print(f"  ❌ ERREUR .info : {e}")

    # ── 2b. .history ─────────────────────────────────────────────────────────
    # .history() retourne un DataFrame OHLCV indexé par date.
    # C'est LA donnée centrale du Stocks Scorer :
    #   - Close → SMA50, SMA200 (Golden Cross)
    #   - Close → RSI
    #   - Close → MACD
    #   - High/Low/Close → Bandes de Bollinger
    #   - Volume → confirmation de signal
    # On teste d'abord 1 mois, puis 1 an pour voir la profondeur dispo.
    print("\n── .history(period='1mo') ──────────────────────────────────")
    try:
        hist_1m = t.history(period="1mo")
        if hist_1m.empty:
            print("  ⚠️  DataFrame VIDE — ISIN non reconnu ou pas de données")
        else:
            print(f"  Shape    : {hist_1m.shape}")
            print(f"  Colonnes : {hist_1m.columns.tolist()}")
            print(f"  Du       : {hist_1m.index.min().date()}")
            print(f"  Au       : {hist_1m.index.max().date()}")
            print(f"  NaN      : {hist_1m.isna().sum().to_dict()}")
            print(f"\n  3 dernières lignes :")
            print(hist_1m[["Open", "High", "Low", "Close", "Volume"]].tail(3).to_string())
    except Exception as e:
        print(f"  ❌ ERREUR .history 1mo : {e}")

    print("\n── .history(period='1y') ───────────────────────────────────")
    try:
        hist_1y = t.history(period="1y")
        if hist_1y.empty:
            print("  ⚠️  DataFrame VIDE")
        else:
            print(f"  Shape    : {hist_1y.shape}")
            print(f"  Du       : {hist_1y.index.min().date()}")
            print(f"  Au       : {hist_1y.index.max().date()}")
            # Trous = jours ouvrés sans données — signe de problème qualité
            nb_trous = hist_1y["Close"].isna().sum()
            print(f"  NaN Close: {nb_trous} ({'⚠️  trous détectés' if nb_trous > 0 else '✅ aucun trou'})")
    except Exception as e:
        print(f"  ❌ ERREUR .history 1y : {e}")

    # ── 2c. Actions corporatives ─────────────────────────────────────────────
    # Dividendes et splits : pas directement dans le scorer,
    # mais utile en silver layer pour vérifier que les cours
    # sont bien ajustés (cours brut ≠ cours ajusté après split).
    print("\n── Actions corporatives ────────────────────────────────────")
    try:
        divs = t.dividends
        splits = t.splits
        print(f"  Dividendes  : {len(divs)} entrées")
        print(f"  Stock splits: {len(splits)} entrées")
        if len(splits) > 0:
            print(f"  Derniers splits : {splits.tail(3).to_dict()}")
    except Exception as e:
        print(f"  ❌ ERREUR corporate actions : {e}")
