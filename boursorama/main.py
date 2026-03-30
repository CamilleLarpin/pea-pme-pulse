# boursorama/main.py
import csv
import time
from typing import Dict, Any, List

from .config import LETTERS, OUTPUT_CSV, DELAY_SECONDS
from .isin import enrich_with_isin
from .listing import scrape_listing_for_letter


def main():
    """
    Pipeline complet :
    1) scraping du listing par lettre
    2) dédoublonnage
    3) enrichissement ISIN
    4) export CSV final
    """
    all_rows: List[Dict[str, Any]] = []
    seen = set()

    # ------------------------------------------------------------
    # Étape 1 : récupération du listing sur 27 lettres
    # ------------------------------------------------------------
    for letter in LETTERS:
        print(f"[LISTING] Lettre {letter}...")
        rows = scrape_listing_for_letter(letter)

        for row in rows:
            # Clé de déduplication :
            # on privilégie ticker_bourso, sinon le couple (nom, URL détail)
            key = row.get("ticker_bourso") or (row.get("name"), row.get("detail_url"))
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        time.sleep(DELAY_SECONDS)

    print(f"[LISTING] Total lignes uniques : {len(all_rows)}")

    # ------------------------------------------------------------
    # Étape 2 : enrichissement ISIN pour chaque société
    # ------------------------------------------------------------
    for idx, row in enumerate(all_rows, start=1):
        name = row.get("name", "")
        print(f"[ISIN] {idx}/{len(all_rows)} - {name}")

        try:
            enrich_with_isin(row)
        except Exception as exc:
            # On ne bloque pas tout le run si une fiche pose problème
            row["isin"] = ""
            row["isin_error"] = str(exc)

        time.sleep(DELAY_SECONDS)

    # ------------------------------------------------------------
    # Export CSV
    # ------------------------------------------------------------
    fieldnames = [
        "name",
        "ticker_bourso",
        "isin",
        "detail_url",
        "last",
        "variation",
        "high",
        "low",
        "volume",
        "raw_data_ist_init",
    ]

    if any("isin_error" in r for r in all_rows):
        fieldnames.append("isin_error")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[OK] CSV final écrit dans : {OUTPUT_CSV}")


if __name__ == "__main__":
    main()