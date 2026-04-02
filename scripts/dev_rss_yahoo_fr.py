"""Dev script — explore Yahoo Finance FR RSS output and test matching."""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bronze.rss_yahoo_fr import FEED_URL, fetch_feed, match_companies

# --- Step 1: fetch and print raw titles ---
print(f"\n Fetching: {FEED_URL}\n")
entries = fetch_feed()

if not entries:
    print("No entries returned — check the feed URL.")
    sys.exit(1)

print(f"{len(entries)} entries fetched\n")
print("--- Titles ---")
for i, e in enumerate(entries):
    print(f"[{i:02d}] {e['title']}")

# --- Step 2: match against referentiel ---
referentiel_path = Path(__file__).parent.parent / "referentiel" / "boursorama_peapme_final.csv"
referentiel = pd.read_csv(referentiel_path)

print(f"\n--- Matching against {len(referentiel)} companies in referentiel ---\n")
df = match_companies(entries, referentiel)

matched = df[df["isin"].notna()]
unmatched = df[df["isin"].isna()]

print(f"Matched  : {len(matched)}")
print(f"Unmatched: {len(unmatched)}\n")

if not matched.empty:
    print("--- Matched entries ---")
    for _, row in matched.iterrows():
        print(f"  [{row['match_score']:.0f}] {row['title'][:70]}")
        print(f"       → {row['matched_name']} ({row['isin']})")
