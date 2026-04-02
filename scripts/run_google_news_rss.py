"""Run Google News RSS ingestion — fetch, dump to GCS, load matched rows to BQ Bronze."""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bronze.rss_google_news import run

referentiel = pd.read_csv(
    Path(__file__).parent.parent / "referentiel" / "boursorama_peapme_final.csv"
)
run(referentiel)
