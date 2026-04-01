"""Run Google News RSS ingestion — fetch, dump to GCS, load matched rows to BQ Bronze."""

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bronze.rss_google_news import run

_default_referentiel = Path(__file__).parent.parent / "referentiel" / "boursorama_peapme_final.csv"
referentiel_path = os.environ.get("REFERENTIEL_PATH", str(_default_referentiel))
referentiel = pd.read_csv(referentiel_path)
run(referentiel)
