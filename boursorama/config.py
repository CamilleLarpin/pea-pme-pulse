# boursorama/config.py
# ------------------------------------------------------------
# Étape unique :
# 1) Lire le listing Boursorama PEA-PME par lettre (A..Z + 0)
# 2) Extraire les infos marché depuis data-ist-init
# 3) Suivre le lien détail de chaque société
# 4) Extraire l'ISIN dans la fiche valeur
# 5) Exporter un CSV final prêt à être exploité ensuite pour le mapping yfinance
# ------------------------------------------------------------

# ---------------------------------------------------------------------
# Configuration générale du scraping
# ---------------------------------------------------------------------

BASE_URL = "https://www.boursorama.com/bourse/actions/cotations/"
MARKET = "PEAPME"

# 27 requêtes : A -> Z + 0
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]

# Délai entre requêtes pour éviter d'enchaîner trop vite
DELAY_SECONDS = 0.3

# Fichier CSV de sortie
OUTPUT_CSV = "boursorama_peapme_final.csv"

# En-têtes HTTP
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}