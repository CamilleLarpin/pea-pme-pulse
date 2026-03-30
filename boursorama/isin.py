# boursorama/isin.py
import re
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup

from .session_http import fetch_html

ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")


def extract_isin(detail_html: str) -> Optional[str]:
    """
    Extrait l'ISIN depuis la page détail.
    Cible principale : h2.c-faceplate__isin
    Fallback : regex sur le texte complet de la page
    """
    soup = BeautifulSoup(detail_html, "html.parser")

    h2 = soup.select_one("h2.c-faceplate__isin")
    if h2:
        text = h2.get_text(" ", strip=True)
        match = ISIN_RE.search(text)
        if match:
            return match.group(0)

    page_text = soup.get_text(" ", strip=True)
    match = ISIN_RE.search(page_text)
    if match:
        return match.group(0)

    return None


def enrich_with_isin(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Télécharge la page détail et ajoute l'ISIN à la ligne.
    """
    detail_url = row.get("detail_url")
    if not detail_url:
        row["isin"] = ""
        return row

    detail_html = fetch_html(detail_url)
    row["isin"] = extract_isin(detail_html) or ""
    return row