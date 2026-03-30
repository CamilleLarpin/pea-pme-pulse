# boursorama/listing.py
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .config import BASE_URL, MARKET
from .helpers_json import extract_json_like, find_first_value
from .session_http import fetch_html


def parse_listing_row(tr) -> Optional[Dict[str, Any]]:
    """
    Parse une ligne du listing :
    - ticker_bourso depuis data-ist
    - données de marché depuis data-ist-init
    - URL détail pour l'enrichissement ISIN
    """
    ticker_bourso = tr.get("data-ist")
    data_ist_init_raw = tr.get("data-ist-init", "")

    # Lien vers la fiche détail
    link = tr.select_one("a[href*='/cours/']")
    detail_href = link.get("href") if link and link.has_attr("href") else None
    detail_url = urljoin(BASE_URL, detail_href) if detail_href else None

    # Nom affiché
    name = link.get_text(" ", strip=True) if link else None
    if not name:
        name = tr.get_text(" ", strip=True) or None

    # JSON inline des données de marché
    payload = extract_json_like(data_ist_init_raw)

    last = find_first_value(payload, ["last", "lastPrice", "price", "value"])
    variation = find_first_value(payload, ["variation", "change", "pctChange", "variationPercent"])
    high = find_first_value(payload, ["high", "dayHigh", "highPrice"])
    low = find_first_value(payload, ["low", "dayLow", "lowPrice"])
    volume = find_first_value(payload, ["volume", "tradedVolume", "turnover"])

    if not any([ticker_bourso, name, detail_url]):
        return None

    return {
        "name": name,
        "ticker_bourso": ticker_bourso,
        "detail_url": detail_url,
        "last": last,
        "variation": variation,
        "high": high,
        "low": low,
        "volume": volume,
        "raw_data_ist_init": data_ist_init_raw,
    }


def scrape_listing_for_letter(letter: str) -> List[Dict[str, Any]]:
    """
    Récupère toutes les sociétés d'une lettre donnée.
    """
    params = {
        "quotation_az_filter[market]": MARKET,
        "quotation_az_filter[letter]": letter,
    }

    html_text = fetch_html(BASE_URL, params=params)
    soup = BeautifulSoup(html_text, "html.parser")

    rows = []
    for tr in soup.select("tr[data-ist-init]"):
        item = parse_listing_row(tr)
        if item:
            rows.append(item)

    return rows