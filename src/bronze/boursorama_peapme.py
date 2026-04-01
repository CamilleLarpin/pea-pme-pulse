
"""
Script autonome pour :
1) récupérer le listing Boursorama PEA-PME par lettre (A..Z + 0),
2) extraire les données marché,
3) suivre chaque lien détail,
4) extraire l'ISIN,
5) écrire / mettre à jour un CSV final.

Sortie :
    boursorama_peapme_final.csv
"""

import csv
import html
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BASE_URL = "https://www.boursorama.com/bourse/actions/cotations/"
MARKET = "PEAPME"
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]
DELAY_SECONDS = 0.3

OUTPUT_CSV = Path("boursorama_peapme_final.csv")

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

ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")


# ---------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = make_session()


def fetch_html(url: str, params: Optional[dict] = None) -> str:
    response = SESSION.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------

def extract_json_like(text: str) -> Any:
    if not text:
        return {}

    text = html.unescape(text).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}

    return {}


def find_first_value(obj: Any, keys: List[str]) -> Optional[Any]:
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key] not in (None, "", [], {}):
                return obj[key]
        for value in obj.values():
            found = find_first_value(value, keys)
            if found is not None:
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = find_first_value(item, keys)
            if found is not None:
                return found

    return None


# ---------------------------------------------------------------------
# Étape 1 : listing
# ---------------------------------------------------------------------

def parse_listing_row(tr) -> Optional[Dict[str, Any]]:
    ticker_bourso = tr.get("data-ist")
    data_ist_init_raw = tr.get("data-ist-init", "")

    # On récupère le lien détail tel quel, au lieu de fabriquer /cours/{ticker}/
    link = tr.select_one("a[href*='/cours/']")
    detail_href = link.get("href") if link and link.has_attr("href") else None
    detail_url = urljoin(BASE_URL, detail_href) if detail_href else None

    name = link.get_text(" ", strip=True) if link else None
    if not name:
        name = tr.get_text(" ", strip=True) or None

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


# ---------------------------------------------------------------------
# Étape 2 : ISIN
# ---------------------------------------------------------------------

def extract_isin(detail_html: str) -> Optional[str]:
    soup = BeautifulSoup(detail_html, "html.parser")

    # Sélecteur principal attendu
    h2 = soup.select_one("h2.c-faceplate__isin")
    if h2:
        text = h2.get_text(" ", strip=True)
        match = ISIN_RE.search(text)
        if match:
            return match.group(0)

    # Fallback : recherche dans tout le texte de la page
    page_text = soup.get_text(" ", strip=True)
    match = ISIN_RE.search(page_text)
    if match:
        return match.group(0)

    return None


def enrich_with_isin(row: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = row.get("detail_url")
    if not detail_url:
        row["isin"] = ""
        return row

    detail_html = fetch_html(detail_url)
    row["isin"] = extract_isin(detail_html) or ""
    return row


# ---------------------------------------------------------------------
# Chargement / sauvegarde CSV
# ---------------------------------------------------------------------

def save_csv(rows: List[Dict[str, Any]], output_file: Path) -> None:
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

    if any("isin_error" in r for r in rows):
        fieldnames.append("isin_error")

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------

def main() -> None:
    all_rows: List[Dict[str, Any]] = []
    seen = set()

    # 1) Listing
    for letter in LETTERS:
        print(f"[LISTING] Lettre {letter}...")
        try:
            rows = scrape_listing_for_letter(letter)
        except Exception as exc:
            print(f"[LISTING] ERREUR lettre {letter}: {exc}")
            time.sleep(DELAY_SECONDS)
            continue

        for row in rows:
            key = row.get("ticker_bourso") or (row.get("name"), row.get("detail_url"))
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        time.sleep(DELAY_SECONDS)

    print(f"[LISTING] Total lignes uniques : {len(all_rows)}")

    # 2) ISIN
    for idx, row in enumerate(all_rows, start=1):
        print(f"[ISIN] {idx}/{len(all_rows)} - {row.get('name', '')}")
        try:
            enrich_with_isin(row)
        except Exception as exc:
            row["isin"] = ""
            row["isin_error"] = str(exc)
            print(f"[ISIN] ERREUR: {exc}")

        time.sleep(DELAY_SECONDS)

    # 3) Export
    save_csv(all_rows, OUTPUT_CSV)
    print(f"[OK] CSV mis à jour : {OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
