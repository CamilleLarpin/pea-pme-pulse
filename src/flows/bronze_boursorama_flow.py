# bronze_boursorama_flow.py
# ------------------------------------------------------------
# Prefect flow — Bronze Boursorama PEA-PME ingestion
# 1) Lire le listing Boursorama PEA-PME par lettre (A..Z + 0)
# 2) Extraire les infos marché depuis data-ist-init
# 3) Suivre le lien détail de chaque société
# 4) Extraire l'ISIN dans la fiche valeur
# 5) Exporter un CSV final
# ------------------------------------------------------------

from __future__ import annotations

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
from prefect import flow, task, get_run_logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------
# Configuration générale
# ---------------------------------------------------------------------

BASE_URL = "https://www.boursorama.com/bourse/actions/cotations/"
MARKET = "PEAPME"
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]

DELAY_SECONDS = 0.3
OUTPUT_CSV = Path(__file__).parent / "data" / "boursorama_peapme_final.csv"

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


# ---------------------------------------------------------------------
# Session HTTP avec retries
# ---------------------------------------------------------------------

def make_session() -> requests.Session:
    """
    Crée une session requests réutilisable avec retries.
    """
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


def fetch_html(url: str, params: Optional[dict] = None) -> str:
    """
    Fait un GET et renvoie le HTML brut.
    """
    session = make_session()
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------
# Helpers de parsing JSON / recherche de valeurs imbriquées
# ---------------------------------------------------------------------

def extract_json_like(text: str) -> Any:
    """
    Tente de parser une chaîne potentiellement JSON encodée / échappée.
    """
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
    """
    Parcourt récursivement un objet dict/list et renvoie la première valeur
    non vide trouvée pour une des clés demandées.
    """
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
# Parsing listing
# ---------------------------------------------------------------------

def parse_listing_row(tr) -> Optional[Dict[str, Any]]:
    """
    Parse une ligne <tr> du listing.
    """
    ticker_bourso = tr.get("data-ist")
    data_ist_init_raw = tr.get("data-ist-init", "")

    link = tr.select_one("a[href*='/cours/']")
    detail_href = link.get("href") if link and link.has_attr("href") else None
    detail_url = urljoin(BASE_URL, detail_href) if detail_href else None

    name = link.get_text(" ", strip=True) if link else None
    if not name:
        text = tr.get_text(" ", strip=True)
        name = text or None

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
# Parsing ISIN
# ---------------------------------------------------------------------

ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")


def extract_isin(detail_html: str) -> Optional[str]:
    """
    Extrait l'ISIN depuis la fiche détail.
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
    Télécharge la page détail et ajoute l'ISIN.
    """
    detail_url = row.get("detail_url")
    if not detail_url:
        row["isin"] = ""
        return row

    detail_html = fetch_html(detail_url)
    row["isin"] = extract_isin(detail_html) or ""
    return row


def export_csv(rows: List[Dict[str, Any]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)

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

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------

@task(name="boursorama-listing-fetch", retries=2, retry_delay_seconds=30)
def boursorama_listing_fetch() -> List[Dict[str, Any]]:
    """
    Récupère le listing complet sur toutes les lettres.
    """
    logger = get_run_logger()
    all_rows: List[Dict[str, Any]] = []
    seen = set()

    for letter in LETTERS:
        logger.info("[LISTING] Lettre %s...", letter)
        rows = scrape_listing_for_letter(letter)

        for row in rows:
            key = row.get("ticker_bourso") or (row.get("name"), row.get("detail_url"))
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        time.sleep(DELAY_SECONDS)

    logger.info("[LISTING] Total lignes uniques : %d", len(all_rows))
    return all_rows


@task(name="boursorama-enrich-isin", retries=2, retry_delay_seconds=30)
def boursorama_enrich_isin(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ajoute l'ISIN à chaque ligne.
    """
    logger = get_run_logger()

    enriched: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        name = row.get("name", "")
        logger.info("[ISIN] %d/%d - %s", idx, len(rows), name)

        try:
            enriched_row = enrich_with_isin(row)
        except Exception as exc:
            row["isin"] = ""
            row["isin_error"] = str(exc)
            enriched_row = row

        enriched.append(enriched_row)
        time.sleep(DELAY_SECONDS)

    return enriched


@task(name="boursorama-export-csv", retries=1, retry_delay_seconds=10)
def boursorama_export_csv(rows: List[Dict[str, Any]], output_csv: str) -> None:
    """
    Exporte le CSV final.
    """
    export_csv(rows, Path(output_csv))


# ---------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------

@flow(name="bronze-boursorama-peapme")
def boursorama_peapme_flow(output_csv: str = str(OUTPUT_CSV)) -> None:
    logger = get_run_logger()

    rows = boursorama_listing_fetch()
    enriched_rows = boursorama_enrich_isin(rows)
    boursorama_export_csv(enriched_rows, output_csv)

    matched = sum(1 for row in enriched_rows if row.get("isin"))
    logger.info(
        "boursorama_peapme_flow complete — %d sociétés, %d ISIN trouvés",
        len(enriched_rows),
        matched,
    )


if __name__ == "__main__":
    boursorama_peapme_flow()