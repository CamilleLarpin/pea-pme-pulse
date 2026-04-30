"""Bronze — Boursorama PEA-PME company listing scraper.

Scrapes the full Boursorama PEA-PME listing (A–Z + 0), enriches each row
with its ISIN from the detail page, then uploads the result as a CSV to GCS.
The BQ external table bronze.boursorama points to that GCS path and
auto-reflects the new data on next query.
"""

import csv
import html
import io
import json
import re
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.boursorama.com/bourse/actions/cotations/"
MARKET = "PEAPME"
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]
DELAY_SECONDS = 0.3

GCS_BUCKET = "pea-pme-yf"
GCS_OBJECT = "boursorama_peapme_final.csv"

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

CSV_FIELDS = [
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
    "ingested_at",
]


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def _make_session() -> requests.Session:
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


_SESSION = _make_session()


def _fetch_html(url: str, params: dict | None = None) -> str:
    response = _SESSION.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _extract_json_like(text: str) -> Any:
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
            pass
    return {}


def _find_first_value(obj: Any, keys: list[str]) -> Any | None:
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key] not in (None, "", [], {}):
                return obj[key]
        for value in obj.values():
            found = _find_first_value(value, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_first_value(item, keys)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------


def _parse_listing_row(tr) -> dict | None:
    ticker_bourso = tr.get("data-ist")
    data_ist_init_raw = tr.get("data-ist-init", "")

    link = tr.select_one("a[href*='/cours/']")
    detail_href = link.get("href") if link and link.has_attr("href") else None
    detail_url = urljoin(BASE_URL, detail_href) if detail_href else None

    name = link.get_text(" ", strip=True) if link else None
    if not name:
        name = tr.get_text(" ", strip=True) or None

    payload = _extract_json_like(data_ist_init_raw)
    last = _find_first_value(payload, ["last", "lastPrice", "price", "value"])
    variation = _find_first_value(payload, ["variation", "change", "pctChange", "variationPercent"])
    high = _find_first_value(payload, ["high", "dayHigh", "highPrice"])
    low = _find_first_value(payload, ["low", "dayLow", "lowPrice"])
    volume = _find_first_value(payload, ["volume", "tradedVolume", "turnover"])

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


def _scrape_listing_for_letter(letter: str) -> list[dict]:
    params = {
        "quotation_az_filter[market]": MARKET,
        "quotation_az_filter[letter]": letter,
    }
    html_text = _fetch_html(BASE_URL, params=params)
    soup = BeautifulSoup(html_text, "html.parser")
    rows = []
    for tr in soup.select("tr[data-ist-init]"):
        item = _parse_listing_row(tr)
        if item:
            rows.append(item)
    return rows


def _extract_isin(detail_html: str) -> str | None:
    soup = BeautifulSoup(detail_html, "html.parser")
    h2 = soup.select_one("h2.c-faceplate__isin")
    if h2:
        match = ISIN_RE.search(h2.get_text(" ", strip=True))
        if match:
            return match.group(0)
    match = ISIN_RE.search(soup.get_text(" ", strip=True))
    return match.group(0) if match else None


def _enrich_with_isin(row: dict) -> dict:
    detail_url = row.get("detail_url")
    if not detail_url:
        row["isin"] = ""
        return row
    try:
        detail_html = _fetch_html(detail_url)
        row["isin"] = _extract_isin(detail_html) or ""
    except Exception as exc:
        row["isin"] = ""
        row["isin_error"] = str(exc)
    return row


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_companies() -> list[dict]:
    """Scrape the full Boursorama PEA-PME listing and return enriched rows."""
    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict] = []
    seen: set = set()

    for letter in LETTERS:
        try:
            rows = _scrape_listing_for_letter(letter)
        except Exception:
            time.sleep(DELAY_SECONDS)
            continue

        for row in rows:
            key = row.get("ticker_bourso") or (row.get("name"), row.get("detail_url"))
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        time.sleep(DELAY_SECONDS)

    for _idx, row in enumerate(all_rows, start=1):
        _enrich_with_isin(row)
        row["ingested_at"] = ingested_at
        time.sleep(DELAY_SECONDS)

    return all_rows


def upload_to_gcs(rows: list[dict]) -> str:
    """Serialize rows to CSV and upload to GCS. Returns the gs:// URI."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_OBJECT)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")

    uri = f"gs://{GCS_BUCKET}/{GCS_OBJECT}"
    return uri
