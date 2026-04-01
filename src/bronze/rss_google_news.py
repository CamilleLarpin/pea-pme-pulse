"""Bronze ingestion — Google News RSS (Euronext Growth + PME Bourse FR)."""

import json
import re
from datetime import datetime, timezone

import feedparser
import pandas as pd
from google.cloud import bigquery, storage
from loguru import logger
from rapidfuzz import fuzz, process

FEED_URLS = {
    "euronext_growth": (
        "https://news.google.com/rss/search?q=Euronext+Growth+France&hl=fr&gl=FR&ceid=FR:fr"
    ),
    "pme_bourse_fr": (
        "https://news.google.com/rss/search?q=bourse+PME+France+small+cap&hl=fr&gl=FR&ceid=FR:fr"
    ),
}

BQ_PROJECT = "bootcamp-project-pea-pme"
BQ_DATASET = "bronze"
BQ_TABLE = "google_news_rss"

GCS_BUCKET = "project-pea-pme"
GCS_PREFIX = "rss_google_news"

MATCH_THRESHOLD = 80
SHORT_NAME_MAX_LEN = 6

# Names that are common words / publication names — skip them to avoid false positives
BLOCKLISTED_NAMES = {"OPTION", "FOCUS", "DIRECT", "CAPITAL", "CONTACT", "VISION"}

# Pattern to strip trailing source attribution " - source.fr" from Google News titles
_SOURCE_SUFFIX_RE = re.compile(r"\s+-\s+\S+\.\S{2,4}$")

# Pattern to detect a word boundary match of `name` inside `title`
_WORD_RE_CACHE: dict[str, re.Pattern] = {}


def _word_boundary_re(name: str) -> re.Pattern:
    if name not in _WORD_RE_CACHE:
        _WORD_RE_CACHE[name] = re.compile(
            r"(?<![a-zA-Z\u00C0-\u024F])" + re.escape(name) + r"(?![a-zA-Z\u00C0-\u024F])",
            re.IGNORECASE,
        )
    return _WORD_RE_CACHE[name]


def _clean_title(title: str) -> str:
    """Strip trailing '- source.com' attribution added by Google News."""
    return _SOURCE_SUFFIX_RE.sub("", title).strip()


def _valid_match(name: str, title: str) -> bool:
    """Post-match guard: name must appear as a whole word/token in the cleaned title."""
    return bool(_word_boundary_re(name).search(title))


def fetch_feed(feed_name: str, url: str) -> list[dict]:
    """Fetch a single Google News RSS feed and return raw entries."""
    feed = feedparser.parse(url)
    fetched_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "feed_name": feed_name,
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", ""),
            "fetched_at": fetched_at,
        }
        for entry in feed.entries
    ]


def fetch_all_feeds(urls: dict[str, str] = FEED_URLS) -> list[dict]:
    """Fetch all configured Google News RSS feeds and deduplicate by title."""
    all_entries = []
    seen_titles = set()
    for feed_name, url in urls.items():
        for entry in fetch_feed(feed_name, url):
            if entry["title"] not in seen_titles:
                seen_titles.add(entry["title"])
                all_entries.append(entry)
    return all_entries


def dump_to_gcs(entries: list[dict]) -> None:
    """Dump raw feed entries to GCS as timestamped JSON.

    RSS feeds have no history — raw dump preserves original data before any filtering.
    """
    client = storage.Client(project=BQ_PROJECT)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"{GCS_PREFIX}/{timestamp}.json"
    bucket = client.bucket(GCS_BUCKET)
    bucket.blob(blob_path).upload_from_string(
        json.dumps(entries, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info("GCS dump: gs://{}/{}", GCS_BUCKET, blob_path)


def _scorer_for(name: str):
    return fuzz.token_set_ratio if len(name) <= SHORT_NAME_MAX_LEN else fuzz.partial_ratio


def match_companies(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fuzzy-match article titles against company names (rapidfuzz >= 80).

    Short names (<=6 chars) use token_set_ratio to avoid false positives.
    Longer names use partial_ratio.
    Post-match: blocklist check + word-boundary validation to filter false positives.
    All entries are kept; unmatched have null isin/ticker_bourso.
    """
    company_names = referentiel["name"].tolist()
    rows = []
    for entry in entries:
        cleaned = _clean_title(entry["title"])
        best_match = None
        for name in company_names:
            if name.upper() in BLOCKLISTED_NAMES:
                continue
            scorer = _scorer_for(name)
            result = process.extractOne(
                cleaned,
                [name],
                scorer=scorer,
                score_cutoff=MATCH_THRESHOLD,
                processor=str.casefold,
            )
            if result and (best_match is None or result[1] > best_match[1]):
                if _valid_match(name, cleaned):
                    best_match = result
        if best_match:
            matched_name, score, _ = best_match
            ref_row = referentiel[referentiel["name"] == matched_name].iloc[0]
            rows.append(
                {
                    **entry,
                    "matched_name": matched_name,
                    "match_score": score,
                    "isin": ref_row["isin"],
                    "ticker_bourso": ref_row["ticker_bourso"],
                }
            )
        else:
            rows.append(
                {
                    **entry,
                    "matched_name": None,
                    "match_score": None,
                    "isin": None,
                    "ticker_bourso": None,
                }
            )
    return pd.DataFrame(rows)


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write matched records to BigQuery bronze.google_news_rss."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info("BQ load: {} rows → {}", len(df), table_id)


def run(referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fetch, dump to GCS, match, and load Google News RSS entries."""
    entries = fetch_all_feeds()
    dump_to_gcs(entries)
    df = match_companies(entries, referentiel)
    matched = df[df["isin"].notna()]
    if not matched.empty:
        write_to_bigquery(matched)
    else:
        logger.info("BQ load: no matched companies — skipping")
    return df
