"""Shared fuzzy matching engine for Bronze RSS ingestion.

Single source of truth for company name matching across all RSS sources.
"""

import re

import pandas as pd
from rapidfuzz import fuzz, process

MATCH_THRESHOLD = 80
SHORT_NAME_MAX_LEN = 6

# Common words / publication names that coincide with company names — skip to avoid false positives
BLOCKLISTED_NAMES = {"OPTION", "FOCUS", "DIRECT", "CAPITAL", "CONTACT", "VISION"}

# Strip trailing source attribution added by Google News: " - source.fr"
_SOURCE_SUFFIX_RE = re.compile(r"\s+-\s+\S+\.\S{2,4}$")

_WORD_RE_CACHE: dict[str, re.Pattern] = {}


def _word_boundary_re(name: str) -> re.Pattern:
    if name not in _WORD_RE_CACHE:
        _WORD_RE_CACHE[name] = re.compile(
            r"(?<![a-zA-Z\u00C0-\u024F])" + re.escape(name) + r"(?![a-zA-Z\u00C0-\u024F])",
            re.IGNORECASE,
        )
    return _WORD_RE_CACHE[name]


def clean_title(title: str) -> str:
    """Strip trailing '- source.com' attribution from RSS titles."""
    return _SOURCE_SUFFIX_RE.sub("", title).strip()


def _scorer_for(name: str):
    return fuzz.token_set_ratio if len(name) <= SHORT_NAME_MAX_LEN else fuzz.partial_ratio


def match_companies(entries: list[dict], referentiel: pd.DataFrame) -> pd.DataFrame:
    """Fuzzy-match article titles against company names.

    Guards applied in order:
    1. clean_title — strip trailing source attribution (e.g. '- lefigaro.fr')
    2. BLOCKLISTED_NAMES — skip generic names that cause systematic false positives
    3. Adaptive scorer — token_set_ratio for short names (<=6 chars), partial_ratio for longer
    4. Word-boundary check — matched name must appear as a whole token in the cleaned title

    All entries are kept; unmatched have null matched_name / match_score / isin / ticker_bourso.
    """
    company_names = referentiel["name"].tolist()
    rows = []
    for entry in entries:
        cleaned = clean_title(entry["title"])
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
                if _word_boundary_re(name).search(cleaned):
                    best_match = result
        if best_match:
            matched_name, score, _ = best_match
            ref_row = referentiel[referentiel["name"] == matched_name].iloc[0]
            rows.append({
                **entry,
                "matched_name": matched_name,
                "match_score": score,
                "isin": ref_row["isin"],
                "ticker_bourso": ref_row["ticker_bourso"],
            })
        else:
            rows.append({
                **entry,
                "matched_name": None,
                "match_score": None,
                "isin": None,
                "ticker_bourso": None,
            })
    return pd.DataFrame(rows)
