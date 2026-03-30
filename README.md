# pea-pme-pulse

Daily investment signal engine for PEA-PME holdings.

## Sources

| Source | Type | Clé de jointure |
|---|---|---|
| Boursorama PEA-PME | HTML scraping | `ticker_bourso`, `isin` (référentiel maître) |
| yfinance + ta | Python library | `isin` |
| AMF flux-amf-new-prod | REST API JSON v2 | `isin` |
| ABCBourse RSS | RSS XML | fuzzy match sur `nom` |
| Yahoo Finance FR RSS | RSS XML | fuzzy match sur `nom` |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```
