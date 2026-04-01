# Architecture & Naming Conventions

## Repository layout

```
src/
├── bronze/       # ingestion — one module per source (incl. boursorama)
├── silver/       # cleaning/parsing
└── gold/         # scoring & ranking
referentiel/      # static reference data files (CSV) — companies list used to match/join across sources
dbt/              # SQL transformation models running on BigQuery (Silver/Gold logic)
scripts/          # one-off dev/exploration scripts — never imported by pipeline code
tests/            # unit and integration tests — mirrors src/ structure (tests/bronze/, tests/silver/, tests/gold/)
docs/             # this folder
```

Pipeline code lives under `src/` · tooling and data files at root level.

> Note: `boursorama/` at root is temporary — the Boursorama scraper should move to `src/bronze/boursorama.py` to follow this layout.

---

## BigQuery — medallion datasets

One dataset per layer, one table per source:

```
project-pea-pme
├── bronze
│   ├── yahoo_rss
│   ├── amf
│   ├── abcbourse_rss
│   └── boursorama
├── silver
│   └── ... (naming convention TBD)
└── gold
    └── ... (naming convention TBD)
```

---

## Google Cloud Storage

GCS stores raw data that cannot be re-fetched. Two rules:
- **RSS feeds** — ephemeral, no history → full raw feed dumped to GCS at fetch time, then filtered + matched entries loaded to BQ Bronze
- **AMF** — full history available via API → GCS for auditability of original regulatory documents (PDFs/XMLs/jsonl)
- **yfinance** — structured data fetched by ISIN, always re-fetchable → straight to BQ Bronze, no GCS step

```
gs://project_bucket/
├── rss_yahoo/    # raw feed dumps, timestamped
├── rss_abcbourse/
└── amf/          # AMF PDFs/XMLs/jsonl
```

---

## Environment variable naming - TBD

| Variable | Example value |
|---|---|
| `GCP_PROJECT_ID` | `bootcamp-project-pea-pme` |
| `GCS_BUCKET_NAME` | `project-pea-pme` |


---

## Referentiel

Master referentiel: `boursorama/boursorama_peapme_final.csv` (569 companies)
Key columns: `name`, `ticker_bourso`, `isin`

Draft (5 companies for testing): `referentiel/companies_draft.csv`
