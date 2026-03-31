# Architecture & Naming Conventions

## Repository layout

```
src/
├── bronze/       # ingestion — one module per source (incl. boursorama)
├── silver/       # cleaning/parsing
└── gold/         # scoring & ranking
referentiel/      # referentiel files (CSV)
dbt/              # transformations (TBD)
scripts/          # dev/exploration scripts
tests/            # mirrors src/ structure
docs/             # this folder
```

Pipeline code lives under `src/` · tooling and data files at root level.

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
- **AMF** — full history available via API → GCS for auditability of original regulatory documents (PDFs/XMLs)
- **yfinance** — structured data fetched by ISIN, always re-fetchable → straight to BQ Bronze, no GCS step

```
gs://project_bucket/
├── rss_yahoo/    # raw feed dumps, timestamped
├── rss_abcbourse/
└── amf/          # AMF PDFs/XMLs
```

---

## Environment variable naming

Pattern: `{SERVICE}_{SOURCE}_{RESOURCE}`

| Variable | Example value |
|---|---|
| `GCP_PROJECT_ID` | `project-pea-pme` |
| `GCS_BUCKET_NAME` | `project_bucket` |
| `GCS_AMF_PREFIX` | `amf` |
| `GCS_YAHOO_PREFIX` | `rss_yahoo` |
| `GCS_ABCBOURSE_PREFIX` | `rss_abcbourse` |
| `BQ_AMF_DATASET` | `bronze` |
| `BQ_AMF_TABLE` | `amf` |
| `BQ_YAHOO_DATASET` | `bronze` |
| `BQ_YAHOO_TABLE` | `yahoo_rss` |

---

## Referentiel

Master referentiel: `boursorama/boursorama_peapme_final.csv` (569 companies)
Key columns: `name`, `ticker_bourso`, `isin`

Draft (5 companies for testing): `referentiel/companies_draft.csv`
