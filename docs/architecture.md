# Architecture & Naming Conventions

## Repository layout

```
src/
├── bronze/       # ingestion — one module per source
├── silver/       # cleaning/parsing
└── gold/         # scoring & ranking
boursorama/       # referentiel scraper
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
│   ├── yahoo_rss
│   ├── amf
│   ├── abcbourse_rss
│   └── ohlc
└── gold
    ├── pea_pme_pulse     # composite scores
    └── ranking_top20
```

---

## Google Cloud Storage

GCS is used only for sources that produce raw files (PDFs, XMLs) before BQ load.
RSS and yfinance write directly to BigQuery — no GCS step.

```
gs://project_bucket/
└── amf/          # AMF PDFs/XMLs only
```

---

## Environment variable naming

Pattern: `{SERVICE}_{SOURCE}_{RESOURCE}`

| Variable | Example value |
|---|---|
| `GCP_PROJECT_ID` | `project-pea-pme` |
| `GCS_BUCKET_NAME` | `project_bucket` |
| `GCS_AMF_PREFIX` | `amf` |
| `BQ_AMF_DATASET` | `bronze` |
| `BQ_AMF_TABLE` | `amf` |
| `BQ_YAHOO_DATASET` | `bronze` |
| `BQ_YAHOO_TABLE` | `yahoo_rss` |

---

## Referentiel

Master referentiel: `boursorama/boursorama_peapme_final.csv` (569 companies)
Key columns: `name`, `ticker_bourso`, `isin`

Draft (5 companies for testing): `referentiel/companies_draft.csv`
