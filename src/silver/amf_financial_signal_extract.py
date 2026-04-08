from __future__ import annotations

import json
import os
import re
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pdfplumber
import requests
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from loguru import logger

# ============================================================================
# Configuration
# ============================================================================

DATA_SOURCE = "amf"


@dataclass(frozen=True)
class FinancialSignalConfig:
    project_id: str
    location: str

    bronze_dataset_id: str
    bronze_table_id: str

    work_dataset_id: str
    work_table_id: str

    max_documents: int | None
    batch_size: int
    request_timeout: int
    llm_timeout: int

    groq_api_key: str
    groq_model: str
    groq_base_url: str
    llm_prompt_version: str
    llm_max_retries: int
    llm_sleep_seconds: float

    text_excerpt_max_chars: int
    financial_context_max_chars: int
    recent_days: int
    minimum_relevance_score: int

    @property
    def full_bronze_table_id(self) -> str:
        return f"{self.project_id}.{self.bronze_dataset_id}.{self.bronze_table_id}"

    @property
    def full_work_table_id(self) -> str:
        return f"{self.project_id}.{self.work_dataset_id}.{self.work_table_id}"


def load_config() -> FinancialSignalConfig:
    return FinancialSignalConfig(
        project_id=os.environ["GCP_PROJECT_ID"],
        location=os.environ.get("GCP_LOCATION", "EU"),
        bronze_dataset_id=os.environ.get("BQ_BRONZE_DATASET_ID", "bronze"),
        bronze_table_id=os.environ.get("BQ_BRONZE_AMF_TABLE_ID", "amf"),
        work_dataset_id=os.environ.get("BQ_WORK_DATASET_ID", "work"),
        work_table_id=os.environ.get(
            "BQ_WORK_AMF_FINANCIAL_SIGNAL_STAGING_TABLE_ID",
            "amf_financial_signal_staging",
        ),
        max_documents=(
            int(os.environ["FINANCIAL_SIGNAL_MAX_DOCUMENTS"])
            if "FINANCIAL_SIGNAL_MAX_DOCUMENTS" in os.environ
            else None
        ),
        batch_size=int(os.environ.get("FINANCIAL_SIGNAL_BATCH_SIZE", "10")),
        request_timeout=int(os.environ.get("FINANCIAL_SIGNAL_REQUEST_TIMEOUT", "120")),
        llm_timeout=int(os.environ.get("FINANCIAL_SIGNAL_LLM_TIMEOUT", "180")),
        groq_api_key=os.environ["GROQ_API_KEY"],
        groq_model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
        groq_base_url=os.environ.get(
            "GROQ_BASE_URL",
            "https://api.groq.com/openai/v1/chat/completions",
        ),
        llm_prompt_version=os.environ.get("LLM_PROMPT_VERSION", "v2"),
        llm_max_retries=int(os.environ.get("LLM_MAX_RETRIES", "2")),
        llm_sleep_seconds=float(os.environ.get("LLM_SLEEP_SECONDS", "0")),
        text_excerpt_max_chars=int(os.environ.get("TEXT_EXCERPT_MAX_CHARS", "10000")),
        financial_context_max_chars=int(os.environ.get("FINANCIAL_CONTEXT_MAX_CHARS", "2500")),
        recent_days=int(os.environ.get("FINANCIAL_SIGNAL_RECENT_DAYS", "90")),
        minimum_relevance_score=int(os.environ.get("FINANCIAL_SIGNAL_MIN_RELEVANCE_SCORE", "4")),
    )


# ============================================================================
# Schema
# ============================================================================

FINANCIAL_SIGNAL_STAGING_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("isin", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("source_url", "STRING"),
    bigquery.SchemaField("pdf_gcs_uri", "STRING"),
    bigquery.SchemaField("document_publication_ts", "TIMESTAMP"),
    bigquery.SchemaField("date_cloture_exercice_raw", "STRING"),
    bigquery.SchemaField("ca_raw", "STRING"),
    bigquery.SchemaField("ca_growth_raw", "STRING"),
    bigquery.SchemaField("ebitda_raw", "STRING"),
    bigquery.SchemaField("marge_op_raw", "STRING"),
    bigquery.SchemaField("dette_nette_raw", "STRING"),
    bigquery.SchemaField("fcf_raw", "STRING"),
    bigquery.SchemaField("document_text_excerpt", "STRING"),
    bigquery.SchemaField("text_length", "INT64"),
    bigquery.SchemaField("page_count", "INT64"),
    bigquery.SchemaField("parser_used", "STRING"),
    bigquery.SchemaField("llm_model", "STRING"),
    bigquery.SchemaField("llm_prompt_version", "STRING"),
    bigquery.SchemaField("extraction_status", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("raw_llm_response", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("source_run_id", "STRING"),
    bigquery.SchemaField("financial_signal_run_id", "STRING"),
    bigquery.SchemaField("extracted_at", "TIMESTAMP"),
]

# ============================================================================
# Utilities
# ============================================================================


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


# ============================================================================
# Financial text reduction
# ============================================================================

FINANCIAL_KEYWORDS = [
    # Revenue / sales
    "chiffre d'affaires",
    "chiffre d affaires",
    "ca ",
    "revenu",
    "revenus",
    "revenue",
    "revenues",
    "sales",
    "net sales",
    # Profitability
    "ebitda",
    "ebit",
    "résultat opérationnel",
    "resultat operationnel",
    "résultat d'exploitation",
    "resultat d'exploitation",
    "operating income",
    "operating profit",
    "operating margin",
    "marge opérationnelle",
    "marge operationnelle",
    "marge op",
    # Debt / cash
    "dette nette",
    "endettement net",
    "trésorerie nette",
    "tresorerie nette",
    "net debt",
    "gross debt",
    "cash position",
    "cash flow",
    "free cash flow",
    "fcf",
    "cash flow libre",
    "flux de trésorerie disponible",
    "flux de tresorerie disponible",
    "flux de trésorerie",
    "flux de tresorerie",
    # Growth / percentages
    "croissance",
    "growth",
    "increase",
    "decrease",
    "year-on-year",
    "yoy",
    "%",
    # Dates / reporting periods
    "clos le",
    "exercice clos",
    "premier semestre clos",
    "deuxième semestre clos",
    "au 30 juin",
    "au 31 décembre",
    "au 31 decembre",
    "for the year ended",
    "for the period ended",
    "as of",
    "ended",
    "financial year",
    "fiscal year",
    "half-year",
    "first half",
    "second half",
    # Units / currencies
    "m€",
    "md€",
    "k€",
    "keur",
    "meur",
    "million eur",
    "thousand eur",
    "billion eur",
    "millions d'euros",
    "millions d’euros",
    "millions of euros",
    "million euros",
    "eur",
    "€",
]

MONEY_PATTERN = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:k€|m€|md€|keur|meur|million|millions|thousand|billion|eur|€)\b",
    flags=re.IGNORECASE,
)


def score_financial_block(block: str) -> int:
    normalized = normalize_text_for_match(block)
    score = 0

    keyword_weights = {
        # Revenue
        "chiffre d'affaires": 3,
        "chiffre d affaires": 3,
        "revenu": 2,
        "revenues": 2,
        "revenue": 2,
        "sales": 2,
        "net sales": 2,
        # Profitability
        "ebitda": 4,
        "ebit": 2,
        "résultat opérationnel": 3,
        "resultat operationnel": 3,
        "operating income": 3,
        "operating profit": 3,
        "marge opérationnelle": 4,
        "marge operationnelle": 4,
        "marge op": 3,
        "operating margin": 4,
        # Debt / cash
        "dette nette": 4,
        "endettement net": 4,
        "net debt": 4,
        "gross debt": 2,
        "free cash flow": 4,
        "fcf": 3,
        "cash flow": 2,
        "cash flow libre": 4,
        "flux de trésorerie disponible": 4,
        "flux de tresorerie disponible": 4,
        "flux de trésorerie": 2,
        "flux de tresorerie": 2,
        # Dates
        "clos le": 3,
        "exercice clos": 3,
        "premier semestre clos": 3,
        "deuxième semestre clos": 3,
        "for the year ended": 3,
        "for the period ended": 3,
        "as of": 2,
        "ended": 1,
        "financial year": 2,
        "fiscal year": 2,
        "half-year": 2,
        "first half": 2,
        "second half": 2,
        # Growth / percentages
        "croissance": 2,
        "growth": 2,
        "year-on-year": 2,
        "yoy": 2,
        "%": 2,
    }

    for keyword, weight in keyword_weights.items():
        if keyword in normalized:
            score += weight

    if MONEY_PATTERN.search(normalized):
        score += 3

    if re.search(r"\b20\d{2}\b", normalized):
        score += 1

    return score


def normalize_text_for_match(text: str) -> str:
    return text.lower().replace("’", "'").replace("\xa0", " ")


def split_text_into_blocks(text: str) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if paragraphs:
        return paragraphs

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    blocks: list[str] = []
    current: list[str] = []

    for line in lines:
        current.append(line)
        if len(current) >= 8:
            blocks.append("\n".join(current))
            current = []

    if current:
        blocks.append("\n".join(current))

    return blocks


def is_financial_block(block: str) -> bool:
    normalized = normalize_text_for_match(block)
    return any(keyword in normalized for keyword in FINANCIAL_KEYWORDS)


def extract_financial_context(
    *,
    document_text: str,
    max_chars: int,
    max_blocks: int = 15,
) -> str:
    blocks = split_text_into_blocks(document_text)

    scored_blocks: list[tuple[int, str]] = []
    for block in blocks:
        score = score_financial_block(block)
        if score > 0:
            scored_blocks.append((score, block))

    if not scored_blocks:
        return document_text[:max_chars].strip()

    scored_blocks.sort(key=lambda x: x[0], reverse=True)

    selected_blocks: list[str] = []
    total_length = 0

    for _, block in scored_blocks[:max_blocks]:
        additional_len = len(block) + (len("\n\n---\n\n") if selected_blocks else 0)

        if total_length + additional_len > max_chars:
            remaining = max_chars - total_length
            if remaining > 100 and not selected_blocks:
                selected_blocks.append(block[:remaining].strip())
            break

        selected_blocks.append(block)
        total_length += additional_len

    context = "\n\n---\n\n".join(selected_blocks)
    return context[:max_chars].strip()


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class BronzeDocument:
    record_id: str
    isin: str | None
    ticker: str | None
    pdf_gcs_uri: str
    source_url: str | None
    document_publication_ts: str | None
    titre: str | None
    sous_type: str | None
    type_information: str | None
    source: str | None
    source_run_id: str | None


@dataclass(frozen=True)
class FinancialSignalRow:
    record_id: str
    isin: str | None
    ticker: str | None
    source_url: str | None
    pdf_gcs_uri: str
    document_publication_ts: str | None

    date_cloture_exercice_raw: str | None
    ca_raw: str | None
    ca_growth_raw: str | None
    ebitda_raw: str | None
    marge_op_raw: str | None
    dette_nette_raw: str | None
    fcf_raw: str | None

    document_text_excerpt: str | None
    text_length: int | None
    page_count: int | None

    parser_used: str
    llm_model: str
    llm_prompt_version: str

    extraction_status: str
    error_message: str | None
    raw_llm_response: str | None

    source: str | None
    source_run_id: str | None
    financial_signal_run_id: str
    extracted_at: str


# ============================================================================
# BigQuery helpers
# ============================================================================


def ensure_dataset_exists(
    *,
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location

    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset OK: {}.{}", project_id, dataset_id)
    except NotFound:
        client.create_dataset(dataset_ref)
        logger.info("Dataset created: {}.{}", project_id, dataset_id)


def ensure_table_exists(
    *,
    client: bigquery.Client,
    full_table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    try:
        client.get_table(full_table_id)
        logger.info("Table OK: {}", full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="extracted_at",
        )
        table.clustering_fields = ["isin", "record_id", "extraction_status"]
        client.create_table(table)
        logger.info("Table created: {}", full_table_id)


def fetch_bronze_documents(
    *,
    client: bigquery.Client,
    config: FinancialSignalConfig,
) -> list[BronzeDocument]:
    # Validate interpolated values to prevent injection
    recent_days = int(config.recent_days)
    minimum_relevance_score = int(config.minimum_relevance_score)
    if recent_days <= 0 or minimum_relevance_score < 0:
        raise ValueError(
            f"Invalid config: recent_days={recent_days}, minimum_relevance_score={minimum_relevance_score}"
        )

    limit_clause = (
        f"\nLIMIT {int(config.max_documents)}" if config.max_documents is not None else ""
    )

    query = f"""
    WITH latest_run AS (
      SELECT MAX(run_id) AS run_id
      FROM `{config.full_bronze_table_id}`
    ),

    base AS (
      SELECT
        record_id,
        isin,
        ticker,
        pdf_gcs_uri,
        pdf_url AS source_url,
        publication_ts AS document_publication_ts,
        titre,
        sous_type,
        type_information,
        source,
        run_id AS source_run_id
      FROM `{config.full_bronze_table_id}`
      WHERE run_id = (SELECT run_id FROM latest_run)
        AND pdf_download_status = 'success'
        AND pdf_gcs_uri IS NOT NULL
        AND publication_ts >= TIMESTAMP_SUB(
          CURRENT_TIMESTAMP(),
          INTERVAL {recent_days} DAY
        )
    ),

    scored AS (
      SELECT
        *,
        (
          CASE
            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'résultat annuel|resultat annuel|annual results|full[- ]year results'
            ) THEN 5

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'rapport financier annuel|annual financial report'
            ) THEN 5

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'résultat semestriel|resultat semestriel|half[- ]year results|half year results|interim results'
            ) THEN 4

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'rapport financier semestriel|half[- ]year financial report|interim financial report'
            ) THEN 4

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r"chiffre d'affaires|chiffre d affaires|revenue|revenues|sales|net sales"
            ) THEN 4

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'résultat|resultat|results'
            ) THEN 3

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(titre, '')),
              r'rapport financier|financial report'
            ) THEN 3

            WHEN REGEXP_CONTAINS(
            LOWER(COALESCE(titre, '')),
            r'mise à disposition|mise a disposition|publication du rapport|disponibilité du rapport|availability of'
            ) THEN -4

            ELSE 0
          END
          +
          CASE
            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(sous_type, '')),
              r'annuel|annual|full[- ]year|fiscal year'
            ) THEN 2

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(sous_type, '')),
              r'semestriel|half[- ]year|half year|interim'
            ) THEN 1

            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(sous_type, '')),
              r'résultat|resultat|results|revenue|sales'
            ) THEN 1

            ELSE 0
          END
          +
          CASE
            WHEN REGEXP_CONTAINS(
              LOWER(COALESCE(type_information, '')),
              r'information réglementée|information reglementee|regulated information'
            ) THEN 1
            ELSE 0
          END
        ) AS relevance_score
      FROM base
      WHERE (
          CASE
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'résultat annuel|resultat annuel|annual results|full[- ]year results') THEN 5
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'rapport financier annuel|annual financial report') THEN 5
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'résultat semestriel|resultat semestriel|half[- ]year results|half year results|interim results') THEN 4
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'rapport financier semestriel|half[- ]year financial report|interim financial report') THEN 4
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r"chiffre d'affaires|chiffre d affaires|revenue|revenues|sales|net sales") THEN 4
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'résultat|resultat|results') THEN 3
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(titre, '')), r'rapport financier|financial report') THEN 3
            ELSE 0
          END
          +
          CASE
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(sous_type, '')), r'annuel|annual|full[- ]year|fiscal year') THEN 2
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(sous_type, '')), r'semestriel|half[- ]year|half year|interim') THEN 1
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(sous_type, '')), r'résultat|resultat|results|revenue|sales') THEN 1
            ELSE 0
          END
          +
          CASE
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(type_information, '')), r'information réglementée|information reglementee|regulated information') THEN 1
            ELSE 0
          END
        ) >= {minimum_relevance_score}
    )

    SELECT
      scored.record_id,
      scored.isin,
      scored.ticker,
      scored.pdf_gcs_uri,
      scored.source_url,
      scored.document_publication_ts,
      scored.titre,
      scored.sous_type,
      scored.type_information,
      scored.source,
      scored.source_run_id
    FROM scored
    LEFT JOIN `{config.full_work_table_id}` work
      ON scored.record_id = work.record_id
      AND work.extraction_status IN ('success', 'no_financial_data')
    WHERE work.record_id IS NULL
    ORDER BY relevance_score DESC, document_publication_ts DESC{limit_clause}
    """

    rows = client.query(query).result()

    documents = []
    for row in rows:
        try:
            pub_ts = (
                isoformat_utc(row["document_publication_ts"])
                if row["document_publication_ts"] is not None
                else None
            )
        except Exception:
            logger.warning("Could not parse publication_ts for record_id={}", row["record_id"])
            pub_ts = None

        documents.append(
            BronzeDocument(
                record_id=row["record_id"],
                isin=row["isin"],
                ticker=row["ticker"],
                pdf_gcs_uri=row["pdf_gcs_uri"],
                source_url=row["source_url"],
                document_publication_ts=pub_ts,
                titre=row["titre"],
                sous_type=row["sous_type"],
                type_information=row["type_information"],
                source=row["source"],
                source_run_id=row["source_run_id"],
            )
        )

    logger.info(
        "Fetched {} bronze document(s) from latest run, last {} days, relevance_score>={}",
        len(documents),
        recent_days,
        minimum_relevance_score,
    )
    return documents


def append_rows_to_bq(
    *,
    client: bigquery.Client,
    full_table_id: str,
    schema: list[bigquery.SchemaField],
    rows: list[FinancialSignalRow],
) -> None:
    if not rows:
        return

    with tempfile.TemporaryDirectory(prefix="financial_signal_") as tmp_dir:
        jsonl_path = Path(tmp_dir) / "financial_signal_rows.jsonl"

        with open(jsonl_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )

        with open(jsonl_path, "rb") as f:
            job = client.load_table_from_file(
                f,
                full_table_id,
                job_config=job_config,
            )
            job.result()

    logger.info("{} row(s) appended to {}", len(rows), full_table_id)


# ============================================================================
# GCS / PDF parsing
# ============================================================================


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    path = gcs_uri.removeprefix("gs://")
    bucket_name, blob_name = path.split("/", 1)
    return bucket_name, blob_name


def download_pdf_bytes_from_gcs(
    *,
    storage_client: storage.Client,
    gcs_uri: str,
) -> bytes:
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()


def extract_text_with_pdfplumber(pdf_bytes: bytes) -> tuple[str, int]:
    all_pages: list[str] = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        page_count = len(pdf.pages)

        for page in pdf.pages:
            text = page.extract_text() or ""
            all_pages.append(text)

    full_text = "\n".join(all_pages).strip()
    return full_text, page_count


# ============================================================================
# LLM extraction
# ============================================================================


def build_llm_messages(document_text: str) -> list[dict[str, str]]:
    system_prompt = """Extract financial signals from a financial document (French or English) and return ONLY valid JSON with exactly these keys (null if missing/uncertain):

    date_cloture_exercice_raw, ca_raw, ca_growth_raw, ebitda_raw, marge_op_raw, dette_nette_raw, fcf_raw

    DEFINITIONS:
    - ca_raw: revenue/chiffre d'affaires/sales
    - ca_growth_raw: revenue growth %
    - ebitda_raw: EBITDA (not EBIT unless labeled EBITDA)
    - marge_op_raw: operating margin/marge opérationnelle
    - dette_nette_raw: net debt/dette nette
    - fcf_raw: free cash flow

    UNIT INFERENCE RULE:
    - When values come from a table, always look for the unit header of that table
    (e.g. "in K€", "in thousands of euros", "€ million", "in €k").
    - Apply that unit to all values extracted from that table.
    - Never return a raw number without a unit — if the unit is ambiguous, return null.
    
    FORMAT RULES:
    - Monetary: "<number> EUR|thousand EUR|million EUR|billion EUR" — dot as decimal, no commas, no €, no spaces in number
    - Percentage: "<number>%" — dot as decimal (e.g. "7.2%", "-3.5%")
    - Date: ISO "YYYY-MM-DD" — look for "clos le / au / exercice clos / year ended / as of / period ended". Null if day unknown.
    - If unit is missing or ambiguous → null

    SELECTION: Use most recent reporting period. Prefer annual/semi-annual consolidated values. Check summary tables, highlights, and bullet lists before returning null. No markdown, no explanation, no extra keys.
    """.strip()

    user_prompt = f"""
Extract the requested financial signals from the following financial document text.

Document text:
\"\"\"
{document_text}
\"\"\"

Return only the JSON object.
""".strip()

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def call_groq_llama(
    *,
    config: FinancialSignalConfig,
    document_text: str,
) -> str:
    headers = {
        "Authorization": f"Bearer {config.groq_api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": config.groq_model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": build_llm_messages(document_text),
    }

    last_exception: Exception | None = None

    for attempt in range(1, config.llm_max_retries + 1):
        if config.llm_sleep_seconds > 0:
            logger.debug(
                "Sleeping {}s before Groq call | attempt={}/{}",
                config.llm_sleep_seconds,
                attempt,
                config.llm_max_retries,
            )
            time.sleep(config.llm_sleep_seconds)

        try:
            response = requests.post(
                config.groq_base_url,
                headers=headers,
                json=payload,
                timeout=config.llm_timeout,
            )

            logger.info(
                (
                    "Groq rate headers | status_code={} | remaining_tokens={} | "
                    "reset_tokens={} | remaining_requests={} | reset_requests={}"
                ),
                response.status_code,
                response.headers.get("x-ratelimit-remaining-tokens"),
                response.headers.get("x-ratelimit-reset-tokens"),
                response.headers.get("x-ratelimit-remaining-requests"),
                response.headers.get("x-ratelimit-reset-requests"),
            )

            response.raise_for_status()

            data = response.json()
            return data["choices"][0]["message"]["content"]

        except requests.HTTPError as exc:
            last_exception = exc
            response = exc.response
            status_code = response.status_code if response is not None else None

            retry_after_raw = response.headers.get("retry-after") if response is not None else None
            remaining_tokens = (
                response.headers.get("x-ratelimit-remaining-tokens")
                if response is not None
                else None
            )
            reset_tokens = (
                response.headers.get("x-ratelimit-reset-tokens") if response is not None else None
            )
            remaining_requests = (
                response.headers.get("x-ratelimit-remaining-requests")
                if response is not None
                else None
            )
            reset_requests = (
                response.headers.get("x-ratelimit-reset-requests") if response is not None else None
            )

            if status_code == 429:
                if retry_after_raw is not None:
                    try:
                        backoff_seconds = float(retry_after_raw)
                    except ValueError:
                        backoff_seconds = 30.0
                else:
                    backoff_seconds = 30.0 * attempt

                logger.warning(
                    (
                        "Groq 429 rate limited | attempt={}/{} | retry_after={}s | "
                        "remaining_tokens={} | reset_tokens={} | "
                        "remaining_requests={} | reset_requests={}"
                    ),
                    attempt,
                    config.llm_max_retries,
                    backoff_seconds,
                    remaining_tokens,
                    reset_tokens,
                    remaining_requests,
                    reset_requests,
                )

                if attempt < config.llm_max_retries:
                    time.sleep(backoff_seconds)
                    continue

            logger.warning(
                (
                    "Groq HTTP error | status_code={} | attempt={}/{} | "
                    "remaining_tokens={} | reset_tokens={} | "
                    "remaining_requests={} | reset_requests={} | error={}"
                ),
                status_code,
                attempt,
                config.llm_max_retries,
                remaining_tokens,
                reset_tokens,
                remaining_requests,
                reset_requests,
                exc,
            )
            raise

        except requests.RequestException as exc:
            last_exception = exc

            if attempt < config.llm_max_retries:
                backoff_seconds = 5.0 * attempt
                logger.warning(
                    "Groq request error | attempt={}/{} | retry_in={}s | error={}",
                    attempt,
                    config.llm_max_retries,
                    backoff_seconds,
                    exc,
                )
                time.sleep(backoff_seconds)
                continue

            raise

        except Exception as exc:
            last_exception = exc
            logger.exception(
                "Unexpected Groq call failure | attempt={}/{}",
                attempt,
                config.llm_max_retries,
            )
            raise

    if last_exception is not None:
        raise last_exception

    raise RuntimeError("Groq call failed without explicit exception.")


def parse_llm_json(raw_llm_response: str) -> dict[str, Any]:
    return json.loads(raw_llm_response)


# ============================================================================
# Processing
# ============================================================================


def process_document(
    *,
    storage_client: storage.Client,
    config: FinancialSignalConfig,
    document: BronzeDocument,
    financial_signal_run_id: str,
) -> FinancialSignalRow:
    extracted_at = isoformat_utc(utc_now())
    ticker = document.ticker

    try:
        pdf_bytes = download_pdf_bytes_from_gcs(
            storage_client=storage_client,
            gcs_uri=document.pdf_gcs_uri,
        )

        document_text, page_count = extract_text_with_pdfplumber(pdf_bytes)
        text_length = len(document_text)

        if not document_text.strip():
            return FinancialSignalRow(
                record_id=document.record_id,
                isin=document.isin,
                ticker=ticker,
                source_url=document.source_url,
                pdf_gcs_uri=document.pdf_gcs_uri,
                document_publication_ts=document.document_publication_ts,
                date_cloture_exercice_raw=None,
                ca_raw=None,
                ca_growth_raw=None,
                ebitda_raw=None,
                marge_op_raw=None,
                dette_nette_raw=None,
                fcf_raw=None,
                document_text_excerpt=None,
                text_length=text_length,
                page_count=page_count,
                parser_used="pdfplumber",
                llm_model=config.groq_model,
                llm_prompt_version=config.llm_prompt_version,
                extraction_status="empty_text",
                error_message="Extracted PDF text is empty.",
                raw_llm_response=None,
                source=document.source,
                source_run_id=document.source_run_id,
                financial_signal_run_id=financial_signal_run_id,
                extracted_at=extracted_at,
            )

        financial_context = extract_financial_context(
            document_text=document_text,
            max_chars=config.financial_context_max_chars,
        )

        logger.info(
            "Financial context prepared | record_id={} | text_length={} | context_length={}",
            document.record_id,
            len(document_text),
            len(financial_context),
        )

        debug_path = Path(f"/tmp/{document.record_id}_financial_context.txt")
        debug_path.write_text(financial_context, encoding="utf-8")

        test_mode = False

        if test_mode:
            llm_input_text = "Le chiffre d'affaires du groupe s'établit à 10 millions d'euros..."
        else:
            llm_input_text = financial_context

        raw_llm_response = call_groq_llama(
            config=config,
            document_text=llm_input_text,
        )
        llm_data = parse_llm_json(raw_llm_response)

        # Filter out documents with no financial data
        FINANCIAL_FIELDS = ["ca_raw", "ebitda_raw", "marge_op_raw", "dette_nette_raw", "fcf_raw"]
        if all(llm_data.get(f) is None for f in FINANCIAL_FIELDS):
            return FinancialSignalRow(
                record_id=document.record_id,
                isin=document.isin,
                ticker=ticker,
                source_url=document.source_url,
                pdf_gcs_uri=document.pdf_gcs_uri,
                document_publication_ts=document.document_publication_ts,
                date_cloture_exercice_raw=None,
                ca_raw=None,
                ca_growth_raw=None,
                ebitda_raw=None,
                marge_op_raw=None,
                dette_nette_raw=None,
                fcf_raw=None,
                document_text_excerpt=financial_context[: config.text_excerpt_max_chars],
                text_length=text_length,
                page_count=page_count,
                parser_used="pdfplumber",
                llm_model=config.groq_model,
                llm_prompt_version=config.llm_prompt_version,
                extraction_status="no_financial_data",
                error_message="All financial fields are null.",
                raw_llm_response=raw_llm_response,
                source=document.source,
                source_run_id=document.source_run_id,
                financial_signal_run_id=financial_signal_run_id,
                extracted_at=extracted_at,
            )

        return FinancialSignalRow(
            record_id=document.record_id,
            isin=document.isin,
            ticker=ticker,
            source_url=document.source_url,
            pdf_gcs_uri=document.pdf_gcs_uri,
            document_publication_ts=document.document_publication_ts,
            date_cloture_exercice_raw=llm_data.get("date_cloture_exercice_raw"),
            ca_raw=llm_data.get("ca_raw"),
            ca_growth_raw=llm_data.get("ca_growth_raw"),
            ebitda_raw=llm_data.get("ebitda_raw"),
            marge_op_raw=llm_data.get("marge_op_raw"),
            dette_nette_raw=llm_data.get("dette_nette_raw"),
            fcf_raw=llm_data.get("fcf_raw"),
            document_text_excerpt=financial_context[: config.text_excerpt_max_chars],
            text_length=text_length,
            page_count=page_count,
            parser_used="pdfplumber",
            llm_model=config.groq_model,
            llm_prompt_version=config.llm_prompt_version,
            extraction_status="success",
            error_message=None,
            raw_llm_response=raw_llm_response,
            source=document.source,
            source_run_id=document.source_run_id,
            financial_signal_run_id=financial_signal_run_id,
            extracted_at=extracted_at,
        )

    except requests.HTTPError as exc:
        response = exc.response
        status_code = response.status_code if response is not None else None

        extraction_status = "llm_error"
        if status_code == 429:
            extraction_status = "llm_rate_limited"

        return FinancialSignalRow(
            record_id=document.record_id,
            isin=document.isin,
            ticker=ticker,
            source_url=document.source_url,
            pdf_gcs_uri=document.pdf_gcs_uri,
            document_publication_ts=document.document_publication_ts,
            date_cloture_exercice_raw=None,
            ca_raw=None,
            ca_growth_raw=None,
            ebitda_raw=None,
            marge_op_raw=None,
            dette_nette_raw=None,
            fcf_raw=None,
            document_text_excerpt=None,
            text_length=text_length if "text_length" in locals() else None,
            page_count=page_count if "page_count" in locals() else None,
            parser_used="pdfplumber",
            llm_model=config.groq_model,
            llm_prompt_version=config.llm_prompt_version,
            extraction_status=extraction_status,
            error_message=str(exc),
            raw_llm_response=None,
            source=document.source,
            source_run_id=document.source_run_id,
            financial_signal_run_id=financial_signal_run_id,
            extracted_at=extracted_at,
        )

    except json.JSONDecodeError as exc:
        return FinancialSignalRow(
            record_id=document.record_id,
            isin=document.isin,
            ticker=ticker,
            source_url=document.source_url,
            pdf_gcs_uri=document.pdf_gcs_uri,
            document_publication_ts=document.document_publication_ts,
            date_cloture_exercice_raw=None,
            ca_raw=None,
            ca_growth_raw=None,
            ebitda_raw=None,
            marge_op_raw=None,
            dette_nette_raw=None,
            fcf_raw=None,
            document_text_excerpt=None,
            text_length=text_length if "text_length" in locals() else None,
            page_count=page_count if "page_count" in locals() else None,
            parser_used="pdfplumber",
            llm_model=config.groq_model,
            llm_prompt_version=config.llm_prompt_version,
            extraction_status="llm_invalid_json",
            error_message=str(exc),
            raw_llm_response=None,
            source=document.source,
            source_run_id=document.source_run_id,
            financial_signal_run_id=financial_signal_run_id,
            extracted_at=extracted_at,
        )

    except Exception as exc:
        return FinancialSignalRow(
            record_id=document.record_id,
            isin=document.isin,
            ticker=ticker,
            source_url=document.source_url,
            pdf_gcs_uri=document.pdf_gcs_uri,
            document_publication_ts=document.document_publication_ts,
            date_cloture_exercice_raw=None,
            ca_raw=None,
            ca_growth_raw=None,
            ebitda_raw=None,
            marge_op_raw=None,
            dette_nette_raw=None,
            fcf_raw=None,
            document_text_excerpt=None,
            text_length=text_length if "text_length" in locals() else None,
            page_count=page_count if "page_count" in locals() else None,
            parser_used="pdfplumber",
            llm_model=config.groq_model,
            llm_prompt_version=config.llm_prompt_version,
            extraction_status="pdf_parse_or_process_error",
            error_message=str(exc),
            raw_llm_response=None,
            source=document.source,
            source_run_id=document.source_run_id,
            financial_signal_run_id=financial_signal_run_id,
            extracted_at=extracted_at,
        )


# ============================================================================
# Main
# ============================================================================


def run_financial_signal_extract() -> None:
    config = load_config()
    financial_signal_run_id = utc_now().strftime("%Y-%m-%dT%H-%M-%SZ")

    logger.info(
        "Starting AMF financial signal extraction | run_id={} | limit={} | batch_size={}",
        financial_signal_run_id,
        config.max_documents if config.max_documents is not None else "FULL",
        config.batch_size,
    )

    bq_client = bigquery.Client(project=config.project_id)
    storage_client = storage.Client(project=config.project_id)

    ensure_dataset_exists(
        client=bq_client,
        project_id=config.project_id,
        dataset_id=config.work_dataset_id,
        location=config.location,
    )
    ensure_table_exists(
        client=bq_client,
        full_table_id=config.full_work_table_id,
        schema=FINANCIAL_SIGNAL_STAGING_SCHEMA,
    )

    documents = fetch_bronze_documents(
        client=bq_client,
        config=config,
    )

    if not documents:
        logger.info("No bronze documents to process.")
        return

    total_success = 0
    total_errors = 0

    for batch_index, batch in enumerate(chunked(documents, config.batch_size), start=1):
        logger.info(
            "Processing batch {} | batch_size={} | processed_so_far={}/{}",
            batch_index,
            len(batch),
            (batch_index - 1) * config.batch_size,
            len(documents),
        )

        rows = [
            process_document(
                storage_client=storage_client,
                config=config,
                document=document,
                financial_signal_run_id=financial_signal_run_id,
            )
            for document in batch
        ]

        append_rows_to_bq(
            client=bq_client,
            full_table_id=config.full_work_table_id,
            schema=FINANCIAL_SIGNAL_STAGING_SCHEMA,
            rows=rows,
        )

        batch_success = sum(row.extraction_status == "success" for row in rows)
        batch_errors = len(rows) - batch_success

        total_success += batch_success
        total_errors += batch_errors

        logger.info(
            "Batch {} done | success={} | errors={} | total_success={} | total_errors={}",
            batch_index,
            batch_success,
            batch_errors,
            total_success,
            total_errors,
        )

    logger.info(
        "AMF financial signal extraction finished | run_id={} | success={} | errors={}",
        financial_signal_run_id,
        total_success,
        total_errors,
    )


if __name__ == "__main__":
    run_financial_signal_extract()
