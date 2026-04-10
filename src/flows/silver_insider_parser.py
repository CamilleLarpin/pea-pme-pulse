"""Prefect flow — Silver AMF Insider Parser pipeline.

Step 1 — load_pending_docs:
    Identifies records in bronze.amf that haven't been processed in silver.amf_insider_signals.

Step 2 — extract_pdf_text & parse_signals_with_ai:
    Downloads the AMF filings, extracts text via pdfplumber, and uses Groq LLM
    to structure insider transactions (Purchase/Sale) into JSON.

Step 3 — validate_signals & upload_to_bigquery:
    Filters the AI output for quality and appends the validated signals
    to the Silver BigQuery table.

Designed to process the latest insider filings identified in the Bronze layer.
"""

import io
import json
import os
import re
import time
from datetime import datetime
from typing import Any

import pdfplumber
import requests
from google.cloud import bigquery
from groq import Groq
from prefect import flow, get_run_logger, task

# Configurations from environment variables
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "bootcamp-project-pea-pme")
DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "bronze")
DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "silver")

TABLE_AMF_RAW = "amf"
TABLE_AMF_SIGNALS = "amf_insider_signals"


@task(name="amf-load-pending-docs", retries=2)
def load_pending_docs() -> list[dict[str, Any]]:
    """
    Identifies unprocessed documents via LEFT JOIN between Bronze and Silver.

    Returns:
        List[Dict[str, Any]]: List of document metadata to be processed.
    """
    logger = get_run_logger()
    client = bigquery.Client(project=GCP_PROJECT)

    # Efficient anti-join to skip already processed record_ids
    query = f"""
        SELECT b.record_id, b.societe, b.isin, b.pdf_url, b.titre
        FROM `{GCP_PROJECT}.{DATASET_BRONZE}.{TABLE_AMF_RAW}` b
        LEFT JOIN `{GCP_PROJECT}.{DATASET_SILVER}.{TABLE_AMF_SIGNALS}` s
          ON b.record_id = s.source_record_id
        WHERE b.type_information = 'Informations réglementées continues'
          AND b.pdf_download_status = 'success'
          AND DATE(b.publication_ts) >= DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 90 DAY)
          AND (UPPER(b.titre) LIKE '%DIRIGEANT%' OR UPPER(b.titre) LIKE '%ARTICLE 19%')
          AND s.source_record_id IS NULL
        ORDER BY b.publication_ts DESC 
        LIMIT 15
    """
    try:
        query_job = client.query(query)
        docs = [dict(row) for row in query_job.result()]
        logger.info(f"Identified {len(docs)} new documents for processing.")
        return docs
    except Exception as e:
        logger.error(f"Failed to fetch pending documents: {e}")
        return []


@task(name="amf-extract-pdf-text")
def extract_pdf_text(url: str) -> str | None:
    """
    Downloads PDF content and extracts raw text using pdfplumber.

    Args:
        url (str): The URL of the PDF document.

    Returns:
        Optional[str]: Extracted text or None if extraction fails.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            return "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()]).strip()
    except Exception as e:
        get_run_logger().warning(f"PDF extraction failed for {url}: {e}")
        return None


@task(name="amf-ai-parse-signals", retries=1)
def parse_signals_with_ai(text: str, doc_info: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extracts structured trading signals from raw text using Groq LLM.

    Args:
        text (str): Raw extracted PDF text.
        doc_info (Dict[str, Any]): Metadata about the document (company, ISIN).

    Returns:
        List[Dict[str, Any]]: List of extracted signals (insider, operation, amount).
    """
    if not text:
        return []

    api_key = os.getenv("GROQ_API_KEY")
    client_groq = Groq(api_key=api_key)

    # Truncate text to fit context window and remove extra whitespace
    clean_text = re.sub(r"\s+", " ", text)[:3500]

    prompt = f"""
    Extract ALL insider trading transactions for the company: {doc_info["societe"]}.
    Return a JSON object with a 'signals' key.
    Schema: {{"signals": [{{"dirigeant": "Name", "type_operation": "Achat/Vente", "montant": 0.0, "date_signal": "YYYY-MM-DD"}}]}}
    Rules: 'montant' must be a float. Output ONLY valid JSON.
    """

    # Rate limit protection for API
    time.sleep(1)

    completion = client_groq.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {
                "role": "system",
                "content": "You are a specialized financial analyst. Output ONLY valid JSON.",
            },
            {"role": "user", "content": prompt + "\n\nTEXT:\n" + clean_text},
        ],
        response_format={"type": "json_object"},
    )

    try:
        raw_output = json.loads(completion.choices[0].message.content)
        return raw_output.get("signals", [])
    except json.JSONDecodeError:
        return []


@task(name="amf-validate-and-enrich")
def validate_signals(
    raw_signals: list[dict[str, Any]], doc: dict[str, Any]
) -> list[dict[str, Any]]:
    """
    Validates LLM output via heuristics and enriches it with source metadata.

    Args:
        raw_signals (List[Dict[str, Any]]): Signals extracted by AI.
        doc (Dict[str, Any]): Original document metadata.

    Returns:
        List[Dict[str, Any]]: Validated and enriched signal records.
    """
    validated = []
    # Keywords often mistaken for person names by LLMs
    forbidden_words = {"ancien", "dirigeant", "membre", "société", "groupe", "fondation"}

    for sig in raw_signals:
        name = str(sig.get("dirigeant", "")).strip()
        name_parts = name.lower().split()

        # Basic heuristic validation
        if len(name_parts) < 2 or any(w in forbidden_words for w in name_parts):
            continue

        # Enrich with Bronze metadata for traceability
        sig.update(
            {
                "societe": doc["societe"],
                "isin": doc["isin"],
                "source_record_id": doc["record_id"],
                "url_doc": doc["pdf_url"],
                "processed_at": datetime.now().isoformat(),
                "extraction_method": "groq-llama-3.1-8b",
            }
        )
        validated.append(sig)

    return validated


@task(name="amf-upload-to-silver", retries=2)
def upload_to_bigquery(rows: list[dict[str, Any]]) -> None:
    """
    Appends the processed signals to the BigQuery Silver layer.

    Args:
        rows (List[Dict[str, Any]]): Final signals list to upload.
    """
    if not rows:
        return

    logger = get_run_logger()
    client = bigquery.Client(project=GCP_PROJECT)
    table_ref = f"{GCP_PROJECT}.{DATASET_SILVER}.{TABLE_AMF_SIGNALS}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()  # Wait for the BQ job to complete
    logger.info(f"Successfully uploaded {len(rows)} signals to {table_ref}")


@flow(name="silver-amf-insider-parser")
def amf_insider_parser_flow() -> None:
    """
    Orchestration flow for parsing AMF Insider Trading documents.
    Pipeline: Identify Pending Docs -> Extract PDF -> AI Extraction -> Validation -> BQ Sync.
    """
    logger = get_run_logger()
    logger.info("Starting silver-amf-insider-parser flow")

    # Identify documents that haven't been processed yet
    docs_to_process = load_pending_docs()

    if not docs_to_process:
        logger.info("No new documents found. Flow complete.")
        return

    # Process each document
    all_signals = []
    for doc in docs_to_process:
        logger.info(f"Processing filing for: {doc['societe']} (ID: {doc['record_id']})")

        text = extract_pdf_text(doc["pdf_url"])
        if text:
            raw_signals = parse_signals_with_ai(text, doc)
            valid_signals = validate_signals(raw_signals, doc)
            all_signals.extend(valid_signals)
        else:
            logger.warning(f"Skipping document {doc['record_id']} due to empty text extraction.")

    # Persistence: upload all validated signals to BigQuery Silver layer
    if all_signals:
        upload_to_bigquery(all_signals)
    else:
        logger.info("No valid signals extracted in this batch.")

    logger.info("Flow silver-amf-insider-parser completed successfully")


if __name__ == "__main__":
    amf_insider_parser_flow()
