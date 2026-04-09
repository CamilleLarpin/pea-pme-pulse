import os
import sys
import tempfile
import json
import io
import re
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import pdfplumber
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from groq import Groq
from prefect import flow, task, get_run_logger

# --- ENVIRONMENT CONFIGURATION ---

# GCP Credentials management for Prefect Managed Runner
_gcp_creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if _gcp_creds_json and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
        _tmp.write(_gcp_creds_json)
        _tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _tmp.name

# Global Table Constants
GCP_PREFIX_INPUT_TABLE_ID = "amf"
GCP_PREFIX_OUTPUT_TABLE_ID = "amf_insider_signals"

# --- TASKS ---

@task(name="amf-load-raw-data", retries=2)
def task_load_from_bigquery(project_id: str, dataset_id: str, table_id: str):
    """
    Fetches records from BigQuery. Dynamically switches between 
    Bronze (new documents) and Silver (already processed) layers.

    Input:
        project_id, dataset_id, table_id (str): BigQuery location details.
    Output:
        list[dict]: Records retrieved from the specified table.
    """
    logger = get_run_logger()
    client = bigquery.Client(project=project_id)
    table_path = f"{project_id}.{dataset_id}.{table_id}"
    
    # Query for Bronze Layer (Targeting unprocessed filings)
    if dataset_id == os.getenv("BQ_DATASET_BRONZE"):
        query = f"""
            SELECT record_id, societe, isin, pdf_url, titre
            FROM `{table_path}`
            WHERE type_information = 'Informations réglementées continues'
            AND pdf_download_status = 'success'
            AND DATE(publication_ts) >= DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 90 DAY)
            AND (UPPER(titre) LIKE '%DIRIGEANT%' OR UPPER(titre) LIKE '%ARTICLE 19%')
            ORDER BY publication_ts DESC LIMIT 12
        """
    # Query for Silver Layer (Checking existing record IDs)
    else:
        query = f"SELECT DISTINCT source_record_id FROM `{table_path}`"

    try:
        query_job = client.query(query)
        return [dict(row) for row in query_job.result()]
    except Exception as e:
        logger.error(f"BigQuery error ({dataset_id}): {e}")
        return []

@task(name="amf-extract-pdf-text")
def task_extract_text(url: str):
    """
    Downloads the PDF document and extracts full text content.

    Input:
        url (str): Public URL of the PDF.
    Output:
        str: Extracted text or None if the download/parse fails.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            return "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()]).strip()
    except Exception as e:
        return None

@task(name="amf-ai-parse-signals", retries=1)
def task_parse_signals_with_ai(text: str, doc_info: dict, api_key: str):
    """
    Processes extracted text using Groq LLM (Llama 3.1) to find trading signals.

    Input:
        text (str): Raw document text.
        doc_info (dict): Source metadata (Company name, ID).
        api_key (str): Groq API key.
    Output:
        list[dict]: A list of potential insider transaction objects.
    """
    if not text: return []
    
    logger = get_run_logger()
    clean_text = re.sub(r'\s+', ' ', text)[:3000] # Text truncation for context window
    client_groq = Groq(api_key=api_key)
    
    prompt = f"""
    Extract ALL insider trading transactions for {doc_info['societe']}.
    The output must be a JSON object with a 'signals' key.
    Format: {{"signals": [{{"dirigeant": "Name", "type_operation": "Achat/Vente", "montant": 0.0, "date_signal": "YYYY-MM-DD"}}]}}
    Rules: 'montant' must be a pure float. If price/qty are present, perform the multiplication.
    """

    # Rate limiting protection for free-tier APIs
    time.sleep(2)

    completion = client_groq.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "You are a financial analyst. Output ONLY valid JSON."},
            {"role": "user", "content": prompt + "\n\nTEXT:\n" + clean_text}
        ],
        response_format={"type": "json_object"}
    )
    
    raw_output = json.loads(completion.choices[0].message.content)
    signals = raw_output.get("signals", []) if isinstance(raw_output, dict) else []
    return signals if isinstance(signals, list) else [signals]

@task(name="amf-validate-and-enrich")
def task_validate_signals(raw_signals: list, doc: dict):
    """
    Cleans extracted names, filters descriptive noise, and adds source traceability.

    Input:
        raw_signals (list): List of dictionaries from AI extraction.
        doc (dict): Original document metadata.
    Output:
        list[dict]: Enriched signals ready for Silver layer insertion.
    """
    logger = get_run_logger()
    validated = []
    
    # Blacklist to filter out generic descriptive titles mistaken for names
    forbidden_words = {"ancien", "dirigeant", "membre", "société", "groupe", "fondation"}

    for sig in raw_signals:
        name = str(sig.get("dirigeant", "")).strip()
        name_parts = name.lower().split()
        
        # Heuristic filters for valid names (word count and blacklist)
        if len(name_parts) < 2 or len(name_parts) > 4: continue
        if any(w in forbidden_words for w in name_parts): continue
        
        # Data enrichment from Bronze layer metadata
        sig.update({
            "societe": doc['societe'],
            "isin": doc['isin'],
            "source_record_id": doc['record_id'],
            "url_doc": doc['pdf_url'],
            "processed_at": datetime.now().isoformat(),
            "extraction_method": "groq-llama-3.1-8b",
            "ticker": sig.get("ticker") or doc['isin']
        })
        validated.append(sig)
        
    return validated

@task(name="amf-upload-silver", retries=2)
def task_upload_bq(rows: list, project_id: str, dataset_id: str):
    """
    Persists the final enriched signals into BigQuery Silver layer.

    Input:
        rows (list[dict]): The validated signal list.
        project_id, dataset_id (str): Destination BigQuery details.
    Output:
        None
    """
    if not rows: return
    
    logger = get_run_logger()
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(GCP_PREFIX_OUTPUT_TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result() # Wait for completion
    logger.info(f"Successfully uploaded {len(rows)} signals to {dataset_id}.{GCP_PREFIX_OUTPUT_TABLE_ID}")

# --- FLOW ---

@flow(name="silver-amf-insider-parser")
def amf_insider_parser_flow():
    """
    Main orchestration flow for AMF Insider Signal Parser.
    Pipeline: Bronze Load -> Duplicate Check -> PDF Parse -> AI Extraction -> Silver Upload.
    """
    logger = get_run_logger()

    # 1. Environment configuration
    config = {
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "db_bronze": os.getenv("BQ_DATASET_BRONZE"),
        "db_silver": os.getenv("BQ_DATASET_SILVER"),
        "groq_key": os.getenv("GROQ_API_KEY")
    }

    # 2. Data loading and reconciliation
    pending_docs = task_load_from_bigquery(config["project_id"], config["db_bronze"], GCP_PREFIX_INPUT_TABLE_ID)
    current_docs = task_load_from_bigquery(config["project_id"], config["db_silver"], GCP_PREFIX_OUTPUT_TABLE_ID)

    # 3. Duplicate filtering (O(1) lookup using set)
    already_done_ids = {doc['source_record_id'] for doc in current_docs if 'source_record_id' in doc}
    docs_to_process = [d for d in pending_docs if d.get('record_id') not in already_done_ids]

    logger.info(f"New documents identified for processing: {len(docs_to_process)}")

    # 4. Iterative processing of new documents
    final_payload = []
    for doc in docs_to_process:
        logger.info(f"Processing filing for company: {doc['societe']}")
        
        text = task_extract_text(doc['pdf_url'])
        if text:
            # AI logic triggered only if text extraction is successful
            raw_signals = task_parse_signals_with_ai(text, doc, config["groq_key"])
            valid_signals = task_validate_signals(raw_signals, doc)
            final_payload.extend(valid_signals)

    # 5. Final persistence layer
    if final_payload:
        task_upload_bq(final_payload, config["project_id"], config["db_silver"])
    else:
        logger.warning("No valid insider trading signals detected in this batch.")

if __name__ == "__main__":
    amf_insider_parser_flow()