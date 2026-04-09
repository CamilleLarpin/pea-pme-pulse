import io
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pdfplumber
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from groq import Groq
from loguru import logger

# Configuration for BigQuery and Groq
GCP_PREFIX_INPUT_TABLE_ID = "amf"
GCP_PREFIX_OUTPUT_TABLE_ID = "amf_insider_signals"


def load_and_log_environment():
    """
    Loads environment variables, logs their status, and returns them as a dictionary.

    Input:
        - None (Reads directly from the OS Environment Variables).

    Output:
        - config (dict): A dictionary mapping environment variable names to their values.
    """

    # Get current file's parent directory (2 levels up) to ensure we are in the project root
    base_dir = Path(__file__).resolve().parents[2]

    gcp_credential_json_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not gcp_credential_json_file:
        logger.critical("GOOGLE_APPLICATION_CREDENTIALS not defined")
        sys.exit(1)

    # Initialize the configuration dictionary by fetching values from the OS
    config = {
        "BASE_DIR": base_dir,
        "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "BQ_DATASET_BRONZE": os.getenv("BQ_DATASET_BRONZE"),
        "BQ_DATASET_SILVER": os.getenv("BQ_DATASET_SILVER"),
        "GROQ_API_KEY": os.getenv("GROQ_API_KEY"),
        "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY"),
    }

    logger.info("--- Environment Configuration Check ---")

    # Iterate through the dictionary to validate and log each setting
    for var, value in config.items():
        if value:
            # Log the successfully loaded variable with alignment
            logger.success(f"{var: <30} : {value}")
        else:
            # Critical warning if a variable is missing (value is None)
            logger.warning(f"{var: <30} : NOT DEFINED")

    logger.info("---------------------------------------")

    # Return the dictionary to the caller for further use in the pipeline
    return config


def load_from_bigquery(project_id, dataset_id, table_id):
    """
    Load documents for processing from the input layer (Bronze/Silver).
    Uses the global constant GCP_PREFIX_INPUT_TABLE_ID for the table name.
    Filters for successful PDF downloads and insider-related titles.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The dataset name (e.g., 'bronze' or 'silver').
        table_id (str): The destination table name.

    Returns:
        list[dict]: A list of dictionaries containing record details,
                   or an empty list if no records are found or an error occurs.
    """
    client = bigquery.Client(project=project_id)
    table_path = f"{project_id}.{dataset_id}.{table_id}"

    logger.info(f"📊 Fetching documents from: {table_path}")

    # SQL Query: Filters for confirmed 'success' downloads and valid insider signals
    if dataset_id == "bronze":
        logger.info(
            "Applying Bronze layer filters: Only 'success' downloads with insider-related titles from the last 90 days."
        )
        query = f"""
            SELECT 
                record_id, 
                societe, 
                isin, 
                pdf_url, 
                titre
            FROM `{table_path}`
            WHERE type_information = 'Informations réglementées continues'
            AND pdf_download_status = 'success'
            -- Filter last 90 days to focus on recent filings (adjust as needed for testing)
            AND DATE(publication_ts) >= DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 90 DAY)
            AND (
                UPPER(titre) LIKE '%DIRIGEANT%' 
                OR UPPER(titre) LIKE '%PERSONNE EXERÇANT%'
                OR UPPER(titre) LIKE '%ARTICLE 19%'
            )
            -- Exclusion filters for corporate noise
            AND UPPER(titre) NOT LIKE '%ACTIONS PROPRES%'
            AND UPPER(titre) NOT LIKE '%RACHAT%'
            AND UPPER(titre) NOT LIKE '%DROITS DE VOTE%'
            ORDER BY publication_ts DESC
            LIMIT 12
            """
    elif dataset_id == "silver":
        logger.info(
            "Applying Silver layer filters: Only 'success' downloads with insider-related titles from the last 90 days, excluding already processed records."
        )
        query = f"SELECT DISTINCT source_record_id FROM `{table_path}`"

    try:
        query_job = client.query(query)
        # Convert BigQuery RowIterator directly into a list of dictionaries
        pdf_list = [dict(row) for row in query_job.result()]
        if len(pdf_list) > 0:
            logger.success(
                f"✅ Successfully retrieved {len(pdf_list)} valid records for PDF analysis."
            )
            return pdf_list
        else:
            logger.warning("⚠️ No valid records found for PDF analysis.")
            return []

    except Exception as e:
        logger.warning(f"❌ Error during BigQuery data extraction: {e}")
        return []


def extract_text_from_pdf(url):
    """
    Downloads the PDF and returns the contained text.

    Args:
        url (str): The public URL of the PDF document to be processed.

    Returns:
        str: The full text extracted from the PDF, or None if the download
             or parsing fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Ensure the download was successful

        # Use BytesIO to read the PDF directly from memory
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    full_text += page_text + "\n"

            return full_text.strip()

    except Exception as e:
        print(f"❌ Error during download/parsing of PDF {url}: {e}")
        return None


def parse_insider_data(text, doc_info, env):
    """
    Extracts ALL insider trading signals from a document.
    Returns a LIST of JSON strings, one for each transaction found.

    Args:
        text (str): The raw text extracted from the PDF document.
        doc_info (dict): Dictionary containing metadata (societe, record_id, pdf_url).
        env (dict): Environment variables, including API keys for Groq.
    Returns:
        list[str]: A list of JSON-formatted strings, each representing a transaction.
                   Returns None if no text is provided or an error occurs.
    """
    if not text:
        return None

    # 1. TEXT TRUNCATION
    # 3000 chars is sufficient for the main tables in Novacyt/Ubisoft docs
    clean_text = re.sub(r"\s+", " ", text)[:3000]

    # 2. ENHANCED MULTI-SIGNAL PROMPT
    # Specifically asking for a list to handle multiple directors in one PDF
    context_prompt = f"""
    Extract ALL insider trading transactions for the company {doc_info["societe"]} from the text.
    The document may contain multiple transactions for different directors.

    RETURN A JSON OBJECT WITH A KEY "signals" CONTAINING A LIST OF TRANSACTIONS.

    STRICT DATA TYPE AND CALCULATION RULES:
    - 'montant': MUST be a PURE FLOAT. 
    * IMPORTANT: If price and quantity are given, you MUST perform the calculation yourself (price * quantity).
    * DO NOT include formulas like "100 * 1.5" or asterisks (*) in the output.
    * DO NOT include currency symbols or commas. Use a dot (.) as decimal separator.
    * Example: if Price is 0.5 and Quantity is 1000, 'montant' must be 500.0.
    - 'date_signal': Use the specific transaction date (YYYY-MM-DD). If only year/month is available, use YYYY-MM-01.
    - 'dirigeant': Full name of the individual.
    - 'type_operation': Usually 'Achat' (Purchase), 'Vente' (Sale), or 'Nomination'.

    JSON STRUCTURE REQUIREMENT:
    Output only valid JSON. No explanations, no markdown notes, no raw math.
    Example structure: {{"signals": [{{"dirigeant": "John Doe", "type_operation": "Achat", "montant": 1250.50, "date_signal": "2026-03-25"}}]}}
    """

    # 3. PREVENTIVE DELAY (Rate Limit Protection)
    time.sleep(12)

    try:
        # Groq client initialization using environment API key
        client_groq = Groq(api_key=env["GROQ_API_KEY"])

        completion = client_groq.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {
                    "role": "system",
                    "content": "You are a financial analyst. You output ONLY valid JSON. If multiple directors are present, extract each one.",
                },
                {
                    "role": "user",
                    "content": context_prompt + "\n\nTEXT:\n" + clean_text,
                },
            ],
            response_format={"type": "json_object"},
        )

        raw_output = json.loads(completion.choices[0].message.content)

        # 4. ROBUST MULTI-SIGNAL HANDLING
        # Extract signals regardless of JSON structure variations (list vs object)
        raw_signals = []
        if "signals" in raw_output:
            raw_signals = raw_output["signals"]
        elif isinstance(raw_output, list):
            raw_signals = raw_output
        else:
            raw_signals = [raw_output]  # Single object fallback

        processed_signals = []

        for data in raw_signals:
            if not isinstance(data, dict):
                continue

            # 5. FINAL SCHEMA ENFORCEMENT & CALCULATIONS
            final_signal = {
                "ticker": str(data.get("ticker", "Unknown")),
                "date_signal": str(data.get("date_signal", "Unknown")),
                "dirigeant": str(data.get("dirigeant", "Unknown")),
                "qualite": str(data.get("qualite", "Unknown")),
                "type_operation": str(data.get("type_operation", "Unknown")),
                "montant": 0.0,
                "source_record_id": doc_info.get("record_id"),  # Link back to Bronze layer
                "url_doc": doc_info.get("pdf_url", ""),
            }

            # Safe Float Conversion & Calculation for 'montant' (amount)
            try:
                m = data.get("montant")
                # If montant is 0 or missing, try to calculate from quantity * price
                if not m or m == 0:
                    qty = float(
                        str(data.get("nombre_actions", 0)).replace(" ", "").replace(",", "")
                    )
                    px = float(str(data.get("prix_moyen", 0)).replace("€", "").replace(",", "."))
                    m = qty * px

                if isinstance(m, str):
                    m = m.replace("€", "").replace(" ", "").replace(",", ".")

                final_signal["montant"] = float(m)
            except Exception:
                final_signal["montant"] = 0.0

            # Convert each signal back to JSON string for the payload list
            processed_signals.append(json.dumps(final_signal))

        return processed_signals  # Returns a LIST of strings

    except Exception as e:
        logger.error(f"❌ Error during parsing for {doc_info['societe']}: {e}")
        return None


def is_valid_insider(dirigeant, type_op, date_signal):
    """
    Validates the extracted insider trading record based on name format and date.

    Args:
        dirigeant (str): The name of the director or individual.
        type_op (str): The type of operation (e.g., Purchase, Sale).
        date_signal (str): The transaction date in 'YYYY-MM-DD' format.

    Returns:
        bool: True if the record passes all validation filters, False otherwise.
    """
    if not dirigeant or not type_op:
        return False

    name = str(dirigeant).strip()
    name_lower = name.lower()
    name_parts = name_lower.split()
    num_words = len(name_parts)

    # 1. Word count filter (Targeting 2 or 3 words maximum for person names)
    if num_words < 2 or num_words > 3:
        return False

    # 2. DESCRIPTIVE WORD BLACKLIST
    # If the "name" contains any of these words, it is likely a description, not a person
    forbidden_words = {
        "ancien",
        "dirigeant",
        "membre",
        "société",
        "groupe",
        "direction",
        "representant",
        "permanent",
        "fondation",
    }

    # Check if at least one word in the name is in the blacklist
    if any(word in forbidden_words for word in name_parts):
        return False

    # 3. Known entities filter (Background noise reduction)
    bad_entities = ["carbios", "nestlé", "pepsico", "l'oréal", "suntory"]
    if any(entity in name_lower for entity in bad_entities):
        return False

    # 4. Date filter (Discard signals without a valid date)
    date_str = str(date_signal).lower() if date_signal else "none"

    try:
        from datetime import datetime

        # Attempt to convert the string into a datetime object
        # If date_str is "2026-03-26", it transforms it into a manipulatable object
        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")

        # If the date is prior to 2025, discard the signal
        if dt_obj.year < 2025:
            return False
    except (ValueError, TypeError):
        # If the date is not in the correct format or is "unknown"/"none",
        # the try block fails and the record is discarded
        return False

    return not ("inconnue" in date_str or date_str == "none")


def upload_full_json_to_bigquery(rows, project_id, dataset_id, table_id):
    """
    Standardized protocol to upload a list of records to a specific BigQuery table.

    Args:
        rows (list[dict]): A list of dictionaries representing the records to insert.
        project_id (str): The Google Cloud Project ID.
        dataset_id (str): The destination dataset (e.g., 'silver').
        table_id (str): The destination table name (e.g., 'amf_insider_parser').

    Returns:
        None: The function performs an in-place upload and logs the result.
    """
    client = bigquery.Client(project=project_id)

    # 1. Dataset Management: Reference and creation if missing
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_id}' verified.")
    except NotFound:
        logger.info(f"Dataset '{dataset_id}' not found. Creating it in EU location...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        logger.info(f"Dataset '{dataset_id}' successfully created.")

    # 2. Job Configuration: Optimized for JSON ingestion and Schema Evolution
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Automatically detects schema from JSON fields
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",  # Appends new data to the existing table
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    if not rows:
        logger.warning(f"No records provided for table '{table_id}'. Aborting upload.")
        return

    # 3. Execution: Upload to the specific table provided as parameter
    table_ref = dataset_ref.table(table_id)

    try:
        logger.info(f"🚀 Uploading {len(rows)} rows to {dataset_id}.{table_id}...")

        # Start the Load Job
        load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)

        # Wait for the job to complete (synchronous block)
        result = load_job.result()

        logger.success(
            f"✅ Success: {result.output_rows} rows appended to "
            f"'{project_id}.{dataset_id}.{table_id}'."
        )

    except Exception as e:
        logger.error(f"❌ Critical error uploading to table '{table_id}': {e}")


def extract_insider_signals(all_documents, env):
    """
    Processes a list of documents to extract insider trading signals using AI.
    Handles multiple signals per document (e.g., cases with multiple directors).

    Args:
        all_documents (list[dict]): A list of document metadata dictionaries
                                    (including 'societe', 'pdf_url', 'isin', and 'record_id').

    Returns:
        list[dict]: A list of enriched and validated signal dictionaries ready
                    for database insertion.
    """
    final_payload = []

    for doc in all_documents:
        logger.info(f"Starting analysis for: {doc['societe']}")

        # 1. Text Extraction
        text = extract_text_from_pdf(doc["pdf_url"])

        if not text:
            logger.warning(f"Empty text for {doc['societe']}. Skipping.")
            continue

        logger.info(f"Text extracted ({len(text)} chars). Calling AI...")

        # 2. AI Extraction (Groq/Llama)
        result_json_list = parse_insider_data(text, doc, env)

        if result_json_list is None:
            logger.critical(
                "🛑 Groq API limit reached or error occurred. Stopping pipeline to avoid unnecessary loops."
            )
            break

        if isinstance(result_json_list, list):
            for signal_json_str in result_json_list:
                try:
                    # Decode the JSON string extracted by the AI
                    signal = json.loads(signal_json_str)

                    # --- 1. DATA EXTRACTION AND VALIDATION ---
                    dirigeant_value = signal.get("dirigeant")
                    type_op_value = signal.get("type_operation", "")
                    date_value = signal.get("date_signal")

                    # Validate the individual signal to filter out noise or formatting errors
                    if not is_valid_insider(dirigeant_value, type_op_value, date_value):
                        logger.warning(f"⚠️ Discarded: {dirigeant_value} | Op: {type_op_value}")
                        continue

                    # --- 2. ENRICHMENT AND SAFETY OVERRIDE ---
                    # Use verified data from the Bronze layer to prevent AI hallucinations
                    signal["societe"] = doc.get("societe", "Unknown")
                    signal["isin"] = doc.get("isin", "Unknown")
                    signal["source_record_id"] = doc.get("record_id")

                    # Traceability metadata
                    signal["processed_at"] = datetime.now().isoformat()
                    signal["extraction_method"] = "groq-llama-3.1-8b-instant"

                    # Fallback for the ticker if not found by AI
                    if not signal.get("ticker") or signal.get("ticker") == "Unknown":
                        signal["ticker"] = doc.get("isin", "Unknown")

                    # Add to the final payload
                    final_payload.append(signal)
                    logger.success(f"✅ Signal identified: {dirigeant_value} ({doc['societe']})")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON signal for {doc['societe']}: {e}")
                except Exception as e:
                    logger.error(f"Error during signal enrichment for {doc['societe']}: {e}")
        else:
            logger.warning(
                f"Expected a list of signals from parse_insider_data, but got {type(result_json_list)}"
            )

    # Courtesy pause between processing documents to manage load
    time.sleep(1)

    logger.info(f"Analysis complete. Total signals extracted: {len(final_payload)}")
    return final_payload


"""
def filter_insider_text(text):
    
    if not text:
        return "EMPTY", None

    # Keywords for insider trading detection
    keywords = [r"achat", r"vente", r"souscription", r"exercice", r"acquisition", r"cession"]
    pattern = re.compile("|".join(keywords), re.IGNORECASE)

    # If keywords are found, we return a success status and the original text
    if pattern.search(text):
        return "KEEP", text
    
    # Otherwise, we return a skip status
    return "SKIP", None
"""


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(env) -> None:
    """
    Main orchestration function for the AMF Insider Trading pipeline.

    Main steps:
        Load pending records from the Bronze layer (BigQuery).
        Load existing records from the Silver layer to check for duplicates.
        Filter documents to identify new, unprocessed records.
        Trigger AI extraction (Groq/Llama) on filtered documents.
        Upload successfully parsed signals to the Silver layer (BigQuery).

    Returns:
        None: Executes the end-to-end pipeline and logs progress/results.
    """
    try:
        # Data Loading from BigQuery: fetch records from the Bronze layer using our global table constant
        pending_docs = load_from_bigquery(
            env["GCP_PROJECT_ID"], env["BQ_DATASET_BRONZE"], GCP_PREFIX_INPUT_TABLE_ID
        )
        if not pending_docs:
            logger.info("No documents found to process. Exiting pipeline.")
            return
        else:
            logger.success(
                f"✅ Successfully retrieved {len(pending_docs)} valid records for PDF analysis."
            )

        # Duplicate Check: retrieve successfully processed records from the Silver layer
        # Ensure the Silver query selects the 'source_record_id' column
        current_docs = load_from_bigquery(
            env["GCP_PROJECT_ID"], env["BQ_DATASET_SILVER"], GCP_PREFIX_OUTPUT_TABLE_ID
        )
        if not current_docs:
            logger.info("Silver table is currently empty. All pending documents will be processed.")
        else:
            logger.success(
                f"✅ Retrieved {len(current_docs)} records from Silver layer for comparison."
            )

        # Create a set of already processed record IDs for O(1) lookups
        already_done_ids = {
            doc["source_record_id"] for doc in current_docs if "source_record_id" in doc
        }

        # Filtering Logic: identify pending documents have not yet been processed
        # by checking their record_id against the Silver layer IDs
        docs_to_process = []
        for doc in pending_docs:
            # Verify that the key exists to prevent critical failure during lookup
            if "record_id" in doc:
                if doc["record_id"] not in already_done_ids:
                    docs_to_process.append(doc)
            else:
                logger.warning(f"Document record_id missing: {doc}")

        logger.info(
            f"📊 Stats: {len(pending_docs)} found in Bronze, {len(already_done_ids)} already in Silver."
        )
        logger.success(f"🚀 To process: {len(docs_to_process)} new documents.")

        # AI Extraction: trigger the Groq/Llama extraction only on the filtered list of documents
        if docs_to_process:
            logger.info(f"📊 Starting AI extraction on {len(docs_to_process)} new documents...")
            final_payload = extract_insider_signals(docs_to_process, env)

            # PHASE 5: Save to BigQuery (Silver Layer)
            if final_payload:
                logger.info(f"Uploading {len(final_payload)} signals to Silver layer...")

                # Uploading the rows using the standardized BigQuery helper
                upload_full_json_to_bigquery(
                    rows=final_payload,
                    project_id=env["GCP_PROJECT_ID"],
                    dataset_id=env["BQ_DATASET_SILVER"],
                    table_id=GCP_PREFIX_OUTPUT_TABLE_ID,  # Global constant for 'amf_insider_parser'
                )
            else:
                logger.info("No valid insider signals found in this batch.")

    except Exception as e:
        # Global exception handler to prevent silent failures in the workflow
        logger.critical(f"Critical failure in processing pipeline: {e}")


if __name__ == "__main__":
    """
    Entry point for the AMF Insider Parser pipeline.
    Initializes logging and triggers the orchestration flow.

    Input:
        Environment variables loaded via load_and_log_environment().
    Output:
        Execution of the 'run()' pipeline and log entries in the console/file.
    """
    # Load environment variables (API keys, Project IDs, etc.)
    global env
    env = load_and_log_environment()

    logger.info("Pipeline 'amf_insider_parser' started.")
    run(env)
