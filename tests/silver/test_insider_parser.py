import json
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Import the core logic functions from your silver module
from silver.silver_amf_insider_parser import is_valid_insider, parse_insider_data


def load_and_log_environment():
    """
    Redefined loader for the test environment to ensure absolute path resolution.
    """
    # Force the base directory to the project root
    base_dir = Path(__file__).resolve().parents[2]
    env_path = base_dir / ".env"

    # Explicitly load the .env file from the absolute path
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.success(f"Environment variables loaded from: {env_path}")
    else:
        logger.warning(f".env file not found at: {env_path}")

    # Build the config dictionary
    config = {
        "BASE_DIR": base_dir,
        "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "BQ_DATASET_BRONZE": os.getenv("BQ_DATASET_BRONZE"),
        "BQ_DATASET_SILVER": os.getenv("BQ_DATASET_SILVER"),
        "GROQ_API_KEY": os.getenv("GROQ_API_KEY"),
        "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY"),
    }

    logger.info("--- Test Environment Configuration Check ---")
    for var, value in config.items():
        if value:
            logger.success(f"{var: <30} : {value}")
        else:
            logger.warning(f"{var: <30} : NOT DEFINED")
    logger.info("--------------------------------------------")

    return config


# Global environment variable for the module
env = load_and_log_environment()


def test_basic_flow():
    """
    Main test function to verify AI parsing and insider validation logic.
    """
    logger.info("\n--- [TEST] Starting Basic Flow ---")

    if not env.get("GROQ_API_KEY"):
        logger.error("GROQ_API_KEY is missing. AI parsing test skipped.")
        return

    # Mock Data Preparation
    mock_doc_info = {
        "record_id": "amf_2026_test_001",
        "societe": "UBISOFT ENTERTAINMENT",
        "isin": "FR0000054470",
    }

    mock_pdf_text = """
    AMF France - Insider Trading Declaration.
    Company: UBISOFT ENTERTAINMENT.
    Declarant: Yves Guillemot. Role: CEO.
    Operation: Purchase of shares.
    Date: 2026-03-25. 
    Price: 15.50 EUR. Quantity: 2000.
    """

    # AI Parsing Execution
    # We pass the env dictionary if your function requires it,
    # otherwise it uses the one in its own module.
    logger.info("Sending data to AI for parsing...")
    extracted_signals = parse_insider_data(mock_pdf_text, mock_doc_info)

    if not extracted_signals:
        logger.warning("No signals were extracted. Check API logs.")
        return

    # --- [STEP 3] Validation & Enrichment ---
    final_payload = []
    for signal_str in extracted_signals:
        try:
            signal = json.loads(signal_str)

            # Use the imported validation function
            if is_valid_insider(
                signal.get("dirigeant"), signal.get("type_operation"), signal.get("date_signal")
            ):
                signal.update(
                    {"isin": mock_doc_info["isin"], "processed_at": "2026-04-09T10:00:00"}
                )
                final_payload.append(signal)
                logger.success(f"VALIDATED: {signal.get('dirigeant')} | {signal.get('montant')}€")
            else:
                logger.warning(f"REJECTED: {signal.get('dirigeant')} failed validation rules.")
        except Exception as e:
            logger.error(f"Error parsing signal JSON: {e}")

    logger.info(f"\n--- [RESULTS] Successfully processed {len(final_payload)} signals ---")


if __name__ == "__main__":
    try:
        test_basic_flow()
    except Exception as e:
        logger.error(f"Test crashed with error: {e}")
