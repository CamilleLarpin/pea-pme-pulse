import os
import sys
from pathlib import Path
import json
import csv
import io
from datetime import datetime
from time import mktime
from loguru import logger
import feedparser
import re
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from src.bronze.fuzzy_match import match_companies
from dotenv import load_dotenv

# --- CONFIGURATION ---
JSON_DB_PATH_SOURCE = 'abcbourse_data.json'
JSON_DB_PATH_SOURCE_RAW = 'abcbourse_data_raw.json'
GCP_PREFIX_TABLE_ID = 'abcbourse'

GCP_JSON_ACCESS_CREDENTIALS = "bootcamp-project-pea-pme.json"

# Mapping of RSS sources
rss_sources = [
    {"url": "https://www.abcbourse.com/rss/displaynewsrss", "key": "news_rss"},
    {"url": "https://www.abcbourse.com/rss/lastanalysisrss", "key": "analysis_rss"},
    {"url": "http://www.abcbourse.com/rss/chroniquesrss", "key": "chroniques_rss"} 
]

def load_and_log_environment():
    """
    Loads environment variables, logs their status, and returns them as a dictionary.

    Input:
        - None (Reads directly from the OS Environment Variables).

    Output:
        - config (dict): A dictionary mapping environment variable names to their values.
    """

    # Get current file's parent directory (3 levels up) to ensure we are in the project root
    base_dir = Path(__file__).resolve().parents[3] 
    
    gcp_credential_json_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not gcp_credential_json_file:
        logger.critical("GOOGLE_APPLICATION_CREDENTIALS not defined")
        sys.exit(1)

    # Initialize the configuration dictionary by fetching values from the OS
    config = {
        "BASE_DIR": base_dir,
        "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "GCS_BUCKET_NAME": os.getenv("GCS_BUCKET_NAME"),
        "BQ_DATASET_BRONZE": os.getenv("BQ_DATASET_BRONZE"),
        "GCS_SOURCE_REFERENTIEL": os.getenv("GCS_SOURCE_REFERENTIEL")
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
    
def reset_local_db():
    """
    Completely resets the local JSON database by overwriting it 
    with an empty structure and returns an initial data dictionary.
    
    Returns:
        dict: A dictionary mapping source keys to empty lists, 
              ready for new RSS data ingestion.
    """
    # Get current directory of the script to ensure we are writing to the correct location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, JSON_DB_PATH_SOURCE)
    
    # Define an empty structure for your new JSON: to prevents 'FileNotFoundError' in subsequent steps
    empty_structure = {
        "sources": [],
        "last_updated": datetime.now().isoformat(),
        "status": "initialized"
    }

    try:
        # Using 'w' mode automatically truncates (empties) the file if it exists
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(empty_structure, f, indent=4)           
        logger.success(f"Local database reset and initialized at: {config_path}")
    except Exception as e:
        logger.exception(f"Failed to reset local database: {e}")

   # Initialize structure if file is missing
    return {source['key']: [] for source in rss_sources}

def save_local_db(data, filename=JSON_DB_PATH_SOURCE):
    """
    Writes the updated data dictionary back to the JSON file with indentation.

    Args:
        data (dict): The dictionary containing RSS entries or filtered results.
        filename (str): The target file path (defaults to JSON_DB_PATH_SOURCE).
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Database saved successfully to {filename}")
    except Exception as e:
        logger.error(f"Failed to save JSON database: {e}")

def import_rss(json_data):
    """
    Parses RSS feeds and appends new, unique entries to the provided data dictionary.
    
    Args:
        json_data (dict): The in-memory database (dictionary) to be populated. 
                          Expected format: { "source_key": [ {entry_1}, {entry_2} ] }
    
    Returns:
        None: Updates the json_data object in-place.
    """
    # Global regex pattern for ISIN (2 letters + 10 alphanumeric characters)
    ISIN_RE = re.compile(r'\b([A-Z]{2}[A-Z0-9]{10})\b')

    for source in rss_sources:
        key = source['key']
        url = source['url']
        
        logger.info(f"--- Processing source: {key} ({url}) ---")
        feed = feedparser.parse(url)
        
        if feed.bozo:
            logger.warning(f"Feed at {url} might be malformed, attempting to parse anyway.")

        # Extract existing GUIDs for deduplication
        existing_guids = {item['guid'] for item in json_data.get(key, [])}
        new_entries_count = 0
        
        for entry in feed.entries:
            # Use 'id' (GUID) or fallback to 'link'
            guid = entry.get('id', entry.link)
            
            if guid not in existing_guids:
                # 1. Handle Publication Date
                if 'published_parsed' in entry:
                    pub_date = datetime.fromtimestamp(mktime(entry.published_parsed)).isoformat()
                else:
                    pub_date = datetime.now().isoformat()
                
                # Extract ISIN. Try to get it directly from common RSS tags (if provided by the source)
                isin_value = entry.get('isin') or entry.get('finance_isin') or entry.get('asset_isin')
                
                # If no tag is found, search within Title and Summary using Regex
                if not isin_value:
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')
                    search_text = f"{title} {summary}"
                    
                    match = ISIN_RE.search(search_text)
                    isin_value = match.group(1) if match else None

                # Build the new entry object
                new_item = {
                    "guid": guid,
                    "isin": isin_value,
                    "title": entry.get('title', 'No Title'),
                    "link": entry.link,
                    "description": entry.get('summary', ''),
                    "published": pub_date,
                    "ingested_at": datetime.now().isoformat()
                }
                
                # Append to the database
                if key not in json_data:
                    json_data[key] = []
                
                json_data[key].append(new_item)
                new_entries_count += 1               

        logger.success(f"Finished {key}: Added {new_entries_count} new entries.")

def load_company_list(base_path, file_rel_path):
    """
    Reads a CSV file from the local filesystem and returns it as a Pandas DataFrame.

    Args:
        base_path (str): The root directory (e.g., HOME path).
        file_rel_path (str): The relative path to the CSV file from the base_path.

    Returns:
        pd.DataFrame: A DataFrame containing the company list. 
                      Returns an empty DataFrame if an error occurs.
    """
    # Construct the absolute path
    full_path = os.path.join(base_path, file_rel_path)
    
    logger.info(f"Attempting to load company list from local path: {full_path}")
    
    try:
        # Check if file exists before trying to read it
        if not os.path.exists(full_path):
            logger.error(f"File not found at: {full_path}")
            return pd.DataFrame()

        # Load directly from the local path
        df = pd.read_csv(full_path)
        
        # Column cleaning: force lowercase and remove leading/trailing whitespaces
        df.columns = [c.strip().lower() for c in df.columns]
        
        logger.info(f"Successfully loaded from local storage. Found {len(df)} companies.")
        return df

    except Exception as e:
        # Log the error and return an empty DataFrame to avoid pipeline crashes
        logger.error(f"Error reading local CSV: {e}")
        return pd.DataFrame()

def filter_rss_entries_fuzzy(db_data: dict, referentiel: pd.DataFrame) -> dict:
    """
    Orchestrates fuzzy matching across multiple RSS categories using a reference DataFrame.

    Input:
        - db_data (dict): Dictionary where keys are categories (e.g., 'news_rss') 
                          and values are lists of RSS entry dictionaries.
        - referentiel (pd.DataFrame): Reference table containing official company names 
                                      and identifiers (ISIN, Ticker).

    Output:
        - filtered_db (dict): Dictionary containing only the entries that successfully 
                              matched a company, converted back to a list of records.
    """
    if referentiel.empty:
        logger.warning("Referentiel is empty, skipping fuzzy matching.")
        return {}

    # Standardize column names: force lowercase and remove leading/trailing spaces
    # This ensures consistency if the CSV uses 'Name' instead of 'name'
    referentiel.columns = [c.lower().strip() for c in referentiel.columns]

    filtered_db = {}

    # Iterate through each RSS category and its corresponding entries
    for category, entries in db_data.items():
        if not entries:
            continue
            
        # Call the core matching function (External dependency - UNTOUCHED)
        matched_df = match_companies(entries, referentiel)
        
        # Filter only valid matches based on the existence of 'matched_name'
        if "matched_name" in matched_df.columns:
            # Create a mask to identify rows where a match was actually found (not NaN)
            mask = matched_df["matched_name"].notna()
            final_matches_df = matched_df[mask]
            
            # If valid matches exist, store them in the final dictionary as list of dicts
            if not final_matches_df.empty:
                filtered_db[category] = final_matches_df.to_dict(orient="records")
            
    return filtered_db

def upload_to_bucket(local_file_path, bucket_name, destination_blob_name):
    """
    Uploads a local file to a Google Cloud Storage (GCS) bucket.

    Args:
        local_file_path (str): The path to the file on your local machine.
        bucket_name (str): The name of the target GCS bucket.
        destination_blob_name (str): The desired path/name within the bucket.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        logger.info(f"Preparing to upload {local_file_path} to GCP bucket '{bucket_name}' as '{destination_blob_name}'") 
        
        # Initialize the client: automatically look for GOOGLE_APPLICATION_CREDENTIALS env var
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        logger.info(f"Uploading {local_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        
        blob.upload_from_filename(local_file_path)       
        logger.success("Upload completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to GCP: {e}")
        return False

def upload_full_json_to_bigquery(full_data_dict, project_id, dataset_id):
    """
    Takes a dictionary where keys are table names and values are lists of records.
    Automatically creates the dataset if missing and manages table uploads.

    Args:
        full_data_dict (dict): Data structured as {table_name: [list_of_dicts]}.
        project_id (str): The Google Cloud Project ID.
        dataset_id (str): The BigQuery Dataset ID.
    """
    client = bigquery.Client(project=project_id)
    
    # Check if the dataset exists, if not, create it
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists.")
        
    except NotFound:
        logger.info(f"Dataset {dataset_id} not found. Creating it now...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU" # Or your preferred location (US, etc.)
        client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_id} created.")

    # Job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND", 
    )

    if not full_data_dict:
        logger.info("Input dictionary is empty.")
        return

    for table_name, rows in full_data_dict.items():
        if not rows:
            continue

        full_table_name = f"{GCP_PREFIX_TABLE_ID}_{table_name}"
        table_ref = dataset_ref.table(full_table_name)
        
        try:
            logger.info(f"Uploading to {full_table_name}...")
            load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
            load_job.result()  
            logger.info(f"Success: Table '{full_table_name}' updated.")
        except Exception as e:
            logger.error(f"Error uploading to table '{full_table_name}': {e}")

if __name__ == "__main__":
    try:
        logger.info("Pipeline 'ingestion-abcbourse-peapme' started.")
        env = load_and_log_environment()
        print(f"DEBUG: Credenziali attive -> {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
   
        # Clear existing data to prevent duplicates
        db_items = reset_local_db()
    
        # fetching, deduplicating, and storing RSS entries
        import_rss(db_items)
        logger.info(f"Current RSS feed has {len(db_items)} entries.")

        # Save the raw source to the JSON file
        save_local_db(db_items, JSON_DB_PATH_SOURCE_RAW)
        
        # Get current list of companies from CSV in GCS
        companies_list = load_company_list(env["BASE_DIR"], env["GCS_SOURCE_REFERENTIEL"])
            
        db_filtered_items = filter_rss_entries_fuzzy(db_items, companies_list)
        logger.info(f"Filtered RSS entries: {len(db_filtered_items)} matched company targets.")

        # Save the entire updated dictionary to the JSON file
        save_local_db(db_filtered_items)
        
        # Upload to GCP bucket
        upload_to_bucket(JSON_DB_PATH_SOURCE_RAW, env["GCS_BUCKET_NAME"], JSON_DB_PATH_SOURCE_RAW)
        
        upload_full_json_to_bigquery(db_filtered_items, env["GCP_PROJECT_ID"], env["BQ_DATASET_BRONZE"] )

    except Exception as e:
        logger.critical(f"Critical failure in ingestion pipeline: {e}")
