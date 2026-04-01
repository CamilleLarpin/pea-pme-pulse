import os
from pathlib import Path
import json
import csv
import io
from datetime import datetime
from time import mktime
from loguru import logger
import feedparser
from rapidfuzz import fuzz, process
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- CONFIGURATION ---
JSON_DB_PATH_SOURCE = 'abcbourse_data.json'
JSON_DB_PATH_SOURCE_RAW = 'abcbourse_data_raw.json'
CSV_COMPANY_LIST_PATH = 'companies_draft.csv'
GCP_PROJECT_ID = 'bootcamp-project-pea-pme'
GCP_DATASET_ID = 'abcbourse'
GCP_BUCKET_NAME = 'project-pea-pme'
GCP_SOURCE_REFERENTIEL_BLOB = 'boursorama_peapme_final.csv'
GCP_JSON_ACCESS_CREDENTIALS = "bootcamp-project-pea-pme.json"

# Constants for fuzzy matching (adjust as needed)
MATCH_THRESHOLD = 80
SHORT_NAME_MAX_LEN = 6

logger.info("Pipeline 'ingestion-abcbourse-peapme' started.")

# Mapping of RSS sources
rss_sources = [
    {"url": "https://www.abcbourse.com/rss/displaynewsrss", "key": "news_rss"},
    {"url": "https://www.abcbourse.com/rss/lastanalysisrss", "key": "analysis_rss"},
    {"url": "http://www.abcbourse.com/rss/chroniquesrss", "key": "chroniques_rss"} 
]

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
    
    # Define an empty structure for your new JSON
    # This prevents 'FileNotFoundError' in subsequent steps
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
        json_data (dict): The in-memory database to be populated with news entries.
    """
    for source in rss_sources:
        key = source['key']
        url = source['url']
        
        logger.info(f"--- Processing source: {key} ({url}) ---")
        feed = feedparser.parse(url)
        
        # Safety check for feed parsing errors
        if feed.bozo:
            logger.warning(f"Feed at {url} might be malformed, attempting to parse anyway.")

        # Extract existing GUIDs for this specific category to prevent duplicates
        # Using a set provides O(1) lookup time, making it very efficient
        existing_guids = {item['guid'] for item in json_data.get(key, [])}
        new_entries_count = 0
        
        for entry in feed.entries:
            # Use the 'id' (GUID) or fallback to the 'link' for deduplication purposes
            guid = entry.get('id', entry.link)
            
            if guid not in existing_guids:
                # Convert time to ISO format string
                if 'published_parsed' in entry:
                    pub_date = datetime.fromtimestamp(mktime(entry.published_parsed)).isoformat()
                else:
                    pub_date = datetime.now().isoformat()
                
                # Dictionary representing the items we want to store in our JSON database
                new_item = {
                    "guid": guid,
                    "title": entry.title,
                    "link": entry.link,
                    "description": entry.get('summary', ''),
                    "published": pub_date,
                    "ingested_at": datetime.now().isoformat()
                }
                
                # Ensure the category list exists and append the item
                if key not in json_data:
                    json_data[key] = []
                
                json_data[key].append(new_item)
                new_entries_count += 1               
        logger.success(f"Finished {key}: Added {new_entries_count} new entries.")

def load_company_list_gcs(bucket_name, blob_name):
    """
    Reads the company CSV from a GCS bucket and returns a list of names in lowercase.
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The path to the CSV file within the bucket
    Returns:
        list: A list of company names in lowercase. Returns an empty list if an error occurs.
    """
    logger.info(f"Attempting to load company list from GCS: gs://{bucket_name}/{blob_name}")
    company_names = []
    try:
        # Initialize the GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the content as a string
        content = blob.download_as_text(encoding='utf-8')
        
        # Use io.StringIO to let DictReader read the string as if it were a file
        f = io.StringIO(content)
        reader = csv.DictReader(f, delimiter=',')

        for row in reader:
            if 'name' in row and row['name']:
                company_names.append(row['name'].strip().lower())      
        logger.info(f"Successfully loaded from GCS. Found {len(company_names)} companies.")

    except Exception as e:
        # Ensure 'logger' is configured in your script
        logger.error(f"Error reading CSV from GCS ({bucket_name}/{blob_name}): {e}")
        
    return company_names

def filter_rss_entries(db_data, company_list):
    """
    Filters a dictionary of news items (db_data) keeping only those 
    mentioning companies found in the company_list.
    
    Args:
        db_data (dict[str, list]): local database loaded from JSON.
        company_list (list): List of company names (strings).
        
    Returns:
        dict[str, list]: A new dictionary containing only the matches found.
    """   
    filtered_db = {}
    
    # Pre-process targets to lowercase for case-insensitive matching (strip whitespace as well)
    targets = [c.lower().strip() for c in company_list]
    
    for category, entries in db_data.items():
        # Temporary list to store matches for the current category
        category_matches = []
        
        for entry in entries:
            # Extract text fields from the JSON entry and convert to lowercase for case-insensitive matching
            title = entry.get('title', '').lower()
            description = entry.get('description', '').lower()
            content = f"{title} {description}"

            # Check if any company name from our list exists as a substring
            if any(company in content for company in targets):
                category_matches.append(entry)
        
        # Only add the category to the new dict if it contains at least one match
        if category_matches:
            filtered_db[category] = category_matches
    return filtered_db

def filter_rss_entries_fuzzy(db_data: dict, referentiel) -> dict:
    """
    Filters RSS entries by matching content against a company reference list using fuzzy logic.
    
    Args:
        db_data (dict): A dictionary where keys are categories and values are lists of 
                        RSS entry dictionaries (e.g., {'tech': [{'title': '...', ...}]}).
        referentiel (pd.DataFrame or list): A list of company names or a pandas DataFrame 
                                            containing at least a 'name' column. If a DataFrame 
                                            is provided, ISIN and Ticker are enriched.                                      
    Returns:
        dict: A filtered dictionary containing only entries that matched a company, 
              enriched with match metadata (score, name, isin, ticker).
    """
    # Handle referentiel input type (DataFrame vs List)
    is_df = isinstance(referentiel, pd.DataFrame)
    
    if is_df:
        company_names = referentiel["name"].tolist()
    else:
        # If it's a list, we use it directly as the source of names
        company_names = referentiel
        
    filtered_db = {}
    
  

    def _scorer_for(name: str):
        """Internal helper: Use strict ratio for short names, partial for long ones."""
        return fuzz.token_set_ratio if len(name) <= SHORT_NAME_MAX_LEN else fuzz.partial_ratio

    logger.info(f"Starting fuzzy filtering with {len(company_names)} reference names.")

    for category, entries in db_data.items():
        category_matches = []
        
        # Skip if entries is not a list (defensive check)
        if not isinstance(entries, list):
            continue
            
        for entry in entries:
            # Prepare content for matching: combine Title and Description
            title = entry.get('title', '')
            description = entry.get('description', '')
            content = f"{title} {description}"
            
            best_match = None
            
            # Find the best matching company name for this specific entry
            for name in company_names:
                scorer = _scorer_for(name)
                
                # result = (matched_string, score, index)
                result = process.extractOne(
                    content,
                    [name],
                    scorer=scorer,
                    score_cutoff=MATCH_THRESHOLD,
                    processor=str.casefold,
                )
                
                if result and (best_match is None or result[1] > best_match[1]):
                    best_match = result
            
            # Match is found: enrich the entry
            if best_match:
                matched_name, score, _ = best_match
                enriched_entry = entry.copy()
                
                # Basic match info
                enriched_entry.update({
                    "matched_name": matched_name,
                    "match_score": score,
                })

                # This prevents the "list indices must be integers" TypeError
                if is_df:
                    mask = referentiel["name"] == matched_name
                    if not referentiel[mask].empty:
                        ref_row = referentiel[mask].iloc[0]
                        enriched_entry.update({
                            "isin": ref_row.get("isin"),
                            "ticker_bourso": ref_row.get("ticker_bourso"),
                        })
                else:
                    # Default values if no DataFrame context is available
                    enriched_entry.update({
                        "isin": None,
                        "ticker_bourso": None,
                    })
                
                category_matches.append(enriched_entry)
        
        # Only add categories that have at least one match
        if category_matches:
            filtered_db[category] = category_matches
            
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

        table_ref = dataset_ref.table(table_name)
        
        try:
            logger.info(f"Uploading to {table_name}...")
            load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
            load_job.result()  
            logger.info(f"Success: Table '{table_name}' updated.")
        except Exception as e:
            logger.error(f"Error uploading to table '{table_name}': {e}")


if __name__ == "__main__":
    try:
        # Clear existing data to prevent duplicates
        db_items = reset_local_db()
    
        # fetching, deduplicating, and storing RSS entries
        import_rss(db_items)
        logger.info(f"Current RSS feed has {len(db_items)} entries.")

        # Save the raw source to the JSON file
        save_local_db(db_items, JSON_DB_PATH_SOURCE_RAW)
        
        # Get current list of companies from CSV in GCS
        companies_list = load_company_list_gcs(GCP_BUCKET_NAME, GCP_SOURCE_REFERENTIEL_BLOB)
            
        db_filtered_items = filter_rss_entries_fuzzy(db_items, companies_list)
        logger.info(f"Filtered RSS entries: {len(db_filtered_items)} matched company targets.")

        # Save the entire updated dictionary to the JSON file
        save_local_db(db_filtered_items)
        
        # Upload to GCP bucket
        upload_to_bucket(JSON_DB_PATH_SOURCE_RAW, GCP_BUCKET_NAME, JSON_DB_PATH_SOURCE_RAW)
        
        # Upload to GCP BigQuery
        current_path = Path(__file__).resolve()
        project_root = current_path.parents[3]
        credentials_path = os.path.join(project_root, GCP_JSON_ACCESS_CREDENTIALS)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        upload_full_json_to_bigquery(db_items, GCP_PROJECT_ID, GCP_DATASET_ID)

    except Exception as e:
        logger.critical(f"Critical failure in ingestion pipeline: {e}")
