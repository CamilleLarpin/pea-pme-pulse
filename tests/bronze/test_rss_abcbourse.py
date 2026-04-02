import pandas as pd
from loguru import logger
from src.bronze.rss_abcbourse.rss_abcbourse import import_rss, rss_sources
from src.bronze.fuzzy_match import match_companies

def test_basic_flow():
    """
    Main test function to verify the RSS ingestion and fuzzy matching logic.
    
    Input: 
        - Live RSS feeds from ABC Bourse.
        - A Mock Pandas DataFrame acting as a company reference table.
    Output: 
        - Console logs of matched financial news entries.
    """
    
    # Initialize local mock database: a dictionary with source keys as keys and empty lists as values
    # This simulates the structure expected by import_rss and match_companies functions
    db_items = {source['key']: [] for source in rss_sources}

    # Test RSS Ingestion: fetch and parse live feeds from ABC Bourse
    # We expect db_items to be populated with entries categorized by source keys after this step
    logger.info("\n--- [TEST] Starting RSS Ingestion ---")
    import_rss(db_items)
    
    total_entries = sum(len(entries) for entries in db_items.values())
    logger.info(f"Total entries retrieved from RSS: {total_entries}")

    if total_entries == 0:
        logger.warning("No entries found in RSS feeds. Match test will likely be empty.")
        return

    # Prepare Mock Reference DataFrame: this simulates the 'referentiel' DataFrame used in match_companies
    # We include 'name', 'isin', 'ticker_bourso', and 'ticker
    mock_data = {
        "name": [
            "TotalEnergies", "LVMH", "Airbus", "Renault", 
            "Stellantis", "Société Générale", "Orange", "Vivendi"
        ],
        "isin": [
            "FR0000120271", "FR0000121014", "NL0000235190", "FR0000131906", 
            "NL00150001Q9", "FR0000130809", "FR0000133308", "FR0000127771"
        ],
        "ticker_bourso": ["TTE", "MC", "AIR", "RNO", "STLAP", "GLE", "ORA", "VIV"],
        "ticker": ["TTE", "MC", "AIR", "RNO", "STLA", "GLE", "ORA", "VIV"]
    }
    referentiel_df = pd.DataFrame(mock_data)
    
    logger.info("\n--- [TEST] Starting Fuzzy Matching Analysis ---")
    
    # Execute Matching Logic: we iterate through each category to match entries against our mock DataFrame
    # The match_companies function will return a DataFrame with matched entries, scores, and company metadata
    # We will filter for successful matches (where 'matched_name' is not NaN) and convert back to list of dicts for display
    filtered_results = {}
    
    for category, entries in db_items.items():
        if not entries:
            continue
            
        logger.info(f"Processing category: {category}...")
        
        # Call the core matching function
        # Input: List of dicts (RSS entries), DataFrame (Reference)
        # Output: DataFrame with matching scores and company metadata
        matched_df = match_companies(entries, referentiel_df)
        
        # Filter for successful matches only (where 'matched_name' is not NaN)
        if not matched_df.empty and "matched_name" in matched_df.columns:
            mask = matched_df["matched_name"].notna()
            final_matches_df = matched_df[mask]
            
            if not final_matches_df.empty:
                # Convert back to list of dicts for storage/display
                filtered_results[category] = final_matches_df.to_dict(orient="records")

    # Display Match Results
    total_matches = sum(len(entries) for entries in filtered_results.values())
    logger.info(f"\n--- [RESULTS] Total matches found: {total_matches} ---")

    if total_matches > 0:
        for category, entries in filtered_results.items():
            for entry in entries:
                # Extract details safely
                company = entry.get('matched_name', 'Unknown')
                score = entry.get('match_score', 0)
                title = entry.get('title', 'No Title')
                isin = entry.get('isin', 'N/A')
                
                logger.success(f"[{category}] Match: {company} (ISIN: {isin}) "
                      f"Score: {score}% -> {title[:75]}...")
    else:
        logger.info("No matches found. This is normal if today's news doesn't mention the mock companies.")
        logger.info("Try adding companies currently in the news to 'mock_data'.")

if __name__ == "__main__":
    try:
        test_basic_flow()
    except Exception as e:
        logger.error(f"Test failed with error: {e}")