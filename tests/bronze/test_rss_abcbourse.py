from bronze.rss_abcbourse.rss_abcbourse import import_rss, rss_sources, filter_rss_entries_fuzzy

def test_basic_flow():
    """
    Main test function to verify the RSS ingestion and fuzzy matching logic.
    """
    
    # Initialize local mock database (mirroring reset_local_db behavior)
    # Input: rss_sources (list of dicts with 'key' and 'url')
    # Output: dict with source keys as keys and empty lists as values
    db_items = {source['key']: [] for source in rss_sources}

    # Test RSS Ingestion
    # Action: Fetch and parse live feeds from ABC Bourse
    print("--- [TEST] Starting RSS Ingestion ---")
    import_rss(db_items)
    
    total_entries = sum(len(entries) for entries in db_items.values())
    print(f"Total entries retrieved: {total_entries}")

    # Test Fuzzy Matching with Mock Company List
    # Input: List of strings (company names)
    # Note: Use names likely to appear in today's financial news for a successful match
    mock_companies = ["TotalEnergies", "LVMH", "Airbus", "Renault", "Stellantis", "Société Générale"]
    
    print("\n--- [TEST] Starting Fuzzy Matching Analysis ---")
    # Output: Dict containing only enriched entries that crossed the MATCH_THRESHOLD
    filtered = filter_rss_entries_fuzzy(db_items, mock_companies)
    
    match_count = sum(len(entries) for entries in filtered.values())
    print(f"Total matches found: {match_count}")

    # Display Match Results
    if match_count > 0:
        for category, entries in filtered.items():
            for entry in entries:
                # Log detail: [Category] Match: Name (Score) -> Title snippet
                print(f"[{category}] Match: {entry['matched_name']} "
                      f"(Score: {entry['match_score']}) -> {entry['title'][:60]}...")
    else:
        print("No matches found. Consider lowering MATCH_THRESHOLD or updating mock_companies.")

if __name__ == "__main__":
    test_basic_flow()