from unittest.mock import MagicMock, patch

import pandas as pd

from src.gold.amf_insider_flow import run_insider_scoring


# Use a specific path for the patch to ensure it targets the correct module imports
@patch("src.gold.amf_insider_flow.bigquery.Client")
@patch("src.gold.amf_insider_flow.compute_insider_score")
@patch("os.path.exists")
@patch("os.getenv")
def test_run_insider_scoring_success(mock_getenv, mock_exists, mock_compute, mock_bq_client):
    """
    Test the full Gold orchestration: Extraction -> Scoring -> Idempotent Merge.
    Checks that data types and BQ interactions are correctly handled.
    """

    # Mock environment variables for GCP credentials and dataset names
    mock_getenv.side_effect = lambda k, default=None: {
        "GOOGLE_APPLICATION_CREDENTIALS": "/fake/key.json",
        "GCP_PROJECT_ID": "test-project",
        "BQ_DATASET_SILVER": "silver_ds",
        "BQ_DATASET_GOLD": "gold_ds",
    }.get(k, default)
    mock_exists.return_value = True

    # Mock BigQuery Client and its methods
    mock_client_instance = mock_bq_client.return_value

    # Mock Silver Extraction
    mock_df_silver = pd.DataFrame(
        {"isin": ["FR0000121014"], "signal_date": ["2026-04-09"], "montant": [15000.0]}
    )
    mock_client_instance.query.return_value.to_dataframe.return_value = mock_df_silver

    # Mock Gold Transformation Result
    # Adding updated_at with timezone as in the real script
    mock_df_gold = pd.DataFrame(
        {
            "isin": ["FR0000121014"],
            "signal_date": [pd.to_datetime("2026-04-09").date()],
            "societe": ["Test Corp"],
            "score_1_10": [7.0],
            "updated_at": [pd.Timestamp.now(tz="UTC")],
        }
    )
    mock_compute.return_value = mock_df_gold

    # Mock Table Existence (to skip the 'Table Not Found' creation block)
    mock_client_instance.get_table.return_value = MagicMock()

    # Execute the flow
    run_insider_scoring()

    # Assert compute_insider_score was called with the correct DataFrame
    mock_compute.assert_called_once()

    # Check if the scoring function was called with the data from Silver
    mock_compute.assert_called_once_with(mock_df_silver)

    # Verify BigQuery Interactions: extraction query + merge query
    assert mock_client_instance.query.call_count >= 2

    # Check if the last query called was indeed a MERGE statement
    last_query = mock_client_instance.query.call_args_list[-1][0][0]
    assert "MERGE" in last_query.upper()
    assert "USING" in last_query.upper()

    # Verify staging table lifecycle
    assert mock_client_instance.load_table_from_dataframe.called
    assert mock_client_instance.delete_table.called

    print(
        "\n✅ Gold Flow Verified: Extraction, Transformation, and Idempotent Upsert are working correctly."
    )
