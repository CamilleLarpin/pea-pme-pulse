import os

from fastapi import FastAPI
from google.cloud import bigquery

app = FastAPI(title="pea-pme-pulse API", version="0.1.0")

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "bootcamp-project-pea-pme")


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


@app.get("/overview")
def overview():
    """List all datasets in the BQ project with their tables."""
    client = get_bq_client()
    result = {}
    for dataset in client.list_datasets():
        tables = [t.table_id for t in client.list_tables(dataset.dataset_id)]
        result[dataset.dataset_id] = tables
    return {"project": GCP_PROJECT, "datasets": result}


@app.get("/overview/gold")
def overview_gold():
    """List tables in the gold dataset."""
    client = get_bq_client()
    tables = [t.table_id for t in client.list_tables("gold")]
    return {"project": GCP_PROJECT, "dataset": "gold", "tables": tables}
