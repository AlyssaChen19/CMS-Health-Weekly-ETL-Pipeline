from google.cloud import bigquery

def create_schema(name):
    client = bigquery.Client()
    dataset_id = "cms_datasets"
    raw_table_id = f"{client.project}.{dataset_id}.{name}_raw"

    # Create dataset if it does not exist
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_id)
        print(f"Created dataset: {dataset_id}")

    # Try to clear the raw table if it exists
    try:
        client.query(f"DELETE FROM `{raw_table_id}` WHERE true").result()
        print(f"Cleared raw table: {raw_table_id}")
    except Exception:
        print(f"Raw table {raw_table_id} not found, skipping delete.")
