# /functions/extract/main.py
import pandas as pd
import requests
import os
from google.cloud import storage
from datetime import datetime

def fetch_data_from_api(dataset_id):
    all_data = []
    offset = 0
    limit = 1000
    url = f"https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0"

    while True:
        payload = {"offset": offset, "limit": limit}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        batch = response.json().get("results", [])
        all_data.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

    return pd.DataFrame(all_data)

def upload_parquet_to_gcs(df, bucket_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    df.to_parquet("temp.parquet", index=False)
    blob.upload_from_filename("temp.parquet")
    os.remove("temp.parquet")

def extract_data(dataset_id, name):
    df = fetch_data_from_api(dataset_id)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    blob_path = f"{name}/raw/{name}_{timestamp}.parquet"
    upload_parquet_to_gcs(df, bucket_name="cms-extract-parquet", destination_blob_name=blob_path)
    return f"gs://cms-extract-parquet/{blob_path}"
