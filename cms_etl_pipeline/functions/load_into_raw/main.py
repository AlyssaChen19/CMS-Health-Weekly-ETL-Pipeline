from google.cloud import bigquery

def load_to_raw_table(df, name):
    client = bigquery.Client()
    table_id = f"cms_datasets.{name}_raw"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Append new data to the raw table
        autodetect=True  # Let BigQuery automatically infer the schema from the DataFrame
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to finish

    print(f"Loaded {len(df)} rows into {table_id}")
