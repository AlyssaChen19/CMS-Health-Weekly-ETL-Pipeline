from google.cloud import bigquery

def upsert_into_stage(name):
    client = bigquery.Client()
    dataset = "cms_datasets"
    raw_table = f"{dataset}.{name}_raw"
    stage_table = f"{dataset}.{name}_stage"

    # Check if stage table exists; if not, create it using raw table's schema
    try:
        client.get_table(stage_table)
    except Exception:
        raw_schema = client.get_table(raw_table).schema
        table = bigquery.Table(f"{client.project}.{stage_table}", schema=raw_schema)
        client.create_table(table)
        print(f"Created stage table: {stage_table}")

    # Use different MERGE keys depending on dataset
    if name == "hospital_info":
        merge_keys = "S.facility_id = R.facility_id AND S.state = R.state AND S.zip_code = R.zip_code"
    else:
        merge_keys = "S.facility_id = R.facility_id"

    query = f"""
    MERGE `{stage_table}` AS S
    USING `{raw_table}` AS R
    ON {merge_keys}
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    job = client.query(query)
    job.result()
    print(f"Upserted from {raw_table} into {stage_table} using MERGE")
