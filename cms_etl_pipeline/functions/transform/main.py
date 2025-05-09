import pandas as pd
from google.cloud import storage
import tempfile
import os
from datetime import datetime

def read_parquet_from_gcs(gcs_path):
    if not gcs_path.startswith("gs://"):
        raise ValueError("Invalid GCS path")

    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Download Parquet file from GCS to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        blob.download_to_filename(tmp_file.name)
        df = pd.read_parquet(tmp_file.name)

    os.remove(tmp_file.name)
    return df

def transform_data(gcs_path, name):
    df = read_parquet_from_gcs(gcs_path)

    # Standardize column names: lowercase and underscores
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.utcnow()

    if name == "hospital_info":
        # Drop footnote columns and birthing friendly designation
        df = df.drop(columns=[c for c in df.columns if "footnote" in c] + [
            "meets_criteria_for_birthing_friendly_designation"
        ], errors="ignore")

        # Rename selected columns
        df = df.rename(columns={
            "citytown": "city",
            "countyparish": "county",
            "telephone_number": "phone",
            "hospital_type": "type",
            "hospital_ownership": "ownership",
            "emergency_services": "has_emergency"
        })

    elif name == "hosp_equity":
        df = df.drop(columns=[
            "address", "citytown", "state", "countyparish", "telephone_number", "footnote"
        ], errors="ignore")
        df = df.rename(columns={"score": "equity_score"})
        df["equity_score"] = pd.to_numeric(
            df["equity_score"].replace("Not Applicable", pd.NA), errors="coerce"
        )

    elif name == "unplanned_visits":
        df = df.drop(columns=[
            "address", "citytown", "state", "countyparish", "telephone_number", "footnote"
        ], errors="ignore")
        df = df.rename(columns={"score": "visit_score"})
        df["visit_score"] = pd.to_numeric(df["visit_score"], errors="coerce")

    elif name == "payment_value":
        df = df.drop(columns=[
            "address", "citytown", "state", "countyparish", "telephone_number",
            "payment_footnote", "value_of_care_footnote"
        ], errors="ignore")

    # Convert start_date and end_date to datetime
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Replace "Not Available" with missing value (NaN) in specific columns
    cols_to_fix = [
        "hospital_overall_rating",
        "mort_group_measure_count",
        "count_of_facility_mort_measures",
        "count_of_mort_measures_better",
        "count_of_mort_measures_no_different",
        "count_of_mort_measures_worse",
        "safety_group_measure_count",
        "count_of_facility_safety_measures",
        "count_of_safety_measures_better",
        "count_of_safety_measures_no_different",
        "count_of_safety_measures_worse",
        "readm_group_measure_count",
        "count_of_facility_readm_measures",
        "count_of_readm_measures_better",
        "count_of_readm_measures_no_different",
        "count_of_readm_measures_worse",
        "pt_exp_group_measure_count",
        "count_of_facility_pt_exp_measures",
        "te_group_measure_count",
        "count_of_facility_te_measures",
        "number_of_patients",
        "number_of_patients_returned"
    ]

    for col in cols_to_fix:
        if col in df.columns:
            df[col] = df[col].replace("Not Available", pd.NA)

    return df
