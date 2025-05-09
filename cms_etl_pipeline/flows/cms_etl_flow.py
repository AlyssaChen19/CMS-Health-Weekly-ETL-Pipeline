import sys
import os
import time
from datetime import datetime, timezone

from prefect import flow  

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from functions.extract.main import extract_data
from functions.schema_setup.main import create_schema
from functions.transform.main import transform_data
from functions.load_into_raw.main import load_to_raw_table
from functions.load_into_stage.main import upsert_into_stage

def safe_extract(dataset_id, name, max_retries=3):
    for i in range(max_retries):
        try:
            return extract_data(dataset_id=dataset_id, name=name)
        except Exception as e:
            print(f"❌ Extract failed for {name}, retry {i+1}/{max_retries}: {e}")
            time.sleep(3)
    raise Exception(f"Failed to extract {name} after {max_retries} retries")

@flow  
def cms_etl_pipeline(target_dataset=None):
    datasets = [
        ("hospital_info", "xubh-q36u"),
        ("hosp_equity", "v6ff-cmgx"),
        ("unplanned_visits", "632h-zaca"),
        ("payment_value", "c7us-v4mf")
    ]

    for name, dataset_id in datasets:
        if target_dataset and name != target_dataset:
            continue  # skip others if a specific one is selected

        print(f"\n=== Processing {name} at {datetime.now(timezone.utc).isoformat()} UTC ===")

        try:
            gcs_path = safe_extract(dataset_id=dataset_id, name=name)
            create_schema(name=name)
            df = transform_data(gcs_path=gcs_path, name=name)
            load_to_raw_table(df=df, name=name)
            upsert_into_stage(name=name)
            print(f"✅ Finished processing {name}")
        except Exception as e:
            print(f"❌ Error while processing {name}: {str(e)}")

if __name__ == "__main__":
    target = None
    cms_etl_pipeline(target_dataset=target)
