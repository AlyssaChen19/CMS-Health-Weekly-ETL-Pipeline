# CMS Health Weekly ETL Pipeline

This repository contains an automated, production-ready ETL pipeline that extracts U.S. healthcare facility data from CMS (Centers for Medicare & Medicaid Services), transforms and cleans the data, and loads it into Google BigQuery for downstream analytics and visualization.

---

## 📌 Project Objectives

- Automate the weekly ingestion and standardization of publicly available CMS hospital datasets.
- Create a clean, consistent dataset in BigQuery for healthcare research and analysis.
- Enable seamless integration with Apache Superset and Streamlit dashboards for real-time visualization and monitoring.

---

## 🗂️ Data Sources

This pipeline pulls from four key CMS datasets, each updated regularly and retrieved via CMS's Open Data API:

1. **Hospital General Information**  
   https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0

2. **Hospital Equity Measures**  
   https://data.cms.gov/provider-data/api/1/datastore/query/v6ff-cmgx/0

3. **Unplanned Hospital Visits**  
   https://data.cms.gov/provider-data/api/1/datastore/query/632h-zaca/0

4. **Payment and Value of Care**  
   https://data.cms.gov/provider-data/api/1/datastore/query/c7us-v4mf/0

---

## ⚙️ ETL Workflow Overview

The pipeline follows a modular ETL structure:

1. **Extract**  
   - API requests (with pagination) to download full datasets.
   - Raw data is stored as Parquet files in a GCS bucket.

2. **Transform**  
   - Clean column names, drop irrelevant or duplicated fields.
   - Convert "Not Available" to `NaN`, and unify date/time formats.
   - Normalize overlapping fields (e.g., `score`, `footnote`) across datasets.
   - Add `ingestion_timestamp` to track when data is loaded.

3. **Load**  
   - Raw data is first written to a `_raw` table in BigQuery.
   - A `_stage` table is updated using a `MERGE` strategy to prevent duplicates.

---

## 🗃️ Storage & Infrastructure

- **BigQuery**  
  All structured data is stored in a BigQuery dataset named `cms_datasets`.

- **Google Cloud Storage (GCS)**  
  Intermediate Parquet files are stored in the `cms-extract-parquet` bucket.

- **Prefect 2.x**  
  Used for orchestration and scheduling of weekly automated runs.

---

## 📊 Visualization (Coming Soon)

- **Apache Superset**  
  For BI dashboards, metric monitoring, and SQL-based visual analysis.

- **Streamlit**  
  For interactive analytics apps built on top of the CMS dataset (e.g., hospital performance comparison, equity score trends).

---

## 🧪 Run the Flow (Local or Cloud Shell)

To manually run the flow:

```bash
python cms_etl_pipeline/flows/cms_etl_flow.py
