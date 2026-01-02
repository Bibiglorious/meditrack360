from datetime import datetime
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"

BRONZE_PREFIX = "bronze/lab_results" 
SILVER_KEY = "silver/lab_results/lab_results_clean.csv"  


def bronze_to_silver_lab_results():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)


    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=BRONZE_PREFIX)

    if not keys:
        raise FileNotFoundError(
            f"No keys found in s3://{BUCKET_NAME}/{BRONZE_PREFIX}"
        )

    csv_keys = [k for k in keys if k.endswith(".csv")]
    if not csv_keys:
        raise FileNotFoundError(
            f"No CSV files found under s3://{BUCKET_NAME}/{BRONZE_PREFIX}"
        )


    frames = []
    for key in sorted(csv_keys):
        content = hook.read_key(key=key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(StringIO(content))
        frames.append(df)
        print(f"Read {key} with {len(df)} rows")

    df = pd.concat(frames, ignore_index=True)
    print(f"Combined rows: {len(df)}")

   
   
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

   
    expected_cols = [
        "lab_result_id",
        "patient_id",
        "admission_id",
        "test_type",
        "result_value",
        "unit",
        "collected_time",
        "completed_time",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in data: {missing}")

   
    for col in ["collected_time", "completed_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["result_date"] = df["collected_time"].dt.date

    
    df["result_value"] = pd.to_numeric(df["result_value"], errors="coerce")

   
    df = df.dropna(how="all")

    
    df = df.dropna(subset=["lab_result_id", "patient_id", "test_type"], how="any")

    df = df.drop_duplicates(subset=["lab_result_id"])

    print(f"Rows after cleaning: {len(df)}")


    # 4) Write back to S3 as a single Silver CSV
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    hook.load_string(
        string_data=buffer.getvalue(),
        key=SILVER_KEY,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    print(f"Wrote Silver file to s3://{BUCKET_NAME}/{SILVER_KEY}")


default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="silver_ingestion_dag",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=["meditrack", "silver", "lab_results"],
) as dag:

    silver_task = PythonOperator(
        task_id="bronze_to_silver_lab_results",
        python_callable=bronze_to_silver_lab_results,
    )