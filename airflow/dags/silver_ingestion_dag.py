from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-buckett-group2"


# ============================================================
# LAB RESULTS: Bronze → Silver (CSV)
# ============================================================

def bronze_to_silver_lab_results():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    prefix = "bronze/lab_results"
    output_key = "silver/lab_results/lab_results_clean.csv"

    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    if not keys:
        raise FileNotFoundError(f"No files found in s3://{BUCKET_NAME}/{prefix}")

    csv_keys = [k for k in keys if k.endswith(".csv")]
    if not csv_keys:
        raise FileNotFoundError(f"No CSV files found under s3://{BUCKET_NAME}/{prefix}")

    frames = []
    for key in sorted(csv_keys):
        content = hook.read_key(key=key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(StringIO(content))
        frames.append(df)
        print(f"Read {key} with {len(df)} rows")

    df = pd.concat(frames, ignore_index=True)

    # Clean column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

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
        raise ValueError(f"Missing expected columns: {missing}")

    for col in ["collected_time", "completed_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        

    df["result_date"] = df["collected_time"].dt.date
    df["result_value"] = pd.to_numeric(df["result_value"], errors="coerce")

    df = df.dropna(how="all")
    df = df.dropna(subset=["lab_result_id", "patient_id", "test_type"])
    df = df.drop_duplicates(subset=["lab_result_id"])

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    hook.load_string(
        string_data=buffer.getvalue(),
        key=output_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    print(f"✅ Wrote Silver lab results to s3://{BUCKET_NAME}/{output_key}")


# ============================================================
# API: Bronze → Silver (CSV)
# ============================================================

def bronze_to_silver_api():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    prefix = "bronze/api"
    output_key = "silver/api/api_clean.csv"

    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    if not keys:
        raise FileNotFoundError(f"No files found in s3://{BUCKET_NAME}/{prefix}")

    csv_keys = [k for k in keys if k.endswith(".csv")]
    if not csv_keys:
        raise FileNotFoundError(f"No CSV files found under s3://{BUCKET_NAME}/{prefix}")

    frames = []
    for key in sorted(csv_keys):
        content = hook.read_key(key=key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(StringIO(content))
        frames.append(df)
        print(f"Read {key} with {len(df)} rows")

    df = pd.concat(frames, ignore_index=True)

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.dropna(how="all").drop_duplicates()

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    hook.load_string(
        string_data=buffer.getvalue(),
        key=output_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    print(f"✅ Wrote Silver API CSV to s3://{BUCKET_NAME}/{output_key}")


# ============================================================
# API: Silver CSV → Silver Parquet
# ============================================================

def silver_api_csv_to_parquet():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    input_key = "silver/api/api_clean.csv"
    output_key = "silver/api/api_clean.parquet"

    csv_content = hook.read_key(key=input_key, bucket_name=BUCKET_NAME)
    df = pd.read_csv(StringIO(csv_content))

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    hook.load_bytes(
        bytes_data=buffer.read(),
        key=output_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    print(f"Converted {input_key} → {output_key}")


# ============================================================
# DAG DEFINITION
# ============================================================

default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="silver_ingestion_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "silver"],
) as dag:

    silver_lab_results_task = PythonOperator(
        task_id="bronze_to_silver_lab_results",
        python_callable=bronze_to_silver_lab_results,
    )

    silver_api_task = PythonOperator(
        task_id="bronze_to_silver_api",
        python_callable=bronze_to_silver_api,
    )

    silver_api_parquet_task = PythonOperator(
        task_id="silver_api_csv_to_parquet",
        python_callable=silver_api_csv_to_parquet,
    )

    # Execution order
    silver_lab_results_task >> silver_api_task >> silver_api_parquet_task