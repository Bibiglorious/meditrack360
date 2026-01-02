from datetime import datetime
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"


SILVER_KEY = "silver/lab_results/lab_results_clean.csv"


GOLD_DAILY_KEY = "gold/lab_results/daily_summary.csv"
GOLD_PATIENT_KEY = "gold/lab_results/patient_summary.csv"
GOLD_TESTTYPE_KEY = "gold/lab_results/test_type_summary.csv"


def generate_gold_lab_results():
    
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    
    print(f" Reading Silver file: s3://{BUCKET_NAME}/{SILVER_KEY}")
    csv_str = s3.read_key(key=SILVER_KEY, bucket_name=BUCKET_NAME)
    if not csv_str:
        raise ValueError(f"Could not read Silver file at {SILVER_KEY}")

    df = pd.read_csv(StringIO(csv_str))

    if df.empty:
        print("Silver DataFrame is empty. No Gold outputs will be generated.")
        return

    print(f"Loaded Silver DataFrame with {len(df)} rows and {len(df.columns)} columns")

    # Ensure datetime columns are proper
    for col in ["collected_time", "completed_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Ensure result_date exists as date
    if "result_date" not in df.columns:
        if "collected_time" in df.columns:
            df["result_date"] = df["collected_time"].dt.date
        else:
            raise ValueError("Neither 'result_date' nor 'collected_time' is available to derive a date.")

    # Ensure result_value is numeric
    if "result_value" in df.columns:
        df["result_value"] = pd.to_numeric(df["result_value"], errors="coerce")

    # Derive a simple turnaround time in minutes (if timestamps exist)
    if "collected_time" in df.columns and "completed_time" in df.columns:
        df["turnaround_minutes"] = (
            (df["completed_time"] - df["collected_time"]).dt.total_seconds() / 60.0
        )
    else:
        df["turnaround_minutes"] = pd.NA

    
    print("Building daily_summary (per result_date)...")

    daily_group = df.groupby("result_date", dropna=True)

    daily_summary = daily_group.agg(
        total_tests=("lab_result_id", "count"),
        unique_patients=("patient_id", "nunique"),
        avg_result_value=("result_value", "mean"),
        min_result_value=("result_value", "min"),
        max_result_value=("result_value", "max"),
        avg_turnaround_minutes=("turnaround_minutes", "mean"),
    ).reset_index()

    print(f"Daily summary rows: {len(daily_summary)}")

   
    print("Building patient_summary (per patient_id)...")

    patient_group = df.groupby("patient_id", dropna=True)

    patient_summary = patient_group.agg(
        total_tests=("lab_result_id", "count"),
        first_test_date=("result_date", "min"),
        last_test_date=("result_date", "max"),
        avg_result_value=("result_value", "mean"),
        avg_turnaround_minutes=("turnaround_minutes", "mean"),
        distinct_test_types=("test_type", "nunique"),
    ).reset_index()

    print(f"   - Patient summary rows: {len(patient_summary)}")

 
    print(" Building test_type_summary (per test_type)...")

    test_group = df.groupby("test_type", dropna=True)

    test_type_summary = test_group.agg(
        total_tests=("lab_result_id", "count"),
        unique_patients=("patient_id", "nunique"),
        avg_result_value=("result_value", "mean"),
        min_result_value=("result_value", "min"),
        max_result_value=("result_value", "max"),
        avg_turnaround_minutes=("turnaround_minutes", "mean"),
    ).reset_index()

    print(f"   - Test type summary rows: {len(test_type_summary)}")

    
    def write_df_to_s3(df_out: pd.DataFrame, key: str):
        buf = StringIO()
        df_out.to_csv(buf, index=False)
        buf.seek(0)
        s3.load_string(
            string_data=buf.getvalue(),
            bucket_name=BUCKET_NAME,
            key=key,
            replace=True,
        )
        print(f"Wrote {len(df_out)} rows to s3://{BUCKET_NAME}/{key}")

    print("Writing Gold outputs to S3...")
    write_df_to_s3(daily_summary, GOLD_DAILY_KEY)
    write_df_to_s3(patient_summary, GOLD_PATIENT_KEY)
    write_df_to_s3(test_type_summary, GOLD_TESTTYPE_KEY)

    print("Gold lab results generation completed.")


default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="gold_lab_results_dag",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=["meditrack", "gold", "lab_results"],
) as dag:

    gold_task = PythonOperator(
        task_id="generate_gold_lab_results",
        python_callable=generate_gold_lab_results,
    )