from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow")

from scripts.etl.extract import extract_lab_results_to_bronze


AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"

default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="bronze_ingestion_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "bronze", "lab_results"],
) as dag:

    upload_lab_results_to_bronze = PythonOperator(
        task_id="extract_lab_results_to_bronze",
        python_callable=extract_lab_results_to_bronze,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "local_bronze_dir": "/opt/airflow/data/bronze/lab_results",
            "s3_prefix": "bronze/lab_results",
            "replace": False,  
        },
    )