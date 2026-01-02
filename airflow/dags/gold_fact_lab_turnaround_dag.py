from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.etl.gold.build_fact_lab_turnaround import build_fact_lab_turnaround_to_gold

AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"

default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="gold_fact_lab_turnaround_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "gold", "lab_results"],
) as dag:

    build_fact = PythonOperator(
        task_id="build_fact_lab_turnaround",
        python_callable=build_fact_lab_turnaround_to_gold,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "bucket_name": BUCKET_NAME,
            "silver_prefix": "silver/lab_results/",
            "gold_key": "gold/fact_lab_turnaround/fact_lab_turnaround.parquet",
        },
    )