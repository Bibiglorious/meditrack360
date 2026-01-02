from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.etl.gold.build_dim_date import build_dim_date
from scripts.etl.gold.build_dim_patient import build_dim_patient
from scripts.etl.gold.build_dim_test_type import build_dim_test_type

AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"

default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="gold_dimensions_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "gold", "dimensions", "lab_results"],
) as dag:

    dim_date = PythonOperator(
        task_id="build_dim_date",
        python_callable=build_dim_date,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "silver_prefix": "silver/lab_results/",
            "gold_prefix": "gold/dim_date/",
        },
    )

    dim_patient = PythonOperator(
        task_id="build_dim_patient",
        python_callable=build_dim_patient,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "silver_prefix": "silver/lab_results/",
            "gold_prefix": "gold/dim_patient/",
        },
    )

    dim_test_type = PythonOperator(
        task_id="build_dim_test_type",
        python_callable=build_dim_test_type,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "silver_prefix": "silver/lab_results/",
            "gold_prefix": "gold/dim_test_type/",
        },
    )

    dim_date >> dim_patient >> dim_test_type