from __future__ import annotations

import os
import glob
from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# -----------------------------
# Config (edit if needed)
# -----------------------------
AWS_CONN_ID = "aws_meditrack"
BUCKET_NAME = "meditrack360-datalake-dev"

# Inside the Airflow containers (because you mounted ../data -> /opt/airflow/data)
LOCAL_BRONZE_DIR = "/opt/airflow/data/bronze/lab_results"
LOCAL_SILVER_DIR = "/opt/airflow/data/silver/lab_results"

S3_BRONZE_PREFIX = "bronze/lab_results/"
S3_SILVER_PREFIX = "silver/lab_results/"
S3_GOLD_PREFIX = "gold/lab_results/"  # placeholder


HOST_ROOT = os.environ.get("MEDITRACK_HOST_ROOT")
if not HOST_ROOT:
    raise RuntimeError(
        "MEDITRACK_HOST_ROOT is not set. Add it to docker-compose.yml under "
        "airflow-webserver and airflow-scheduler environment."
    )

HOST_SCRIPTS = f"{HOST_ROOT}/scripts"
HOST_DATA = f"{HOST_ROOT}/data"


BIND_MOUNTS = [
    Mount(source=HOST_SCRIPTS, target="/scripts", type="bind"),
    Mount(source=HOST_DATA, target="/data", type="bind"),
]


# -----------------------------
# Helpers
# -----------------------------
def upload_bronze_csvs_to_s3():
    
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    csv_paths = sorted(glob.glob(f"{LOCAL_BRONZE_DIR}/*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No CSVs found in {LOCAL_BRONZE_DIR}")

    for p in csv_paths:
        fname = os.path.basename(p)
        key = f"{S3_BRONZE_PREFIX}{fname}"
        hook.load_file(filename=p, key=key, bucket_name=BUCKET_NAME, replace=True)

    print(f"Uploaded {len(csv_paths)} bronze file(s) to s3://{BUCKET_NAME}/{S3_BRONZE_PREFIX}")


def upload_silver_parquet_to_s3():
    """
    Upload local Spark output parquet to S3 silver.
    Spark writes to your Mac folder: data/silver/lab_results
    Airflow sees it via mount: /opt/airflow/data/silver/lab_results
    """
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    parquet_paths = sorted(glob.glob(f"{LOCAL_SILVER_DIR}/*.parquet"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found in {LOCAL_SILVER_DIR}")

    for p in parquet_paths:
        fname = os.path.basename(p)
        key = f"{S3_SILVER_PREFIX}{fname}"
        hook.load_file(filename=p, key=key, bucket_name=BUCKET_NAME, replace=True)

    print(f"Uploaded {len(parquet_paths)} silver parquet file(s) to s3://{BUCKET_NAME}/{S3_SILVER_PREFIX}")


def gold_placeholder():
    """
    Placeholder for Gold marts.
    Next step: read Silver parquet, create facts/dims, write to /data/gold, upload to S3 gold.
    """
    print("Gold step placeholder: will build facts/dims next.")


# -----------------------------
# DAG
# -----------------------------
default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="meditrack_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "pipeline", "bronze", "silver", "dq", "gold"],
) as dag:

    # -----------------------------
    # Bronze
    # -----------------------------
    with TaskGroup("bronze", tooltip="Bronze ingestion (raw -> S3 bronze)") as tg_bronze:
        bronze_to_s3 = PythonOperator(
            task_id="upload_bronze_csvs_to_s3",
            python_callable=upload_bronze_csvs_to_s3,
        )

    # -----------------------------
    # Silver (Spark)
    # -----------------------------
    with TaskGroup("silver", tooltip="Silver transform (Spark) + upload to S3 silver") as tg_silver:
        run_spark_silver = DockerOperator(
            task_id="spark_transform_to_silver",
            image="spark:3.5.1-scala2.12-java11-python3-ubuntu",
            command="/opt/spark/bin/spark-submit /scripts/etl/silver_lab_results_spark.py",
            docker_url="unix://var/run/docker.sock",
            network_mode="docker_default",
            mounts=BIND_MOUNTS,
            auto_remove=True,
        )

        silver_to_s3 = PythonOperator(
            task_id="upload_silver_parquet_to_s3",
            python_callable=upload_silver_parquet_to_s3,
        )

        run_spark_silver >> silver_to_s3

    

    # Data Quality (Great Expectations container)
    with TaskGroup("data_quality", tooltip="Great Expectations checks on Silver") as tg_dq:
        ge_validate_silver = DockerOperator(
            task_id="validate_silver_with_gx",
            image="docker-great-expectations",
            command="python /scripts/data_quality/validate_silver_lab_results.py",
            docker_url="unix://var/run/docker.sock",
            network_mode="docker_default",
            mounts=BIND_MOUNTS,
            auto_remove=True,
        )

    
    
    # Gold 
    with TaskGroup("gold", tooltip="Gold marts (facts/dims) - placeholder") as tg_gold:
        build_gold = PythonOperator(
            task_id="gold_placeholder",
            python_callable=gold_placeholder,
        )

    # Flow
    tg_bronze >> tg_silver >> tg_dq >> tg_gold