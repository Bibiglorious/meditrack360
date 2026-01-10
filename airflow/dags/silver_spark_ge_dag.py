from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {"start_date": datetime(2023, 1, 1)}

# Get the host root directory from env variable
HOST_ROOT = os.environ.get("MEDITRACK_HOST_ROOT")
if not HOST_ROOT:
    raise RuntimeError("MEDITRACK_HOST_ROOT is not set in docker-compose env.")

# Define Docker volume mounts
mounts = [
    Mount(source=f"{HOST_ROOT}/scripts", target="/scripts", type="bind"),
    Mount(source=f"{HOST_ROOT}/data", target="/data", type="bind"),
]

# Define DAG
with DAG(
    dag_id="silver_spark_ge_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "silver", "spark", "dq"],
) as dag:

    # 1. Transform lab results to Silver
    run_spark_silver = DockerOperator(
        task_id="spark_transform_to_silver",
        image="spark:3.5.1-scala2.12-java11-python3-ubuntu",
        command="/opt/spark/bin/spark-submit /scripts/etl/silver_lab_results_spark.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    # 2. Validate lab results using Great Expectations
    validate_silver_lab_results_with_ge = DockerOperator(
        task_id="validate_silver_lab_results_with_ge",
        image="docker-great-expectations",
        command="python /scripts/data_quality/validate_silver_lab_results.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    # 3. Convert API CSV to Parquet
    convert_api_csv_to_parquet = DockerOperator(
        task_id="convert_api_csv_to_parquet",
        image="docker-great-expectations",  # Use the same image that has pandas + pyarrow
        command="python /scripts/data_quality/convert_api_csv_to_parquet.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    # 4. Validate API data with GE
    validate_silver_api_with_ge = DockerOperator(
        task_id="validate_silver_api_with_ge",
        image="docker-great-expectations",
        command="python /scripts/data_quality/validate_silver_api.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    # Task dependencies
    run_spark_silver >> validate_silver_lab_results_with_ge
    validate_silver_lab_results_with_ge >> convert_api_csv_to_parquet
    convert_api_csv_to_parquet >> validate_silver_api_with_ge