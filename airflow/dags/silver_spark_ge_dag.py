from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {"start_date": datetime(2023, 1, 1)}

HOST_ROOT = os.environ.get("MEDITRACK_HOST_ROOT")
if not HOST_ROOT:
    raise RuntimeError("MEDITRACK_HOST_ROOT is not set in docker-compose env.")

mounts = [
    Mount(source=f"{HOST_ROOT}/scripts", target="/scripts", type="bind"),
    Mount(source=f"{HOST_ROOT}/data", target="/data", type="bind"),
]

with DAG(
    dag_id="silver_spark_ge_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "silver", "spark", "dq"],
) as dag:

    run_spark_silver = DockerOperator(
        task_id="spark_transform_to_silver",
        image="spark:3.5.1-scala2.12-java11-python3-ubuntu",
        command="/opt/spark/bin/spark-submit /scripts/etl/silver_lab_results_spark.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    #Great Expectations validate -> reads parquet from /data/silver/lab_results/
    validate_silver_ge = DockerOperator(
        task_id="validate_silver_with_great_expectations",
        image="docker-great-expectations",
        command="python /scripts/data_quality/validate_silver_lab_results.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )

    run_spark_silver >> validate_silver_ge