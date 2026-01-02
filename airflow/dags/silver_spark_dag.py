from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {"start_date": datetime(2023, 1, 1)}

HOST_SCRIPTS = "/Users/ebereglorious/Downloads/MediTrack_360/scripts"
HOST_DATA = "/Users/ebereglorious/Downloads/MediTrack_360/data"

mounts = [
    Mount(source=HOST_SCRIPTS, target="/opt/spark/scripts", type="bind"),
    Mount(source=HOST_DATA, target="/opt/spark/data", type="bind"),
]

with DAG(
    dag_id="silver_spark_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "silver", "spark"],
) as dag:

    run_silver_spark = DockerOperator(
        task_id="run_silver_spark_transform",
        image="spark:3.5.1-scala2.12-java11-python3-ubuntu",
        command="/opt/spark/bin/spark-submit /opt/spark/scripts/etl/silver_lab_results_spark.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        mounts=mounts,
        auto_remove=True,
    )