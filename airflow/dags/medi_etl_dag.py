from datetime import datetime
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BUCKET_NAME = "meditrack360-datalake-dev"   
AWS_CONN_ID = "aws_meditrack"           


def upload_test_file_to_s3(**context):
   

    # 1) Create a small CSV file locally (inside the container)
    tmp_dir = Path("/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    execution_date = context["ds_nodash"]
    local_path = tmp_dir / f"medi_hello_{execution_date}.csv"

    rows = [
        "message,timestamp",
        f'"Hello MediTrack360!", "{context["ts"]}"',
    ]
    local_path.write_text("\n".join(rows))

    # 2) Upload to S3 Bronze using S3Hook
    s3_key = f"bronze/test/medi_hello_{execution_date}.csv"

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_file(
        filename=str(local_path),
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    print(f"Uploaded {local_path} to s3://{BUCKET_NAME}/{s3_key}")


default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="medi_etl_dag",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=["meditrack"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_test_file_to_s3",
        python_callable=upload_test_file_to_s3,
        provide_context=True,
    )
