from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from duckdb import df
import pandas as pd
import boto3
from datetime import datetime
import os

POSTGRES_CONN_ID = 'supabase_postgres'
S3_BUCKET = 'meditrack360-buckett-group2'
S3_PREFIX = 'raw/postgres'

def get_tables(engine):
    from sqlalchemy import  inspect
    inspector = inspect(engine)

    tables = inspector.get_table_names()
    return tables


query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
"""


def extract_and_upload():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    records = hook.get_records(query)
    tables = [r[0] for r in records]
    print(tables)
    

    for table in tables:
        df = hook.get_pandas_df(f"Select * from {table}")
        upload_df_to_s3(df=df,bucket='meditrack360-buckett-group2', key=f'bronze/raw/{table}.csv')

        # local_path = f"/tmp/{table}.csv"
        print(df)
    
        # df.to_csv(local_path, index=False)

        # # Upload to S3
        # s3 = boto3.client('s3')
        # s3.upload_file(local_path, S3_BUCKET, f"{S3_PREFIX}/{table}.csv")
        print(f" Uploaded {table}.csv to S3")


def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str, aws_conn_id: str = "aws_meditrack"):
    """
    Convert DataFrame to CSV in memory and upload to S3 using Airflow S3Hook.
    """
    csv_buffer = df.to_csv(index=False)

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer,
        key=key,
        bucket_name=bucket,
        replace=True   # overwrite if exists
    )
    return f"Uploaded: s3://{bucket}/{key}"  



def extract_all_tables():
        
        extract_and_upload()

default_args = {'start_date': datetime(2023, 1, 1)}

with DAG("extract_postgres_to_s3_dag", schedule_interval=None, catchup=False, default_args=default_args) as dag:
    extract_task = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_all_tables
    )