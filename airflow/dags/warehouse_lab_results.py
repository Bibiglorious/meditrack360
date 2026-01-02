from datetime import datetime
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

AWS_CONN_ID = "aws_meditrack"
PG_CONN_ID = "supabase_postgres"  
BUCKET_NAME = "meditrack360-datalake-dev"
SILVER_KEY = "silver/lab_results/lab_results_clean.csv"

TARGET_TABLE = "lab_results_silver" 


def load_silver_to_postgres():
    # 1) Read Silver CSV from S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    csv_str = s3_hook.read_key(key=SILVER_KEY, bucket_name=BUCKET_NAME)

    if not csv_str:
        raise ValueError(f"Could not read {SILVER_KEY} from bucket {BUCKET_NAME}")

    df = pd.read_csv(StringIO(csv_str))

    if df.empty:
        print("Silver DataFrame is empty – nothing to load.")
        return

    print(f"Loaded {len(df)} rows from Silver CSV")

    
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    
    df.to_sql(
        TARGET_TABLE,
        engine,
        if_exists="replace",  
        index=False,
    )

    print(f"Wrote {len(df)} rows to Postgres table '{TARGET_TABLE}'")


default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="warehouse_lab_results",
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=["meditrack", "warehouse", "lab_results"],
) as dag:

    load_task = PythonOperator(
        task_id="load_silver_to_postgres",
        python_callable=load_silver_to_postgres,
    )