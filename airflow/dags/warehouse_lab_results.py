from datetime import datetime
from io import StringIO
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Connection and bucket settings
AWS_CONN_ID = "aws_meditrack"
PG_CONN_ID = "supabase_postgres"
BUCKET_NAME = "meditrack360-buckett-group2"

def load_csv_to_postgres(s3_key, target_table):
    # Connect to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    csv_str = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)

    if not csv_str:
        raise ValueError(f"Could not read {s3_key} from bucket {BUCKET_NAME}")

    df = pd.read_csv(StringIO(csv_str))

    if df.empty:
        print(f"{s3_key} is empty – nothing to load.")
        return

    print(f"Loaded {len(df)} rows from {s3_key}")

    # Connect to Supabase Postgres
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql(
        TARGET_TABLE,
        engine,
        schema="medi_reader",  
        if_exists="replace",
        index=False,
    )

    print(f"Wrote {len(df)} rows to Postgres table '{target_table}'")
    return f"Success: {len(df)} rows written to {target_table}"

# DAG setup
default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="warehouse_lab_results",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "warehouse"],
) as dag:

    load_lab_results = PythonOperator(
        task_id="load_silver_lab_results_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "s3_key": "silver/lab_results/lab_results_clean.csv",
            "target_table": "lab_results_silver"
        },
    )

    load_api_results = PythonOperator(
        task_id="load_silver_api_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "s3_key": "silver/api/api_clean.csv",
            "target_table": "api_silver"
        },
    )

    # task order
    load_lab_results >> load_api_results