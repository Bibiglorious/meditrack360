from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "supabase_postgres"


def test_postgres_connection():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

   
    sql = "SELECT current_database(), current_schema(), now();"
    rows = hook.get_records(sql)

    print("Connected to Postgres successfully.")
    print("Result:")
    for row in rows:
        print(row)


default_args = {"start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="postgres_connection_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["meditrack", "postgres", "supabase"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )