from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('load_gold_to_redshift',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

    load_redshift = BashOperator(
        task_id='copy_to_redshift',
        bash_command='psql -h meditrack-wg.449692851516.eu-west-2.redshift-serverless.amazonaws.com -p 5439 -U admin -d dev -f /opt/airflow/sql/redshift/copy_gold_to_redshift.sql'
    )
