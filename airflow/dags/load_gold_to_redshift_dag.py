from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='load_gold_to_redshift',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['redshift', 'etl', 'gold']
) as dag:

    copy_dim_date = PostgresOperator(
        task_id='copy_dim_date',
        postgres_conn_id='group2_redshift_meditrack',
        sql="""
            COPY meditrack.dim_date
            FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_date.csv'
            IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
            FORMAT AS CSV
            IGNOREHEADER 1
            REGION 'eu-north-1';
        """
    )

    copy_dim_patient = PostgresOperator(
        task_id='copy_dim_patient',
        postgres_conn_id='group2_redshift_meditrack',
        sql="""
            COPY meditrack.dim_patient
            FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_patient.csv'
            IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
            FORMAT AS CSV
            IGNOREHEADER 1
            REGION 'eu-north-1';
        """
    )

    copy_dim_test_type = PostgresOperator(
        task_id='copy_dim_test_type',
        postgres_conn_id='group2_redshift_meditrack',
        sql="""
            COPY meditrack.dim_test_type
            FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_test_type.csv'
            IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
            FORMAT AS CSV
            IGNOREHEADER 1
            REGION 'eu-north-1';
        """
    )

    copy_fact_lab_turnaround = PostgresOperator(
        task_id='copy_fact_lab_turnaround',
        postgres_conn_id='group2_redshift_meditrack',
        sql="""
            COPY meditrack.fact_lab_turnaround
            FROM 's3://meditrack360-buckett-group2/gold/fact_lab_turnaround/'
            IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
            FORMAT AS PARQUET;
        """
    )

    # Load dimensions first, then fact table
    [copy_dim_date, copy_dim_patient, copy_dim_test_type] >> copy_fact_lab_turnaround


    # Data Quality Checks
    validate_fact = PostgresOperator(
        task_id='validate_fact_row_count',
        postgres_conn_id='group2_redshift_meditrack',
        sql='SELECT COUNT(*) FROM meditrack.fact_lab_turnaround;'
    )

    copy_fact_lab_turnaround >> validate_fact
