--  Load dim_date
COPY meditrack.dim_date
FROM 's3://meditrack360-datalake-dev/gold/dimensions/dim_date.csv'
IAM_ROLE 'arn:aws:iam::449692851516:role/meditrack-redshift-s3-read-role'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-west-2';

--  Load dim_patient
COPY meditrack.dim_patient
FROM 's3://meditrack360-datalake-dev/gold/dimensions/dim_patient.csv'
IAM_ROLE 'arn:aws:iam::449692851516:role/meditrack-redshift-s3-read-role'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-west-2';

--  Load dim_test_type
COPY meditrack.dim_test_type
FROM 's3://meditrack360-datalake-dev/gold/dimensions/dim_test_type.csv'
IAM_ROLE 'arn:aws:iam::449692851516:role/meditrack-redshift-s3-read-role'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-west-2';

--  Load fact_lab_turnaround 
COPY meditrack.fact_lab_turnaround
FROM 's3://meditrack360-datalake-dev/gold/fact_lab_turnaround/'
IAM_ROLE 'arn:aws:iam::449692851516:role/meditrack-redshift-s3-read-role'
FORMAT AS PARQUET
REGION 'eu-west-2';