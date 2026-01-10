-- ============================================
-- COPY Raw + Gold Data from S3 into Redshift
-- Source: S3 silver and gold layers
-- IAM Role: AmazonRedshift-CommandsAccessRole
-- Region: eu-north-1
-- Author: [Your Name]
-- Last Updated: 2026-01-10
-- ============================================

-- ========== Load Raw Data into Public Schema ==========

-- Load lab results (raw)
COPY public.warehouse_lab_results
FROM 's3://meditrack360-buckett-group2/silver/lab_results/lab_results_clean.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1';

-- Load API data (raw)
COPY public.warehouse_api_data
FROM 's3://meditrack360-buckett-group2/silver/api/api_clean.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
REGION 'eu-north-1';

-- Load drug inventory (raw)
COPY public.drug_inventory
FROM 's3://meditrack360-buckett-group2/silver/api/api_clean.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
REGION 'eu-north-1';

-- Load lab_results_clean (raw)
COPY public.lab_results_clean
FROM 's3://meditrack360-buckett-group2/silver/lab_results/lab_results_clean.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto'
DATEFORMAT 'auto'
REGION 'eu-north-1';


-- ========== Load Gold Data into Meditrack Schema ==========

-- Load dim_date
COPY meditrack.dim_date
FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_date.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1';

-- Load dim_patient
COPY meditrack.dim_patient
FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_patient.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1';

-- Load dim_test_type
COPY meditrack.dim_test_type
FROM 's3://meditrack360-buckett-group2/gold/dimensions/dim_test_type.csv'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'eu-north-1';

-- Load fact_lab_turnaround
COPY meditrack.fact_lab_turnaround
FROM 's3://meditrack360-buckett-group2/gold/fact_lab_turnaround/'
IAM_ROLE 'arn:aws:iam::819340487562:role/service-role/AmazonRedshift-CommandsAccessRole-20260107T220857'
FORMAT AS PARQUET;