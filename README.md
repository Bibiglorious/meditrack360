# MediTrack360 Data Platform

Overview

MediTrack360 is a simulated healthcare data engineering platform designed to mimic real-world workflows in hospital environments. It models a data pipeline that ingests, cleans, transforms, and loads patient lab result data to support timely decision-making and analytics in clinical settings.
This project was built to demonstrate modern data engineering best practices, focusing on data lakehouse architecture, orchestration with Apache Airflow, and data warehousing with Amazon Redshift.



The Problem: Lab Result Delays in Hospitals
Hospitals generate massive amounts of lab data daily — from blood tests to imaging results. However, poor data pipeline infrastructure can result in:
* Delayed test results
* Redundant or missing data
* Manual and error-prone reporting
* Slow decision-making in emergencies



MediTrack360 simulates this challenge by providing a fully automated pipeline that can:
* Track lab turnaround times
* Identify bottlenecks in data ingestion
* Support analytics dashboards in Redshift for clinical and operational teams


Project Objectives
* Automate ingestion of lab results into a centralized S3-based data lake
* Clean and transform raw data using PySpark
* Enrich data into fact and dimension tables in the Gold layer
* Load final datasets into Amazon Redshift for analytics
* Orchestrate all tasks with Apache Airflow



Tech Stack
* Airflow: Orchestration of end-to-end pipeline
* Apache Spark (PySpark): Data cleaning and transformations
* AWS S3: Data lake (Bronze, Silver, Gold layers)
* Amazon Redshift: Analytics data warehouse
* Docker: Local development environment
* Terraform: Infrastructure-as-code 
* CI/CD: Github Actions



Data Architecture
Implemented a Lakehouse model using the following zones:
1. Bronze Layer – Raw data from hospital systems (CSV files)
2. Silver Layer – Cleaned and standardized using Spark
3. Gold Layer – Final analytics-ready tables (Fact + Dimensions)
4. Redshift – Data warehouse for dashboards and analysis



Pipeline Flow
1. Extract: Uploads local CSV lab results to S3 Bronze.
2. Transform: Cleans and enriches data into Silver using PySpark.
3. Load: Builds fact and dimension tables in Gold layer.
4. Warehouse Load: Uses Airflow to load Gold tables into Redshift using COPY.



Local Setup with Docker
# 1. Clone the repo
git clone https://github.com/your-username/meditrack360.git
cd meditrack360

# 2. Start Airflow with Docker
docker compose up -d

# 3. Access Airflow UI
http://localhost:8080 (user: admin / pw: admin)








