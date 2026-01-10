from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
import os

AWS_CONN_ID = "aws_meditrack"  # Falls back to ~/.aws/credentials
BUCKET_NAME = "meditrack360-buckett-group2"
S3_KEY = "silver/api/api_clean.csv"
LOCAL_PATH = "data/silver/api/api_clean.csv"  # <- MUST match what Spark reads in your DAG

def download_api_silver():
    print("⏳ Downloading api_clean.csv from S3...")
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    content = hook.read_key(bucket_name=BUCKET_NAME, key=S3_KEY)
    df = pd.read_csv(StringIO(content))

    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    df.to_csv(LOCAL_PATH, index=False)

    print(f" Downloaded and saved to {LOCAL_PATH}")

if __name__ == "__main__":
    download_api_silver()