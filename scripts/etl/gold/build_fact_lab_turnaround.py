from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import List, Optional
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dataclass
class GoldLabTurnaroundConfig:
    aws_conn_id: str
    bucket_name: str

    # Where Silver parquet lives in S3 
    silver_prefix: str = "silver/lab_results/"

    # Where Gold output will be written in S3
    gold_key: str = "gold/fact_lab_turnaround/fact_lab_turnaround.parquet"


REQUIRED_COLS = [
    "lab_result_id",
    "patient_id",
    "admission_id",
    "test_type",
    "result_value",
    "unit",
    "collected_time",
    "completed_time",
    "result_date",
]


def _list_parquet_keys(hook: S3Hook, bucket: str, prefix: str) -> List[str]:
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    return [k for k in keys if k.endswith(".parquet")]


def _read_parquets_from_s3(hook: S3Hook, bucket: str, keys: List[str]) -> pd.DataFrame:
    frames = []
    for k in keys:
        obj = hook.get_key(key=k, bucket_name=bucket)
        body = obj.get()["Body"].read()
        df = pd.read_parquet(BytesIO(body))
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_fact_lab_turnaround_to_gold(
    aws_conn_id: str,
    bucket_name: str,
    silver_prefix: str = "silver/lab_results/",
    gold_key: str = "gold/fact_lab_turnaround/fact_lab_turnaround.parquet",
) -> None:
    
    hook = S3Hook(aws_conn_id=aws_conn_id)

    parquet_keys = _list_parquet_keys(hook, bucket_name, silver_prefix)
    if not parquet_keys:
        raise FileNotFoundError(
            f"No Silver parquet files found in s3://{bucket_name}/{silver_prefix}"
        )

    df = _read_parquets_from_s3(hook, bucket_name, parquet_keys)

    # ---- Validate required columns exist ----
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Silver data missing required columns: {missing}")

    # ---- Type conversions ----
    df["collected_time"] = pd.to_datetime(df["collected_time"], errors="coerce", utc=True)
    df["completed_time"] = pd.to_datetime(df["completed_time"], errors="coerce", utc=True)
    df["result_date"] = pd.to_datetime(df["result_date"], errors="coerce").dt.date

    # ---- Compute turnaround ----
    df["turnaround_minutes"] = (
        (df["completed_time"] - df["collected_time"]).dt.total_seconds() / 60.0
    )

    # Remove impossible/negative turnaround
    df = df[df["turnaround_minutes"].notna()]
    df = df[df["turnaround_minutes"] >= 0]

    df["turnaround_hours"] = df["turnaround_minutes"] / 60.0

    # ---- Select final Gold fact columns ----
    fact = df[
        [
            "lab_result_id",
            "patient_id",
            "admission_id",
            "test_type",
            "result_date",
            "collected_time",
            "completed_time",
            "turnaround_minutes",
            "turnaround_hours",
        ]
    ].copy()

    fact = fact.sort_values(["lab_result_id", "completed_time"]).drop_duplicates(
        subset=["lab_result_id"], keep="last"
    )

    # ---- Write parquet to memory and upload to S3 ----
    buf = BytesIO()
    fact.to_parquet(buf, index=False)
    buf.seek(0)

    hook.load_bytes(
        bytes_data=buf.getvalue(),
        key=gold_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(f"Gold fact saved: s3://{bucket_name}/{gold_key}")
    print(f"Rows written: {len(fact)}")