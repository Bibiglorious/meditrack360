# /opt/airflow/scripts/etl/gold/build_dim_date.py

from io import BytesIO
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _read_s3_df(hook: S3Hook, bucket: str, key: str) -> pd.DataFrame:
    """Read a single S3 object into a pandas DataFrame (parquet or csv)."""
    obj = hook.get_key(key, bucket_name=bucket)
    body = obj.get()["Body"].read()

    if key.lower().endswith(".parquet"):
        return pd.read_parquet(BytesIO(body))
    elif key.lower().endswith(".csv"):
        return pd.read_csv(BytesIO(body))
    else:
        raise ValueError(f"Unsupported file type: {key}")


def build_dim_date(bucket_name: str, aws_conn_id: str, silver_prefix: str = "silver/lab_results/"):
    hook = S3Hook(aws_conn_id=aws_conn_id)

    keys = hook.list_keys(bucket_name=bucket_name, prefix=silver_prefix) or []

    # keep only real data files (ignore _SUCCESS, crc, folders)
    data_keys = [
        k for k in keys
        if (k.lower().endswith(".parquet") or k.lower().endswith(".csv"))
        and "_success" not in k.lower()
        and not k.lower().endswith(".crc")
    ]

    if not data_keys:
        raise FileNotFoundError(f"No usable Silver data files found in s3://{bucket_name}/{silver_prefix}")

    parquet_keys = [k for k in data_keys if k.lower().endswith(".parquet")]
    chosen_key = sorted(parquet_keys)[0] if parquet_keys else sorted(data_keys)[0]

    df = _read_s3_df(hook, bucket_name, chosen_key)

    
    date_cols = []
    for c in ["result_date", "collected_time", "completed_time"]:
        if c in df.columns:
            date_cols.append(c)

    if not date_cols:
        raise ValueError("No date columns found in silver data. Expected one of: result_date, collected_time, completed_time")

    # Convert to datetimes safely
    dates = []
    for c in date_cols:
        s = pd.to_datetime(df[c], errors="coerce")
        dates.append(s.dt.date)

    all_dates = pd.Series(pd.concat([d.dropna() for d in dates], ignore_index=True)).dropna().unique()

    dim_date = pd.DataFrame({"date": pd.to_datetime(all_dates)})
    dim_date["date_id"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.dayofweek + 1  # Mon=1..Sun=7
    dim_date["day_name"] = dim_date["date"].dt.day_name()
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date = dim_date.sort_values("date").reset_index(drop=True)

    out_key = "gold/dimensions/dim_date.csv"
    csv_bytes = dim_date.to_csv(index=False).encode("utf-8")

    hook.load_bytes(
        bytes_data=csv_bytes,
        key=out_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(f"Loaded dim_date rows={len(dim_date)} from {chosen_key} -> s3://{bucket_name}/{out_key}")