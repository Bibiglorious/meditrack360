# /opt/airflow/scripts/etl/gold/build_dim_test_type.py

from io import BytesIO
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _read_s3_df(hook: S3Hook, bucket: str, key: str) -> pd.DataFrame:
    body = hook.get_key(key, bucket_name=bucket).get()["Body"].read()

    if key.lower().endswith(".parquet"):
        return pd.read_parquet(BytesIO(body))
    elif key.lower().endswith(".csv"):
        return pd.read_csv(BytesIO(body))
    else:
        raise ValueError(f"Unsupported file type: {key}")


def build_dim_test_type(bucket_name: str, aws_conn_id: str, silver_prefix: str = "silver/lab_results/"):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket_name=bucket_name, prefix=silver_prefix) or []

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

    if "test_type" not in df.columns:
        raise ValueError("test_type column not found in Silver lab_results")

    dim_test_type = (
        df[["test_type"]]
        .dropna()
        .drop_duplicates()
        .sort_values("test_type")
        .reset_index(drop=True)
    )
    dim_test_type["test_type_key"] = range(1, len(dim_test_type) + 1)

    out_key = "gold/dimensions/dim_test_type.csv"
    hook.load_bytes(
        bytes_data=dim_test_type.to_csv(index=False).encode("utf-8"),
        key=out_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(f"Loaded dim_test_type rows={len(dim_test_type)} from {chosen_key} -> s3://{bucket_name}/{out_key}")