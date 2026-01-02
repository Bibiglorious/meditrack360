
from __future__ import annotations

import os
import glob
from typing import List, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _list_local_csvs(local_dir: str) -> List[str]:
    pattern = os.path.join(local_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    return files


def _upload_file_if_needed(
    hook: S3Hook,
    bucket: str,
    local_path: str,
    s3_key: str,
    replace: bool = False,
) -> str:
    

    exists = hook.check_for_key(key=s3_key, bucket_name=bucket)

    if exists and not replace:
        print(f"SKIP (already exists): s3://{bucket}/{s3_key}")
        return "skipped"

    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=bucket,
        replace=True,  
    )
    print(f"UPLOADED: {local_path} -> s3://{bucket}/{s3_key}")
    return "uploaded"


def extract_lab_results_to_bronze(
    bucket_name: str,
    aws_conn_id: str,
    local_bronze_dir: str = "/opt/airflow/data/bronze/lab_results",
    s3_prefix: str = "bronze/lab_results",
    replace: bool = False,
) -> Tuple[int, int]:
    

    print("Extract: lab_results -> S3 Bronze")
    print(f"Local dir: {local_bronze_dir}")
    print(f"S3 prefix: s3://{bucket_name}/{s3_prefix}/")
    print(f"Replace existing: {replace}")

    files = _list_local_csvs(local_bronze_dir)
    if not files:
        raise FileNotFoundError(
            f"No CSV files found in {local_bronze_dir}. "
            f"Check your docker volume mount and folder path."
        )

    hook = S3Hook(aws_conn_id=aws_conn_id)

    uploaded = 0
    skipped = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        s3_key = f"{s3_prefix}/{fname}"

        result = _upload_file_if_needed(
            hook=hook,
            bucket=bucket_name,
            local_path=fpath,
            s3_key=s3_key,
            replace=replace,
        )
        if result == "uploaded":
            uploaded += 1
        else:
            skipped += 1

    print(f"DONE. Uploaded={uploaded}, Skipped={skipped}")
    return uploaded, skipped