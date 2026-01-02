import glob
import sys
import pandas as pd


REQUIRED_COLUMNS = [
    "lab_result_id",
    "patient_id",
    "admission_id",
    "test_type",
    "result_value",
    "collected_time",
    "completed_time",
    "result_date",
]

def main():
    paths = glob.glob("/data/silver/lab_results/*.parquet")
    if not paths:
        print(" No Silver parquet files found in /data/silver/lab_results/")
        sys.exit(1)

    df = pd.read_parquet(paths[0])
    print(f"Loaded Silver parquet: {paths[0]}")
    print(f"Rows: {len(df)} | Cols: {len(df.columns)}")

    
    if len(df) == 0:
        print("Silver dataset is empty")
        sys.exit(1)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print(f"Missing required columns: {missing}")
        sys.exit(1)

    null_patient = df["patient_id"].isna().sum()
    if null_patient > 0:
        print(f"patient_id has {null_patient} nulls")
        sys.exit(1)


    df["collected_time"] = pd.to_datetime(df["collected_time"], errors="coerce")
    df["completed_time"] = pd.to_datetime(df["completed_time"], errors="coerce")

    bad_ts = df[df["completed_time"].notna() & df["collected_time"].notna() & (df["completed_time"] < df["collected_time"])]
    if len(bad_ts) > 0:
        print(f"Found {len(bad_ts)} rows where completed_time < collected_time")
        sys.exit(1)

    print("Silver lab_results PASSED data quality checks ")
    sys.exit(0)


if __name__ == "__main__":
    main()