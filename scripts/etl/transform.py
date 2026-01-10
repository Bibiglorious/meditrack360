import os
import glob
import pandas as pd
from typing import List

def transform_lab_results_to_silver(
    input_dir: str = "/opt/airflow/data/bronze/lab_results",
    output_dir: str = "/opt/airflow/data/silver/lab_results"
) -> List[str]:
    print(f"Transforming lab_results from {input_dir} to {output_dir}")

    os.makedirs(output_dir, exist_ok=True)

    input_files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    if not input_files:
        raise FileNotFoundError(f"No files found in {input_dir}")

    output_files = []

    for fpath in input_files:
        df = pd.read_csv(fpath)

        # Basic cleaning
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        df = df.dropna()
        df = df.drop_duplicates()

        # Save cleaned version
        fname = os.path.basename(fpath)
        out_path = os.path.join(output_dir, fname)
        df.to_csv(out_path, index=False)
        output_files.append(out_path)

        print(f"Transformed: {fname}")

    print(f"Done. Total files: {len(output_files)}")
    return output_files