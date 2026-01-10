import pandas as pd
from io import BytesIO
import os

# Correct path inside Docker
input_path = "/data/silver/api/api_clean.csv"
output_path = "/data/silver/api/api_clean.parquet"

df = pd.read_csv(input_path)

# Convert to Parquet
df.to_parquet(output_path, index=False)

print(f"✅ Converted {input_path} → {output_path}")