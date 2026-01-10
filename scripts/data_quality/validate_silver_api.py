import pandas as pd

file_path = "/data/silver/api/api_clean.parquet"
df = pd.read_parquet(file_path)
print(df.head())