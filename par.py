

"""
This script converts a CSV file into a Parquet file with Zstandard compression.  
It reads the input CSV, skipping any malformed lines, and writes the output  
without preserving the index. The script takes a required `--path` argument  
(without extension), assumes the input is `<path>.csv`, and produces `<path>.parquet`.  
On completion, it prints the number of rows saved and the output path.  
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import os

parser = argparse.ArgumentParser(description="Fast CSV → Parquet")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file")
args = parser.parse_args()
args.path = os.path.splitext(args.path)[0]

input_path = f"{args.path}.csv"
output_path = f"{args.path}.parquet"

df = pd.read_csv(input_path, on_bad_lines="skip", engine="python")

table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(table, output_path, compression="zstd")
print(f"Saved Parquet with {len(df):,} rows → {output_path}")
