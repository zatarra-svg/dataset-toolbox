#!/usr/bin/env python3

"""
Split a CSV into N evenly sized parts.

Return condition:
    - Writes each chunk to <parent>/<i>/split.csv (i = 1..N)
    - Prints per-part write time and summary of rows processed

Key logic:
    - Load the CSV from -p/--path (with or without .csv)
    - Compute rows_per_part = ceil(total_rows / N)
    - Slice the dataframe into N sequential chunks
    - Write each chunk into its own subfolder numbered 1..N

Allowances:
    - Creates parent directories automatically
    - Uses Polars for fast CSV reading/writing
    - Defaults to 10 parts if --split-count is not given
"""

import polars as pl
import math
import time
from rich import print
import argparse
import os

parser = argparse.ArgumentParser(description="Token statistics generator")
parser.add_argument("-p", "--path", required=True, help="folder to CSV file")
parser.add_argument("-s", "--split_count", type=int, default=10, required=True, help="folder to CSV file")
args = parser.parse_args()
args.path = os.path.splitext(args.path)[0]

folder_path = os.path.dirname(args.path)
input_path = f"{args.path}.csv"
num_parts = args.split_count

df = pl.read_csv(input_path)
total_rows = df.height
rows_per_part = math.ceil(total_rows / num_parts)

print(f"[cyan]Loaded ({total_rows} rows) — splitting into {num_parts} parts[/cyan]")

for i in range(num_parts):
    start = i * rows_per_part
    end = min(start + rows_per_part, total_rows)
    chunk = df.slice(start, end - start)
    out_path = f"{folder_path}/{i + 1}/split.csv"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)  # create parent dirs

    print(f"[bold][{i+1}/{num_parts}][/bold] Writing [green]{out_path}[/green]...", end=" ", flush=True)
    t0 = time.time()
    chunk.write_csv(out_path)
    dt = time.time() - t0
    print(f"[cyan]done in {dt:.2f}s[/cyan]")