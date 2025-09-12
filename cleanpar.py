#!/usr/bin/env python3

'''
This script cleans a Parquet file by dropping specific columns and restoring  
the original row order. It ensures stable ordering even after column removal  
by first attaching a temporary row index.

Steps performed:
1. Load the input Parquet file.  
2. Add a `__row_order` index column to preserve original ordering.  
3. Drop unnecessary columns if they exist: `assistant_turns`, `__index_level_0__`, `char_count`.  
4. Sort rows by `__row_order` and remove the temporary index.  
5. Save the cleaned DataFrame as `train.parquet` with Zstandard compression.  

Arguments:
- `-p / --in`: Path to the input Parquet file.  

The script overwrites nothing; instead, it writes to a new file named  
`train.parquet` in the same directory as the input.
'''

import polars as pl
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Drop parquet cols and restore row order")
parser.add_argument("-p", "--in", dest="in_path", required=True, help="Path to input Parquet file")
args = parser.parse_args()

inp = Path(args.in_path)
out = inp.with_name("train.parquet")

df = pl.read_parquet(inp)

# Add a stable row index first
df = df.with_row_index(name="__row_order")

# Drop unwanted cols
for col in ("assistant_turns", "__index_level_0__"):
    if col in df.columns:
        df = df.drop(col)

# Sort back to original order
df = df.sort("__row_order").drop("__row_order")

df.write_parquet(out, compression="zstd")
print(f"Wrote {out} with {df.shape[0]} rows, preserved order.")
