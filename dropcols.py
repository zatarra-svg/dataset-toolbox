#!/usr/bin/env python3

'''
This script removes identifier columns (`id`, `guild_id`, `channel_id`) from a CSV file  
and saves a new copy with the suffix `_pure.csv`. It uses Polars for fast parsing and  
writing, skipping over malformed rows if encountered.

Arguments:
- `-p / --path`: Path to the input CSV file.  

Returns:
- Writes a new CSV with the same base name plus `_pure.csv`.  
- Prints the path of the saved file after processing.  
'''


import polars as pl
import argparse
import os

parser = argparse.ArgumentParser(description="Drop guild_id and channel_id from CSV (fast)")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file")
args = parser.parse_args()

input_path = args.path
output_path = os.path.splitext(input_path)[0] + "_pure.csv"

(
    pl.read_csv(input_path, ignore_errors=True)
    .drop(["guild_id", "channel_id", "id"])
    .write_csv(output_path)
)

print(f"Saved without id, guild_id, and channel_id → {output_path}")