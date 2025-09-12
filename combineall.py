#!/usr/bin/env python3

'''
This script recursively combines all CSV files in a given folder into a single output CSV.  
It supports automatic batching based on available memory to prevent out-of-RAM crashes.  

Workflow:
1. Collect all `.csv` files under the input folder (recursive).  
2. Estimate a safe chunksize from `--max-mem-gb` using a sample read, or use `--chunksize` if provided.  
3. Stream each file in chunks with `pandas.read_csv`, appending to the output file.  
4. Display progress with Rich (files processed, elapsed time, ETA).  

Arguments:
- `-p / --path`: Input folder containing CSV files (searched recursively).  
- `-o / --output`: Path to the combined output CSV file.  
- `--max-mem-gb`: Maximum memory in GB to use for chunk estimation (default: 32).  
- `-c / --chunksize`: Override for rows per chunk (auto-estimated if not set).  

Returns:
- Writes one combined CSV at the given output path.  
- Prints progress and a success message on completion.  
'''

import os
import argparse
import pandas as pd
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn, TextColumn
import psutil
import math

console = Console()

def get_csv_files(folder):
    """Return sorted list of CSV files under folder (recursive)."""
    csvs = []
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".csv"):
                csvs.append(os.path.join(root, f))
    return sorted(csvs)

def estimate_chunksize(max_mem_gb, sample_file):
    """Estimate chunksize for pandas.read_csv based on available memory."""
    sample_rows = 50_000
    sample = pd.read_csv(sample_file, nrows=sample_rows)
    mem_usage = sample.memory_usage(deep=True).sum()
    bytes_per_row = mem_usage / sample_rows
    avail_bytes = max_mem_gb * (1024**3)
    # use ~70% of max mem for safety
    target_bytes = avail_bytes * 0.7
    return max(10_000, int(target_bytes // bytes_per_row))

def main():
    parser = argparse.ArgumentParser(description="Combine all CSVs in a folder (recursively) into one output CSV with batching for given RAM.")
    parser.add_argument("-p", "--path", required=True, help="Folder containing CSV files")
    parser.add_argument("-o", "--output", required=True, help="Output CSV file path")
    parser.add_argument("--max-mem-gb", type=float, default=32, help="Max memory to use in GB")
    parser.add_argument("-c", "--chunksize", type=int, default=None, help="Rows per read/write chunk (auto if not set)")
    args = parser.parse_args()

    files = get_csv_files(args.path)
    if not files:
        console.print(f"[red]No CSV files found in {args.path}[/red]")
        return

    if args.chunksize is None:
        args.chunksize = estimate_chunksize(args.max_mem_gb, files[0])
        console.print(f"[cyan]Auto chunksize based on {args.max_mem_gb}GB RAM: {args.chunksize} rows[/cyan]")

    console.print(f"[cyan]Found {len(files)} CSV files. Combining into {args.output}[/cyan]")

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    first_write = True

    with Progress(
        TextColumn("[bold blue]Combining[/bold blue]"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task("combine", total=len(files))

        for file in files:
            for chunk in pd.read_csv(file, chunksize=args.chunksize):
                chunk.to_csv(
                    args.output,
                    mode="w" if first_write else "a",
                    index=False,
                    header=first_write
                )
                if first_write:
                    first_write = False
            progress.advance(task)

    console.print(f"[green]Combined CSV saved to {args.output}[/green]")

if __name__ == "__main__":
    main()
