#!/usr/bin/env python3

"""
Generate message-block statistics for a CSV dataset and save results as a table and histogram.

Return condition:
    - Writes a text table (<base>_turn_table.txt) with counts of message blocks per row
    - Writes a histogram PNG (<base>_turn_hist.png)
    - Prints the table to the console
    - Exits non-zero if the CSV lacks a 'text' column or cannot be read

Key logic:
    - Read <base>.csv (derived from -p)
    - Count occurrences of '<|im_start|>' per row to measure dialogue turns
    - Build a full frequency distribution across all counts (fill missing bins with 0)
    - Save the distribution table and plot a histogram with matplotlib 'Agg' backend

Allowances:
    - -p may include or omit '.csv' (extension is stripped then '.csv' appended)
    - Output files are written alongside the input
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from rich.console import Console
from rich.table import Table
import os
import argparse
import os

parser = argparse.ArgumentParser(description="Token statistics generator")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file")
args = parser.parse_args()
args.path = os.path.splitext(args.path)[0]

# === Config ===
input_csv = f"{args.path}.csv"
plot_output = f"{args.path}_turn_hist.png"
table_output = f"{args.path}_turn_table.txt"
bin_size = 0.05

# === Load CSV ===
df = pd.read_csv(input_csv)
console = Console()

if "text" not in df.columns:
    console.print("[bold red]Missing 'text' column in CSV.[/bold red]")
    exit()

# Count number of message blocks (<|im_start|>) per row
message_counts = df["text"].astype(str).apply(lambda x: x.count("<|im_start|>"))

# Bin by number of messages
bins = range(message_counts.min(), message_counts.max() + 2)  # +2 so last bin closes
counts = message_counts.value_counts().sort_index()
full_index = range(message_counts.min(), message_counts.max() + 1)
counts = counts.reindex(full_index, fill_value=0)

# === Print and save distribution table ===
table = Table(title="Score Distribution", show_lines=True)
table.add_column("Range", justify="center", style="cyan")
table.add_column("Count", justify="right", style="magenta")

with open(table_output, "w") as f:
    f.write("Score Distribution Table\n")
    f.write("------------------------\n")
    for rng, count in counts.items():
        table.add_row(str(rng), str(count))
        f.write(f"{str(rng):<18} {count}\n")

console.print(table)
console.print(f"[bold green]Score table saved to:[/bold green] {table_output}")

# === Plot histogram ===
plt.figure(figsize=(8, 4))
plt.hist(message_counts, bins=bins, color="lightgreen", edgecolor="black")
filename = os.path.splitext(os.path.basename(plot_output))[0]
plt.title(f"Message Count Histogram: {filename}")
plt.xlabel("Number of Messages (user + assistant blocks)")
plt.ylabel("Count")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(plot_output, dpi=150)
console.print(f"[bold green]Histogram saved to:[/bold green] {plot_output}")