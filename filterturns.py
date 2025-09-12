#!/usr/bin/env python3

"""
Compute token/turn statistics and filter rows by ChatML message-block count.

Return condition:
    - Writes a filtered CSV (<base>{min}to{max}.csv)
    - Writes a histogram PNG (<base>_turn_hist.png)
    - Prints a brief summary
    - Exits non-zero if the CSV lacks a 'text' column or cannot be read

Key logic:
    - Read <base>.csv (derived from -p)
    - Count occurrences of '<|im_start|>' per row to estimate message blocks
    - Filter rows with counts within [--min, --max] inclusive
    - Plot and save a histogram using a non-interactive backend

Allowances:
    - -p may include or omit '.csv' (extension is stripped then '.csv' appended)
    - Uses matplotlib 'Agg' backend for headless environments
    - Table printing code exists but is disabled in the script
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

parser = argparse.ArgumentParser(description="Turn statistics generator")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file")
parser.add_argument("-min", "--min", type=int, required=True, help="Min turns to keep")
parser.add_argument("-max", "--max", type=int, required=True, help="Max turns to keep")
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

# === Print and save distribution table + write 7+ chains CSV ===
table = Table(title="Score Distribution", show_lines=True)
table.add_column("Range", justify="center", style="cyan")
table.add_column("Count", justify="right", style="magenta")

# === Filter and save rows with >= N message blocks ("turns") ===
# NOTE: A "message block" here means each '<|im_start|>' occurrence (user or assistant).
min_messages = args.min
max_messages = args.max
mask = (message_counts >= min_messages) & (message_counts <= max_messages)
filtered = df.loc[mask].copy()

base_dir = os.path.dirname(input_csv)
name_no_ext = os.path.splitext(os.path.basename(input_csv))[0]
filtered_output = os.path.join(base_dir, f"{name_no_ext}{min_messages}to{max_messages}.csv")

filtered.to_csv(filtered_output, index=False)

console.print(
    f"[bold]Filtered rows (>= {min_messages} message blocks):[/bold] "
    f"[bold magenta]{mask.sum()}[/bold magenta]"
)
console.print(f"[bold green]Saved to:[/bold green] {filtered_output}")

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
