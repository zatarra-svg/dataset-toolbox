#!/usr/bin/env python3

'''
This script computes token counts and basic statistics for a CSV dataset.  
It reads rows with a `text` column, tokenizes them in batches using a specified  
Hugging Face tokenizer, and writes an output CSV with the following columns:  

- text: Original text from the input CSV  
- tokens: Number of tokens (per tokenizer)  
- turns: Number of dialogue turns (user + assistant markers)  
- assistant_turns: Number of assistant turns (if provided or inferred)  
- characters: Character count of the text  
- words: Word count of the text  

Arguments:  
- `-p / --path`: Input CSV path (with or without `.csv`)  
- `-m / --tokenizer`: Hugging Face tokenizer to use (default: NousResearch/Hermes-3-Llama-3.1-8B)  
- `-b / --batch-size`: Batch size for tokenization (default: 1024)  
- `--max-rows`: Optional cap on number of rows to process (0 = all)  

The script displays a Rich progress bar, processes the CSV in batches,  
and produces a `<path>_stats.csv` file with the computed statistics.
'''

import csv
import os
import argparse
from typing import List, Dict

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from transformers import AutoTokenizer

# ----------------------------
# Args
# ----------------------------
parser = argparse.ArgumentParser(description="Compute token counts and basic stats for a CSV.")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file (with or without .csv)")
parser.add_argument(
    "-m", "--tokenizer",
    default="NousResearch/Hermes-3-Llama-3.1-8B",
    help="HF tokenizer to use (default: NousResearch/Hermes-3-Llama-3.1-8B)"
)
parser.add_argument(
    "-b", "--batch-size",
    type=int, default=1024,
    help="Batch size for tokenization (default: 1024)"
)
parser.add_argument(
    "--max-rows",
    type=int, default=0,
    help="Optional cap on number of rows to process (0 = all)"
)
args = parser.parse_args()

# Normalize paths
base = os.path.splitext(args.path)[0]
src = f"{base}.csv"
dst = f"{base}_stats.csv"

# Output header
FINAL_HEADER = ["text", "tokens", "turns", "assistant_turns", "characters", "words"]

# ----------------------------
# Count rows for progress bar
# ----------------------------
with open(src, "r", encoding="utf-8", newline="") as f:
    total_lines = sum(1 for _ in f) - 1  # minus header

if args.max_rows and args.max_rows > 0:
    total_target = min(total_lines, args.max_rows)
else:
    total_target = total_lines

# ----------------------------
# Load tokenizer once
# ----------------------------
tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, use_fast=True)

# ----------------------------
# Helpers
# ----------------------------
def count_turns(text: str) -> int:
    if not text:
        return 0
    # Count user + assistant starts, ignore system
    return text.count("<|im_start|>user") + text.count("<|im_start|>assistant")

def count_assistant_turns(text: str) -> int:
    if not text:
        return 0
    return text.count("<|im_start|>assistant")

def tokenize_lengths(texts: List[str]) -> List[int]:
    """Return token counts for each text using batch tokenization."""
    enc = tokenizer(
        texts,
        add_special_tokens=False,
        return_length=True,
        padding=False,
        truncation=False,
    )
    # Fast tokenizer returns 'length' per item
    # If missing, fall back to computing via ids length
    lengths = enc.get("length")
    if lengths is None:
        ids = enc["input_ids"]
        lengths = [len(x) for x in ids]
    return lengths

def flush_batch(batch_rows: List[Dict[str, str]], writer: csv.writer):
    """Tokenize the current batch and write rows with computed stats."""
    if not batch_rows:
        return
    texts = [r["text"] for r in batch_rows]
    token_counts = tokenize_lengths(texts)

    for r, tok in zip(batch_rows, token_counts):
        text_val = r["text"]
        # Prefer 'turn_count' -> 'turns'; fallback to any existing 'turns'; else empty
        turns_val = count_turns(text_val)
        assistant_turns_val = r.get("assistant_turns", "")

        writer.writerow([
            text_val,
            tok,
            turns_val,
            assistant_turns_val,
            len(text_val),
            len(text_val.split())
        ])

# ----------------------------
# Main
# ----------------------------
with open(src, "r", encoding="utf-8", newline="") as fin, \
     open(dst, "w", encoding="utf-8", newline="") as fout, \
     Progress(
         TextColumn("[bold cyan]Tokenizing & Writing"),
         BarColumn(),
         TextColumn("{task.completed}/{task.total}"),
         TimeElapsedColumn(),
     ) as progress:

    reader = csv.DictReader(fin)
    writer = csv.writer(fout)
    writer.writerow(FINAL_HEADER)

    task = progress.add_task("Processing", total=total_target)

    batch = []
    processed = 0
    bs = max(1, args.batch_size)

    for row in reader:
        if args.max_rows and processed >= args.max_rows:
            break

        # Require 'text' column
        text_val = row.get("text")
        if text_val is None:
            # Skip rows without text
            progress.update(task, advance=1)
            continue

        batch.append({
            "text": text_val,
            "assistant_turns": row.get("assistant_turns", ""),
            "turn_count": row.get("turn_count", row.get("turns", "")),
        })

        # Flush when batch is full
        if len(batch) >= bs:
            flush_batch(batch, writer)
            processed += len(batch)
            progress.update(task, advance=len(batch))
            batch.clear()

    # Flush any remainder
    if batch:
        flush_batch(batch, writer)
        processed += len(batch)
        progress.update(task, advance=len(batch))
