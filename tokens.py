#!/usr/bin/env python3

'''
This script generates detailed token statistics for a CSV dataset using a Hugging Face tokenizer.  
It processes the `text` column in memory-aware batches with multiprocessing and logs results  
to a `_tokenstats.txt` file alongside console output.

Workflow:
1. Read the input CSV in chunks (`--path`) with pandas.  
2. Tokenize texts in parallel using the specified tokenizer (default: Hermes-3-Llama-3.1-8B).  
3. Collect per-sample token lengths, character counts, and word counts.  
4. Compute descriptive statistics (min, max, mean, median, std, skew, kurtosis, percentiles, histograms).  
5. Count assistant blocks (`<|im_start|>assistant` or DeepHermes markers if enabled).  
6. Write all stats and totals (tokens, assistant blocks) to a log file named `<stem>_tokenstats.txt`.  

Arguments:
- `-p / --path`: Path to input CSV (must contain a `text` column).  

Configuration flags:
- `chatml`: If True, counts ChatML-style assistant markers (`<|im_start|>assistant`).  
- `deephermes`: If True, counts DeepHermes-style assistant markers.  
- `chunksize`: Rows per pandas chunk (default: 1,000,000).  
- `batch_size`: Texts per tokenization batch (default: 500).  
- `num_workers`: Parallel workers (default: all cores minus one).  

Returns:
- Console progress with Rich (spinner, bar, % complete, elapsed time).  
- A stats log file in the same folder as the input CSV, containing full metrics and totals.  
'''


import pandas as pd
import numpy as np
from transformers import AutoTokenizer
from multiprocessing import Pool, cpu_count
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
import psutil
import os

chatml = True
deephermes = False

import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Token statistics generator")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file")
args = parser.parse_args()

csv_path = args.path
log_path = str(Path(csv_path).with_name(Path(csv_path).stem + "_tokenstats.txt"))

chunksize = 1_000_000
batch_size = 500
num_workers = max(cpu_count() - 1, 1)

settokenizer = "NousResearch/Hermes-3-Llama-3.1-8B"

tokenizer = AutoTokenizer.from_pretrained(
    settokenizer, use_fast=True
)

def show_mem(label=""):
    mem = psutil.virtual_memory()
    print(f"[{label}] RAM used: {mem.used / (1024**3):.2f} GB / {mem.total / (1024**3):.2f} GB")

def column_stats_all(lengths, texts):
    if len(lengths) == 0:
        return None

    lengths = np.array(lengths)
    stats = {
        "min": int(lengths.min()),
        "max": int(lengths.max()),
        "mean": lengths.mean(),
        "median": np.median(lengths),
        "std": lengths.std(),
        "skew": ((lengths - lengths.mean())**3).mean() / (lengths.std()**3) if lengths.std() > 0 else np.nan,
        "kurt": ((lengths - lengths.mean())**4).mean() / (lengths.std()**4) if lengths.std() > 0 else np.nan,
        "count": len(lengths),
        "sum": int(lengths.sum()),
        "99.9%": np.percentile(lengths, 99.9)
    }

    for p in range(1, 101):
        stats[f"{p}%"] = np.percentile(lengths, p)

    char_lengths = np.array([len(t) for t in texts if isinstance(t, str)])
    word_counts = np.array([len(t.split()) for t in texts if isinstance(t, str)])

    if len(char_lengths) > 0:
        stats["total_chars"] = int(char_lengths.sum())
        stats["total_words"] = int(word_counts.sum())
        stats["avg_chars"] = char_lengths.mean()
        stats["avg_words"] = word_counts.mean()
        stats["avg_chars_per_word"] = (
            char_lengths.sum() / word_counts.sum() if word_counts.sum() > 0 else np.nan
        )
        stats["avg_chars_per_sample"] = char_lengths.mean()
        stats["avg_words_per_sample"] = word_counts.mean()
        stats["tokens_per_char"] = (
            lengths.mean() / char_lengths.mean() if char_lengths.mean() > 0 else np.nan
        )

    bins = [0, 8, 16, 32, 64, 128, 256, 384, 512, 768, 1024, 2048, 4096]
    hist, bin_edges = np.histogram(lengths, bins=bins)
    for i in range(len(hist)):
        stats[f"bin_{int(bin_edges[i])}-{int(bin_edges[i+1])}"] = int(hist[i])

    return stats

def chunkify(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def tokenize_and_length(texts):
    encodings = tokenizer(texts, add_special_tokens=False)
    return [len(ids) for ids in encodings["input_ids"]]

if __name__ == "__main__":
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    show_mem("Start")
    col_names = ["text"]
    all_lengths = {"text": []}
    all_texts = {"text": []}

    with Progress(
        SpinnerColumn(),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("{task.description}")
    ) as progress:
        task = progress.add_task("Processing CSV chunks...", total=0)

        for chunk in pd.read_csv(csv_path, usecols=["text"], chunksize=chunksize):
            progress.update(task, total=progress.tasks[0].total + 1)
            for col in chunk.columns:
                texts = chunk["text"].dropna().astype(str).tolist()
                all_texts["text"].extend(texts)
                batches = list(chunkify(texts, batch_size))
                with Pool(num_workers) as pool:
                    for lengths in pool.imap_unordered(tokenize_and_length, batches, chunksize=1):
                        all_lengths["text"].extend(lengths)
            progress.advance(task)
            show_mem("Chunk processed")

    total_tokens = 0
    total_assistants = 0

    with open(log_path, "w", encoding="utf-8") as log_file:
        for col in col_names:
            stats = column_stats_all(all_lengths[col], all_texts[col])
            if stats is None:
                continue
            total_tokens += stats["sum"]

            if chatml:
                assistant_blocks = sum(t.count("<|im_start|>assistant") for t in all_texts[col])
            if deephermes:
                assistant_blocks = sum(t.count("<|start_header_id|>assistant<|end_header_id|>") for t in all_texts[col])
            total_assistants += assistant_blocks

            log_file.write(f"Stats for {col}:\n")
            print(f"Stats for {col}:")
            for k, v in stats.items():
                log_file.write(f"  {k}: {v}\n")
                print(f"  {k}: {v}")
            log_file.write(f"  assistant_blocks: {assistant_blocks}\n")
            print(f"  assistant_blocks: {assistant_blocks}")
            log_file.write("\n")
            print()

        # Final totals (now includes row count)
        log_file.write(f"Total tokens across all columns: {total_tokens}\n")
        log_file.write(f"Total assistant blocks: {total_assistants}\n")

        print(f"Total tokens across all columns: {total_tokens}")
        print(f"Total assistant blocks: {total_assistants}")
