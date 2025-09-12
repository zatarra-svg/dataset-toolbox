# Dataset Toolbox

<p align="center">
  <img src="https://raw.githubusercontent.com/mookiezi/site/refs/heads/main/Dataset-Toolbox-Header-E.png" alt="Dataset Cleaning Toolkit">
</p>

A dataset toolbox for preparing and analyzing conversational datasets, including CSV splitting, CSV → Parquet conversion, dataset statistics, dialogue-turn filtering, token and turn analysis, Parquet cleaning and sorting, Hugging Face–style metadata generation, and batched chain insertion into PostgreSQL — with Rich progress, multiprocessing, and 32 GB-RAM-friendly batching. Every stage is independant and CLI-driven.

---

## Install

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
git clone https://github.com/mookiezi/dataset-toolbox
cd dataset-toolbox
pip install -U pip
pip install -r requirements.txt
```

> Python **3.10+** recommended.

---

## Features

| File                | Purpose                                                                                                                                             | Example Usage                                                                                      |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| **`chains.sh`**     | Batch insert reply chains into PostgreSQL using recursive CTEs. Deduplicates, merges turns, and writes to `chains` table.                           | `chmod +x chains.sh && ./chains.sh` (**Edit `PGUSER` and `DB` inside script.**)                  |
| **`splitcsv.py`**   | Split a CSV into N parts, writing each to `<parent>/<N>/split.csv`. Uses Polars for fast IO.                                                        | `python splitcsv.py -p data/dump.csv -s 10`-                                                       |
| **`combineall.py`** | Recursively combine multiple CSVs into one. Estimates safe chunksize by RAM, streams in batches, shows Rich progress bar.                           | `python combineall.py -p data_folder -o combined.csv --max-mem-gb 32`                              |
| **`dropcols.py`**   | Remove identifier columns (`id`, `guild_id`, `channel_id`) from a CSV. Writes a new `_pure.csv` file alongside the input.                           | `python dropcols.py -p mydata.csv` → `mydata_pure.csv`                                             |
| **`stats.py`**      | Compute token counts and dataset statistics (tokens, turns, chars, words). Uses Hugging Face tokenizer with batch processing and Rich progress bar. | `python stats.py -p mydata.csv -m NousResearch/Hermes-3-Llama-3.1-8B -b 1024` → `mydata_stats.csv` |
| **`tokens.py`**     | Generate detailed token statistics for a CSV (`text` col). Computes descriptive stats, histograms, assistant blocks, and saves a log file.          | `python tokens.py -p mydata.csv` → `mydata_tokenstats.txt`                                         |
| **`turnstats.py`**  | Generate statistics on im_start blocks (turns) from CSV. Saves distribution table (`*_turn_table.txt`) and histogram (`*_turn_hist.png`).           | `python turnstats.py -p mydata.csv`                                                                |
| **`par.py`**        | Convert CSV → Parquet with Zstandard compression. Skips malformed lines, prompts before overwrite.                                                  | `python par.py -p mydata.csv -o mydata.parquet`                                                    |
| **`sortpar.py`**    | Rank dataset by turns and character count. Computes char bonus, effective turns, and composite score to sort rows.                                  | `python sort.py -p train.parquet` → `train_sort.parquet`                                           |
| **`cleanpar.py`**   | Drop unnecessary columns (`assistant_turns`, `__index_level_0__`) from Parquet and restore row order. Writes `train.parquet`.                       | `python cleanpar.py -p mydata.parquet` → `train.parquet`                                           |
| **`parjson.py`**    | Generate Hugging Face–style `dataset_infos.json` from a Parquet file. Reads metadata only (no full load).                                           | `python parjson.py -p train.parquet -o dataset_infos.json`                                         |

---

### `chains.sh`

Batch insert reply chains into PostgreSQL.

-   Uses recursive CTEs to build two-author chains.
-   Deduplicates messages, merges same-author turns.
-   Tracks batch progress in `root_id_progress`.
-   Inserts results into a `chains` table in ChatML format.
-   Shows progress (inserts processed, elapsed time, ETA).

**Usage:**

```bash
chmod +x chains.sh
./chains.sh
```

**Output (per batch):**

```text
🚀 Starting batched chain insert runner...
Running batch 12...
📦 Finished batch in 95s
Running batch 13...
📦 Finished batch in 102s
No more batches left. Exiting.
```

> Configure `PGUSER` and `DB` at the top of the script for your environment.

## `splitcsv.py` — Split CSV into N Parts

Split a large CSV into evenly sized parts, writing each to `<parent>/<i>/split.csv`.  
Uses Polars for fast IO and Rich printing for timing.

**CLI**

```text
-p/--path              Input CSV path (with or without .csv, required)
-s/--split-count       Number of parts to split into (default: 10)
```

**Usage**

```bash
python splitcsv.py -p data/dump.csv -s 10
```

**Output**

```text
Loaded 1,234,567 rows — splitting into 20 parts
[1/20] Writing data/1/split.csv... done in 0.42s
[2/20] Writing data/2/split.csv... done in 0.37s
...
```

### `combineall.py`

Combine multiple CSV files into one large CSV safely.

-   Recursively finds all `.csv` files in a folder.
-   Estimates safe chunksize based on available RAM, or accepts `--chunksize`.
-   Streams files in chunks to avoid OOM errors.
-   Shows Rich progress (files processed, elapsed time, ETA).

**Usage:**

```bash
python combineall.py -p data_folder -o combined.csv --max-mem-gb 32
```

**Output:**

```text
Auto chunksize based on 32GB RAM: 250000 rows
Found 12 CSV files. Combining into combined.csv
Combining ██████████████ 12/12 • 00:55 • 00:00
Combined CSV saved to combined.csv
```

### `dropcols.py`

Remove identifier columns from a CSV quickly using Polars.

-   Drops `id`, `guild_id`, and `channel_id` if present.
-   Skips malformed rows with `ignore_errors=True`.
-   Writes a new `_pure.csv` file alongside the input.

**Usage:**

**Output:**

```text
Saved without id, guild_id, and channel_id → mydata_pure.csv
```

### `stats.py`

Compute token counts and basic stats for a CSV dataset.

-   Counts tokens, turns, assistant turns, characters, and words.
-   Uses any Hugging Face tokenizer (default: `NousResearch/Hermes-3-Llama-3.1-8B`).
-   Processes in batches with a Rich progress bar.

**Usage:**

```bash
python stats.py -p mydata.csv -m NousResearch/Hermes-3-Llama-3.1-8B -b 1024
```

**Output:**

```
`mydata.csv` → `mydata_stats.csv`
```

### `filterturns.py`

Compute token/turn statistics and filter rows by ChatML message-block count.

-   Counts occurrences of `<|im_start|>` per row to estimate dialogue turns.
-   Filters rows with counts within `[--min, --max]` inclusive.
-   Saves a filtered CSV and a histogram PNG.
-   Exits with error if the input CSV lacks a `text` column.

**Usage:**

```bash
python turns.py -p mydata.csv --min 2 --max 8
```

### `par.py`

Convert CSV → Parquet with Zstandard compression.

-   Skips malformed CSV lines.
-   Prompts before overwriting existing files.
-   Lightweight and fast.

**Usage:**

```bash
python par.py -p mydata.csv -o mydata.parquet
```

**Output:**

```text
[ok]Saved 176,131 rows → mydata.parquet
```

### `sortpar.py`

Rank a Parquet dataset by turns and character count.

-   Computes a `char_count` column from text length.
-   Adds a capped character bonus (0–20) scaled between median and 95th percentile.
-   Calculates an `effective_turns` value (max of turns vs. 5 + char bonus, capped at 25).
-   Creates a composite `sort_score` = effective_turns × 1,000,000 + char_count.
-   Sorts by this score and saves a new `_sort.parquet` file.

**Usage:**

```bash
python sort.py -p train.parquet
```

**Output:**

```text
Written sorted dataset to train_sort.parquet
```

### `cleanpar.py`

Clean a Parquet file by dropping extra columns and restoring original row order.

-   Drops `assistant_turns` and `__index_level_0__` if present.
-   Attaches a temporary row index to preserve input order.
-   Sorts back to the original order and removes the index.
-   Saves as `train.parquet` with Zstandard compression.

**Usage:**

```bash
python cleanpar.py -p mydata.parquet
```

**Output:**

```text
Wrote train.parquet with 176,131 rows, preserved order.
```

### `parjson.py`

Generate a Hugging Face–style `dataset_infos.json` from a Parquet file.

-   Reads Parquet footer/schema only (no full load).
-   Extracts row count, size, and primary column.
-   Prompts before overwriting.

**Usage:**

```bash
python parjson.py -p train.parquet -o dataset_infos.json
```

**Output:**

```text
Generated dataset_infos.json
──────────────────────────────────────────
Field              Value
──────────────────────────────────────────
Rows (num_examples)   N examples
Size (bytes)          N bytes
Column name           id
Config                default
Split                 train
──────────────────────────────────────────
[ok]Wrote: dataset_infos.json
```

### `tokens.py`

Generate detailed token statistics for a CSV dataset.

-   Tokenizes the `text` column in batches using Hugging Face tokenizer (default: Hermes-3-Llama-3.1-8B).
-   Computes descriptive stats (min, max, mean, median, std, skew, kurtosis, percentiles, histograms).
-   Counts assistant message blocks (ChatML or DeepHermes markers).
-   Logs results to `<stem>_tokenstats.txt`.

**Usage:**

```bash
python tokens.py -p mydata.csv
```

**Output (excerpt):**

```text
Stats for text:
  min: 3
  max: 2048
  mean: 87.3
  median: 74.0
  std: 56.1
  ...
  assistant_blocks: 25

Total tokens across all columns: 15,382,921
Total assistant blocks: 142,883
```

**Output:**

```text
Filtered rows (>= 2 message blocks): 15,392
Saved to: mydata2to8.csv
Histogram saved to: mydata_turn_hist.png
```

### `turnstats.py`

Generate message-block statistics and a histogram for a CSV dataset.

-   Counts `<|im_start|>` occurrences per row to measure dialogue turns.
-   Produces a full frequency distribution table (including zero-filled bins).
-   Saves the table to `<base>_turn_table.txt`.
-   Plots and saves a histogram as `<base>_turn_hist.png`.
-   Prints the distribution table to the console.

**Usage:**

```bash
python turnstats.py -p mydata.csv
```

**Output:**

```text
Score Distribution
───────────────────────────
Range        Count
───────────────────────────
0            153
1            841
2            392
...
───────────────────────────
Score table saved to: mydata_turn_table.txt
Histogram saved to: mydata_turn_hist.png
```

---

> ## Cross-Platform Notes

-   **Unix/macOS**: after `chmod +x script.py`, you can run `./script.py …` thanks to the shebang.
-   **Windows**: run as `python script.py …`; shebang is ignored by default shell, but code is fully supported.

---

## License

This project is licensed under the **MIT License**.  
See the [LICENSE](LICENSE) file for details.
