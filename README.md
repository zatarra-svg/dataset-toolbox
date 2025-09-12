# datasets-toolbox

A small toolbox for preparing and analyzing conversational datasets. Includes utilities for CSV ↔ Parquet conversion, dataset statistics, and batched chain insertion into PostgreSQL.

---

## Features

### `stats.py`

Compute token counts and basic stats for a CSV dataset.

-   Counts tokens, turns, assistant turns, characters, and words.
-   Uses any Hugging Face tokenizer (default: `NousResearch/Hermes-3-Llama-3.1-8B`).
-   Processes in batches with a Rich progress bar.

**Usage:**

```bash
python stats.py -p mydata.csv -m NousResearch/Hermes-3-Llama-3.1-8B -b 1024
```

```
Output → `mydata_stats.csv`
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

### `chains.sh`

Batch insert reply chains into PostgreSQL.

-   Uses recursive CTEs to build two-author chains.
-   Deduplicates messages, merges same-author turns.
-   Tracks batch progress in `root_id_progress`.
-   Inserts results into a `chains` table in ChatML format.

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

Here are the new parts for your README, in the **exact same format** as the rest so you can copy-paste them in:

---

### `dropcols.py`

Remove identifier columns from a CSV quickly using Polars.

-   Drops `id`, `guild_id`, and `channel_id` if present.
-   Skips malformed rows with `ignore_errors=True`.
-   Writes a new `_pure.csv` file alongside the input.

**Usage:**

```bash
python dropcols.py -p mydata.csv
```

**Output:**

```text
Saved without id, guild_id, and channel_id → mydata_pure.csv
```

---

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

---

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

### `turns.py`

Compute token/turn statistics and filter rows by ChatML message-block count.

-   Counts occurrences of `<|im_start|>` per row to estimate dialogue turns.
-   Filters rows with counts within `[--min, --max]` inclusive.
-   Saves a filtered CSV and a histogram PNG.
-   Exits with error if the input CSV lacks a `text` column.

**Usage:**

```bash
python turns.py -p mydata.csv --min 2 --max 8
```

**Output:**

```text
Filtered rows (>= 2 message blocks): 15,392
Saved to: mydata2to8.csv
Histogram saved to: mydata_turn_hist.png
```

---

## Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

**requirements.txt**

```txt
pandas
polars
pyarrow
rich
transformers
```

---

| File                | Purpose                                                                                                                                             | Example Usage                                                                                      |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **`chains.sh`**     | Batch insert reply chains into PostgreSQL using recursive CTEs. Deduplicates, merges turns, and writes to `chains` table.                           | `chmod +x chains.sh && ./chains.sh` (**Edit `PGUSER` and `DB` inside script.**)                    |
| **`stats.py`**      | Compute token counts and dataset statistics (tokens, turns, chars, words). Uses Hugging Face tokenizer with batch processing and Rich progress bar. | `python stats.py -p mydata.csv -m NousResearch/Hermes-3-Llama-3.1-8B -b 1024` → `mydata_stats.csv` |
| **`par.py`**        | Convert CSV → Parquet with Zstandard compression. Skips malformed lines, prompts before overwrite.                                                  | `python par.py -p mydata.csv -o mydata.parquet`                                                    |
| **`parjson.py`**    | Generate Hugging Face–style `dataset_infos.json` from a Parquet file. Reads metadata only (no full load).                                           | `python parjson.py -p train.parquet -o dataset_infos.json`                                         |
| **`dropcols.py`**   | Remove identifier columns (`id`, `guild_id`, `channel_id`) from a CSV. Writes a new `_pure.csv` file alongside the input.                           | `python dropcols.py -p mydata.csv` → `mydata_pure.csv`                                             |
| **`tokens.py`**     | Generate detailed token statistics for a CSV (`text` col). Computes descriptive stats, histograms, assistant blocks, and saves a log file.          | `python tokens.py -p mydata.csv` → `mydata_tokenstats.txt`                                         |
| **`combineall.py`** | Recursively combine multiple CSVs into one. Estimates safe chunksize by RAM, streams in batches, shows Rich progress bar.                           | `python combineall.py -p data_folder -o combined.csv --max-mem-gb 32`                              |
| **`turns.py`**      | Filter rows by number of ChatML `                                                                                                                   | im_start                                                                                           | ` blocks (turns). Saves filtered CSV and histogram PNG. | `python turns.py -p mydata.csv --min 2 --max 8` → `mydata2to8.csv`, `mydata_turn_hist.png` |

---

## License

This project is licensed under the terms of the MIT [license](LICENSE).
