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

| File             | Purpose                                                                                                                                             | Example Usage                                                                                          |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **`chains.sh`**  | Batch insert reply chains into PostgreSQL using recursive CTEs. Deduplicates, merges turns, and writes to `chains` table.                           | `chmod +x chains.sh && chains.sh` (**Edit `PGUSER` and `DB` inside script.**)                          |
| **`stats.py`**   | Compute token counts and dataset statistics (tokens, turns, chars, words). Uses Hugging Face tokenizer with batch processing and Rich progress bar. | `python stats.py -p mydata.csv -m NousResearch/Hermes-3-Llama-3.1-8B -b 1024<br>` → `mydata_stats.csv` |
| **`par.py`**     | Convert CSV → Parquet with Zstandard compression. Skips malformed lines, prompts before overwrite.                                                  | `python par.py -p mydata.csv -o mydata.parquet<br>`                                                    |
| **`parjson.py`** | Generate Hugging Face–style `dataset_infos.json` from a Parquet file. Reads metadata only (no full load).                                           | `python parjson.py -p train.parquet -o dataset_infos.json<br>`                                         |

---

## License

This project is licensed under the terms of the MIT [license](LICENSE).
