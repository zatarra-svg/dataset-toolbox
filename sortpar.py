#!/usr/bin/env python3

"""
Rank a Parquet dataset by dialogue turns and character count.

Return condition:
    - Writes a new Parquet file `<stem>_sort.parquet` sorted by a composite score
    - Prints the output path when complete

Key logic:
    - Compute `char_count` for each row from the `text` column
    - Derive a capped character-based bonus (0–20) scaled between the median and 95th percentile
    - Define `effective_turns` = max(turns, 5 + char_bonus) if turns > 5, capped at 25
    - Calculate a `sort_score` = effective_turns * 1,000,000 + char_count
    - Sort rows by `sort_score` descending

Allowances:
    - Drops helper columns (`char_bonus`, `effective_turns`, `sort_score`, `__index_level_0__`) before saving
    - Input must be a Parquet file with at least `text` and `turns` columns
    - Output is compressed with Zstandard
"""


import polars as pl
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Rank dataset by turns and character count")
parser.add_argument("-p", "--in", dest="in_path", required=True, help="Path to input Parquet file")
args = parser.parse_args()

in_path = Path(args.in_path)
out_path = in_path.with_name(in_path.stem + "_sort.parquet")

# Read Parquet
df = pl.read_parquet(in_path)

# Add character count
df = df.with_columns(pl.col("text").str.len_chars().alias("char_count"))

# Robust scale for character bonus: median..95th pct mapped to 0..20 (clipped)
q = df.select(
    pl.col("char_count").quantile(0.50).alias("p50"),
    pl.col("char_count").quantile(0.95).alias("p95"),
).row(0)
p50, p95 = int(q[0]), int(q[1])
den = max(p95 - p50, 1)

# Compute a capped "extra turns" from chars: 0..20
df = df.with_columns(
    (((pl.col("char_count") - p50) / den)
       .clip(0, 1) * 20)
    .floor()
    .cast(pl.Int32)
    .alias("char_bonus")
)

# Effective turn rank:
df = df.with_columns(
    pl.when(pl.col("turns") > 5)
      .then(
          pl.max_horizontal(
              pl.col("turns"),
              (5 + pl.col("char_bonus")).clip(upper_bound=25)
          )
      )
      .otherwise(pl.col("turns"))
      .alias("effective_turns")
)

# Final score: effective_turns then char_count
df = df.with_columns(
    (pl.col("effective_turns").cast(pl.Int32) * 1_000_000
     + pl.col("char_count")).alias("sort_score")
)

df = df.sort("sort_score", descending=True)

# Cleanup
drop_cols = ["char_bonus", "effective_turns", "sort_score"]
drop_cols = [c for c in drop_cols if c in df.columns]
if "__index_level_0__" in df.columns:
    drop_cols.append("__index_level_0__")
if drop_cols:
    df = df.drop(drop_cols)

# Write back with auto-named *_sort.parquet
df.write_parquet(out_path, compression="zstd")
print(f"Written sorted dataset to {out_path}")
