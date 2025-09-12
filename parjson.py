#!/usr/bin/env python3

"""
Generate a Hugging Face–style dataset_infos.json from a Parquet file.

If -o/--out is provided and non-empty, JSON is written there; otherwise defaults to 'dataset_infos.json'
in the current directory. Reads Parquet footer/schema only (no full data load), derives row count, size,
and a primary column, then prompts before overwriting an existing output file.
"""

import os
import json
import argparse
from typing import Tuple

from rich.console import Console
from rich.table import Table
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.theme import Theme

console = Console(theme=Theme({"path": "cyan", "ok": "bold green", "warn": "bold yellow", "err": "bold red"}))


def resolve_out_path(in_path: str, out_path: str | None) -> str:
    """
    Return the chosen output path; if -o is omitted/blank, default to '<dir_of_-p>/dataset_infos.json'.
    Key logic: prefer non-empty -o; otherwise join the directory of the input path with the default filename.
    Allowances: accepts relative/absolute paths; input may point to any directory.
    """
    if out_path is not None and out_path.strip() != "":
        return out_path
    base_dir = os.path.dirname(os.path.abspath(in_path))
    return os.path.join(base_dir, "dataset_infos.json")


def confirm_overwrite(path: str) -> bool:
    """
    Return True to proceed when target doesn't exist or when user confirms overwrite (default YES).
    Key logic: prompt with 'Y/n' only if the path exists; treat empty input or anything not starting with 'n' as consent.
    Allowances: trims whitespace; accepts 'n' or 'no' (any case) to decline.
    """
    if not os.path.exists(path):
        return True
    console.print(f"[warn]Output already exists:[/warn] [path]{path}[/path]")
    resp = input("Overwrite? Y/n: ").strip().lower()
    return not (resp.startswith("n"))


def read_parquet_metadata(parquet_path: str) -> Tuple[int, int, str]:
    """
    Return (num_rows, size_bytes, primary_column) by reading Parquet footer/schema without loading data.
    Key logic: use pyarrow.ParquetFile to get metadata and schema; pick first column as primary if multiple.
    Allowances: if only one column exists, use it; otherwise default to the first name.
    """
    try:
        import pyarrow.parquet as pq  # type: ignore
        import pyarrow as pa  # noqa: F401
    except ImportError:
        console.print("[err]Missing dependency:[/err] pyarrow\nInstall: pip install pyarrow rich")
        raise SystemExit(1)

    if not os.path.exists(parquet_path):
        console.print(f"[err]File not found:[/err] [path]{parquet_path}[/path]")
        raise SystemExit(1)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]Reading Parquet metadata[/bold]…"),
        transient=True,
        console=console,
    ) as progress:
        task = progress.add_task("meta", total=None)
        pq_file = pq.ParquetFile(parquet_path)
        metadata = pq_file.metadata
        num_rows = metadata.num_rows
        size_bytes = os.path.getsize(parquet_path)
        schema = pq_file.schema_arrow
        col_names = schema.names
        column_name = col_names[0] if len(col_names) >= 1 else "text"
        progress.update(task, advance=1)

    return num_rows, size_bytes, column_name


def build_dataset_infos(num_rows: int, size_bytes: int, column_name: str) -> dict:
    """
    Return a dataset_infos-compatible dict with one config 'default' and split 'train'.
    Key logic: fill features with the chosen primary column as string Value; set sizes and counts from metadata.
    Allowances: leaves description/citation/homepage/license empty for later editing.
    """
    return {
        "default": {
            "description": "",
            "citation": "",
            "homepage": "",
            "license": "",
            "features": {
                column_name: {
                    "dtype": "string",
                    "_type": "Value"
                }
            },
            "splits": {
                "train": {
                    "name": "train",
                    "num_bytes": size_bytes,
                    "num_examples": num_rows,
                    "dataset_name": "default"
                }
            },
            "download_size": size_bytes,
            "dataset_size": size_bytes,
            "size_in_bytes": size_bytes
        }
    }


def write_json(data: dict, out_path: str) -> None:
    """
    Return None after writing JSON with indent=2 to out_path.
    Key logic: open with text mode and UTF-8 default, dump with 2-space indentation.
    Allowances: overwriting is guarded by a separate confirmation step.
    """
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main() -> None:
    """
    Return None; orchestrates argument parsing, metadata extraction, JSON build, overwrite guard, and summary print.
    Key logic: default output to 'dataset_infos.json' if -o is blank/omitted; show a compact Rich table summary.
    Allowances: minimal I/O; does not validate column types beyond naming first column.
    """
    parser = argparse.ArgumentParser(description="Generate dataset_infos.json from a Parquet file")
    parser.add_argument("-p", "--path", required=True, help="Path to input Parquet file")
    parser.add_argument("-o", "--out", required=False, default=None, help="Output JSON path (blank/omitted → dataset_infos.json)")
    args = parser.parse_args()

    out_path = resolve_out_path(args.path, args.out)
    num_rows, size_bytes, column_name = read_parquet_metadata(args.path)

    dataset_infos = build_dataset_infos(num_rows, size_bytes, column_name)

    if not confirm_overwrite(out_path):
        console.print("[warn]Aborted by user.[/warn]")
        raise SystemExit(2)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]Writing dataset_infos.json[/bold]…"),
        transient=True,
        console=console,
    ):
        write_json(dataset_infos, out_path)

    table = Table(title="Generated dataset_infos.json", box=box.SIMPLE_HEAVY)
    table.add_column("Field", style="bold cyan")
    table.add_column("Value", style="bold")
    table.add_row("Rows (num_examples)", f"{num_rows:,}")
    table.add_row("Size (bytes)", f"{size_bytes:,}")
    table.add_row("Column name", column_name)
    table.add_row("Config", "default")
    table.add_row("Split", "train")
    console.print(table)
    console.print(f"[ok]Wrote:[/ok] [path]{out_path}[/path]")
    console.print("[bold]Next:[/bold] Commit & push this file to your HF dataset repo root.")


if __name__ == "__main__":
    main()
