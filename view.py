import polarsutil as pu
import polars as pl
import argparse
from icecream import ic
from pathlib import Path
from typing import Optional
from sys import stdout
from rich.console import Console
from io import StringIO

console = Console()

# Function to handle the main task
def csv_view(infile: Path, meta: Optional[Path]):
    # Read CSV using polars

    df: pl.DataFrame = pu.read_csv(infile, meta)
    console.print(df)

    stream = StringIO()
    df.write_csv(stream)
    console.print(stream.getvalue())


def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV to Parquet and ensure 'Date' column is parsed as date."
    )
    parser.add_argument("infile", type=str, help="Input CSV file path")
    parser.add_argument("--meta", type=str, required=False, help="meta file path")

    args = parser.parse_args()
    meta = Path(args.meta) if args.meta else None

    csv_view(Path(args.infile), meta)


def print_to_stdout(s):
    print(s, end="")


if __name__ == "__main__":
    main()
