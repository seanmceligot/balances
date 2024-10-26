import polarsutil as pu
import argparse
from icecream import ic
from pathlib import Path
from typing import Optional

# Function to handle the main task
def csv_stat(infile:Path, meta: Optional[Path]):
    # Read CSV using polars
 
    df = pu.read_csv(infile, meta)
    ic(df)

    # Get min and max date from the 'Date' column
    min_date = df['Date'].min().strftime('%Y-%m-%d')
    max_date = df['Date'].max().strftime('%Y-%m-%d')

    ic(max_date)
    ic(min_date)
    ic(df.describe())

def main():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet and ensure 'Date' column is parsed as date.")
    parser.add_argument('infile', type=str, help='Input CSV file path')
    parser.add_argument('--meta', type=str, required=False, help='meta file path')

    args = parser.parse_args()
    meta = Path(args.meta) if args.meta else None

    csv_stat(Path(args.infile), meta)

def print_to_stdout(s):
    print(s, end="")

if __name__ == "__main__":
    #ic.configureOutput(outputFunction=print_to_stdout)
    main()
