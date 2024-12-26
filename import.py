from pathlib import Path
import polarsutil as pu
from typing import Optional
from rich import print
import pdb  ## noqa: F401
import argparse
#from rich.console import Console
#console = Console()

def write_meta(csv_path: Path):
    out_meta_path = csv_path.with_suffix(".json")
    # Create the config
    meta = pu.Meta()
    meta.add_column("Date", "Date", "%Y-%m-%d")
    meta.add_column("Amount", "Amount")
    meta.add_column("Description", "Description")
    meta.write(out_meta_path)


def csv_import(infile: Path, name: str, meta: Optional[Path]):
    # Read CSV using polars
    df = pu.read_csv(infile, meta)
    print(df)

    # Construct the output file name
    out_file_name = Path(f"{name}.csv")
    out_dir = Path(Path.home(), "accounts")
    out_path = Path(out_dir, out_file_name)
    
    print(out_path)
    print(out_dir.stat())

    # Check if output directory exists
    if not out_dir.exists():
        print(f"ERROR: create {out_dir} and run again")
        return
    
    # Merge with existing file if it exists
    if out_path.exists():
        existing_df = pu.read_csv(out_path)
        print(existing_df)
        
        # Perform an outer join to merge both DataFrames on all columns
        cols = set(df.columns)
        df = existing_df.vstack(df).sort("Date", descending=True).unique()
        df = df.sort("Date", descending=True)

        #df = existing_df.join(df, on=["Date","Description","Amount"], how="outer")
        assert set(df.columns) == cols, df.columns
        pu.print_csv(df)
    
    # Save the updated DataFrame to the output file
    df.write_csv(out_path)
    print(f"Data saved to: {out_path}")
    write_meta(out_path)
    
# Argument parser setup
def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV to Parquet and ensure 'Date' column is parsed as date."
    )
    parser.add_argument("infile", type=str, help="Input CSV file path")
    parser.add_argument("--meta", type=str, required=False, help="meta file path")
    parser.add_argument(
        "--name", type=str, required=True, help="Name for the output file"
    )

    args = parser.parse_args()
    meta = Path(args.meta) if args.meta else None
    infile = Path(args.infile)
    name = args.name
    csv_import(infile, name, meta)


if __name__ == "__main__":
    main()
