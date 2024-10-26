import polarsutil as pu
import argparse
from icecream import ic
from pathlib import Path


# Function to handle the main task
def csv_to_parquet(infile, name):
    # Read CSV using polars
    df = pu.read_csv(infile)
    ic(df)
    # ic(df.filter(~pl.col("Date").str.contains("20")))

    # Convert 'Date' column to Date type (without time)

    ic(df)
    # df = df.with_columns(pl.col('Date').dt.date())
    # ic(df)
    # df = df.with_columns(pl.col('Date').str.strptime(pl.Date, fmt='%Y-%m-%d'))

    # Get min and max date from the 'Date' column
    min_date = df["Date"].min().strftime("%Y-%m-%d")
    max_date = df["Date"].max().strftime("%Y-%m-%d")

    # Construct the output file name
    outfile = f"{min_date}-{name}-{max_date}.parquet"

    # Write to Parquet format
    df.write_parquet(outfile)

    print(f"Data successfully saved to: {outfile}")


# Argument parser setup
def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV to Parquet and ensure 'Date' column is parsed as date."
    )
    parser.add_argument("infile", type=str, help="Input CSV file path")
    parser.add_argument(
        "--name", type=str, required=True, help="Name for the output file"
    )

    args = parser.parse_args()

    # Convert CSV to Parquet with appropriate name
    csv_to_parquet(Path(args.infile), args.name)


if __name__ == "__main__":
    main()
