# README.md for "Balances" - Rust Program

## Overview

"Balances" is a Rust Polars program to process and combine financial data from multiple CSV files into a single tab-separated summary.

## Features

* **Read Multiple CSV Files**: Processes a list of CSV files specified in `balances.txt`.
* **Consistent Data Format**: Expects each CSV file to have headers "Date", "Amount", and "Account Name".
* **Cumulative Total Calculation**: Adds a "Total" column to the output, showing the cumulative amount from top to bottom.
* **Output in Tab-Separated Format**: Generates a combined file `balances.csv`, using tab-separated values.

## Prerequisites

* Rust programming environment
* Polars library for Rust

## File Format

Each input CSV file should follow this format:

```csv
"Date","Amount","Account Name"
```

Example:

```csv
2023-12-19,1234.56,Bank of Example
```

The program combines these files into `balances.csv` (tab-separated) with an additional "Total" column.

## Usage

1. **Prepare `balances.txt`**: List the paths of CSV files to be processed.
2. **Run the Program**: Execute the program to process the files and generate `balances.csv`.

## Functionality

* `read_filenames_from_file`: Reads the list of filenames from `balances.txt`.
* `newest_balance`: Processes each CSV file, ensuring the data schema matches and sorts by date.
* `main`: Orchestrates the reading, processing, and combining of data from all files.

## Output

* The output is a tab-separated file named `balances.csv` with an added "Total" column representing the cumulative sum of the "Amount" column.
