import pyarrow.parquet as pq
import pyarrow.feather as feather
import pyarrow as pa
import polarsutil as pu
from datetime import datetime
from typing import Any, Dict, List, Tuple, Set
import csv
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import argparse
import glob
import os
import json
from icecream import ic

# Ensure NLTK resources are downloaded
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Function to read Parquet file without pandas
def read_parquet(file_path: str) -> Dict[str, List[Any]]:
    table: pa.Table = pq.read_table(file_path)
    t = table.to_pydict()
    ic(t)
    return table.to_pydict()

# Function to read Feather file without pandas
def read_feather(file_path: str) -> Dict[str, List[Any]]:
    table: pa.Table = feather.read_table(file_path)
    return table.to_pydict()

# Function to read Avro file without pandas
def read_avro(file_path: str) -> Dict[str, List[Any]]:
    import fastavro
    records: List[Dict[str, Any]] = []
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            records.append(record)
    # Convert list of dicts to dict of lists
    if not records:
        return {}
    keys = records[0].keys()
    result: Dict[str, List[Any]] = {key: [] for key in keys}
    for record in records:
        for key in keys:
            result[key].append(record[key])
    return result

# Function to read CSV file without pandas
def read_csv(file_path: str, customizations: Dict[str, Any] = None) -> Dict[str, List[Any]]:
    result: Dict[str, List[Any]] = {}
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for key, value in row.items():
                if key not in result:
                    result[key] = []
                result[key].append(value)
    # Apply customizations if provided
    if customizations:
        cols = customizations.get('cols', {})
        for old_key, new_key in cols.items():
            if old_key in result:
                result[new_key] = result.pop(old_key)
    return result

# Generic function to read a file based on its extension
def read_file(file_path: str) -> Dict[str, List[Any]]:
    _, ext = os.path.splitext(file_path)
    if ext == '.csv':
        return pu.read_csv(file_path)
    elif ext == '.parquet':
        return read_parquet(file_path)
    elif ext == '.feather':
        return read_feather(file_path)
    elif ext == '.avro':
        return read_avro(file_path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

# Function to check if required columns exist
def check_required_columns(transactions: Dict[str, List[Any]], file_path: str) -> None:
    required_columns = {'Date', 'Amount', 'Description'}
    missing_columns = required_columns - transactions.keys()
    if missing_columns:
        print(f"Error: The file '{file_path}' is missing the following required columns: {', '.join(missing_columns)}")
        print(f"Expected columns: {', '.join(required_columns)}")
        exit(1)

# Normalize amounts by removing cents
def normalize_amounts(transactions: Dict[str, List[Any]], amount_field: str) -> None:
    amounts: List[Any] = transactions[amount_field]
    transactions[amount_field] = [
        int(float(amount)) if isinstance(amount, (int, float, str)) else amount for amount in amounts
    ]

# Normalize descriptions using NLTK
def normalize_descriptions(transactions: Dict[str, List[Any]], description_field: str) -> None:
    descriptions: List[str] = transactions[description_field]
    stop_words_set: Set[str] = set(stopwords.words('english'))
    ps = PorterStemmer()
    normalized_descriptions: List[str] = []
    for desc in descriptions:
        words: List[str] = re.findall(r'\b\w+\b', desc.lower())
        significant_words: List[str] = [
            ps.stem(word) for word in words if word not in stop_words_set
        ]
        normalized_desc: str = ' '.join(significant_words)
        normalized_descriptions.append(normalized_desc)
    transactions[description_field] = normalized_descriptions

# Create sets of transactions represented as tuples for comparison
def create_transaction_set(
    transactions: Dict[str, List[Any]],
    fields: List[str]
) -> Set[Tuple[Any, ...]]:
    transaction_set: Set[Tuple[Any, ...]] = set()
    num_transactions: int = len(transactions[fields[0]])
    for i in range(num_transactions):
        transaction: Tuple[Any, ...] = tuple(transactions[field][i] for field in fields)
        transaction_set.add(transaction)
    return transaction_set

# Function to normalize and prepare data for comparison
def prepare_transactions(transactions: Dict[str, List[Any]], customizations: Dict[str, Any]) -> Dict[str, List[Any]]:
    check_required_columns(transactions, 'Input File')
    normalize_amounts(transactions, "Amount")
    normalize_descriptions(transactions, "Description")
    return transactions

# Function to compare transactions and return match percentage
def compare_transactions(trans1: Dict[str, List[Any]], trans2: Dict[str, List[Any]]) -> float:
    fields: List[str] = ['Date', 'Description', 'Amount']
    set1: Set[Tuple[Any, ...]] = create_transaction_set(trans1, fields)
    set2: Set[Tuple[Any, ...]] = create_transaction_set(trans2, fields)
    matching_transactions: Set[Tuple[Any, ...]] = set1 & set2
    total_transactions: int = len(set1.union(set2))
    if total_transactions == 0:
        return 0.0
    return (len(matching_transactions) / total_transactions) * 100

# Main function
def main():
    parser = argparse.ArgumentParser(description="Find the best match for a bank transactions file among a set of comparison files.")
    parser.add_argument('--in-file', required=True, help="Input file to compare (e.g., file.csv)")
    parser.add_argument('--compare', required=True, help="Pattern to match comparison files (e.g., 'bank1/*.parquet')")
    args = parser.parse_args()

    # Read the input file
    input_transactions = read_file(args.in_file)
    customizations = read_customizations(args.in_file)
    check_required_columns(input_transactions, args.in_file)
    prepare_transactions(input_transactions, customizations)

    # Find all comparison files based on the provided pattern
    compare_files = glob.glob(args.compare)
    if not compare_files:
        print(f"No files found for pattern: {args.compare}")
        return

    # Compare input file against all comparison files and find the best match
    best_match = None
    best_match_percentage = -1.0

    for compare_file in compare_files:
        print(f"Comparing with {compare_file}...")
        compare_transactions_data = read_file(compare_file)
        compare_customizations = read_customizations(compare_file)
        check_required_columns(compare_transactions_data, compare_file)
        prepare_transactions(compare_transactions_data, compare_customizations)
        match_percentage = compare_transactions(input_transactions, compare_transactions_data)
        print(f"Match percentage with {compare_file}: {match_percentage:.2f}%")

        if match_percentage > best_match_percentage:
            best_match_percentage = match_percentage
            best_match = compare_file

    # Output the best match result
    if best_match is not None:
        print(f"Best match: {best_match} with {best_match_percentage:.2f}% overlap")
    else:
        print("No matches found.")

if __name__ == "__main__":
    main()
