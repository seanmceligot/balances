#!/bin/bash
py=/home/sean/git/python/venv/bin/python

#pip install -r requirements.txt
#pq --input-format csv /home/sean/drive/files/accounting/atmos/transactions.csv --output-format parquet -o atmos_2022_08.parquet
infile=2022-08-24-atmos-2023-01-26.parquet
compare=mint-transactions.csv
#infile=a.csv
#compare=b.csv

#atmos_csv.py: error: the following arguments are required: infile, --name
$py find_overlap.py --in-file ${infile} --compare ${compare}
