#!/bin/bash
set -x
infile=/home/sean/drive/files/accounting/atmos/transactions.csv 

python import.py --name atmos ${infile}
