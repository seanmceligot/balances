run:
	cargo clippy
	cargo run --bin balances

test_import:
	ruff check import.py
	./import.sh

test_view:
	python view.py 2022-08-24-atmos-2023-01-26.csv

stream:
	streamlit run stream.py


