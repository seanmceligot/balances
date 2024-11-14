import streamlit as st
import polars as pl
from pathlib import Path
import polarsutil as pu
from typing import Optional

def csv_view(infile: Path, meta: Optional[Path] = None):
    # Read CSV using polars
    df: pl.DataFrame = pu.read_csv(infile, meta)
    return df

def main():
    st.title("BANK A")
    st.write("Bank A  Report.")

    # File uploader to allow the user to upload a CSV file
    #
    uploaded_file = "2022-08-24-atmos-2023-01-26.csv"
    meta_file = "2022-08-24-atmos-2023-01-26.json"

    if uploaded_file is not None:
        # If the user uploads a CSV file, read and display it
        infile_path = Path(uploaded_file)
        meta_path = Path(meta_file) if meta_file else None

        df = csv_view(infile_path, meta_path)

        # Display the dataframe
        st.write("### Bank A:")
        st.dataframe(df.to_pandas())  # Convert to pandas for compatibility with Streamlit

        # Option to download the dataframe as a CSV
        csv = df.write_csv()
        st.download_button(
            label="Download Bank A CSV", data=csv, file_name="output.csv", mime="text/csv"
        )

if __name__ == "__main__":
    main()
