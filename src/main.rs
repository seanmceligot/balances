use polars::prelude::*;
use std::fs::File;
use std::fs::read_to_string;
use anyhow::Result;
use std::path::Path;
use polars::chunked_array::ops::SortOptions; 
use std::sync::Arc;
use std::collections::BTreeMap;
use std::path::PathBuf;

static ACCOUNT_NAME: &str = "Account Name";
static AMOUNT: &str = "Amount";
static DATE: &str = "Date";
static TOTAL: &str = "Total";

// balances.txt is a list of CSV filenames or paths.
// each CSV with is expected to have the following header
//
// "Date","Amount","Account Name"
// ┌────────────┬───────────┬──────────────────────────┐
// │ Date       ┆ Amount    ┆ Account Name             │
// │ ---        ┆ ---       ┆ ---                      │
// │ str        ┆ f64       ┆ str                      │
// ╞════════════╪═══════════╪══════════════════════════╡
// │ 2023-12-19 ┆ 1234.56   ┆ Bank of Example          │
// └────────────┴───────────┴──────────────────────────┘
//
// they will be combined into balances.csv (which is actually tab separated)
//
// Date	Amount	Account Name	Total
//
// Total is cummulative from top to bottom 
//
fn read_filenames_from_file(filenames_file: &str) -> Vec<PathBuf> {
    match read_to_string(filenames_file) {
        Ok(contents) => contents
            .lines()
            .map(|line| PathBuf::from(line.trim()))
            .collect(),
        Err(error) => panic!("Error reading file: {}", error),
    }
}

fn newest_balance(filename: &PathBuf) -> Result<LazyFrame> {
    let fields = vec![
        ArrowField::new(DATE, ArrowDataType::Date32, false),
        ArrowField::new(AMOUNT, ArrowDataType::Float64, false),
        ArrowField::new(ACCOUNT_NAME, ArrowDataType::Utf8, false),
    ];

    let metadata: BTreeMap<String, String> = BTreeMap::new();
    let arrow_schema = ArrowSchema {fields, metadata};
    let polars_schema = Schema::from(Arc::new(arrow_schema));
    println!("read csv: {:?}", filename);
    let df = LazyCsvReader::new(Path::new(filename))
        .has_header(true)
        .with_schema(Some(Arc::new(polars_schema)))
        .finish()?;
    let sorted: LazyFrame = df.sort(DATE, SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: true,
                maintain_order: true,
            });
    Ok(sorted.last())
}
fn main() -> Result<()> {

    let filenames = read_filenames_from_file("balances.txt");

    let mut dataframes = Vec::new();
   
    // commented out code does a comparision to check for errors, but the schema may be enough.
    //let mut last: Option<DataFrame> = None;
    for filename in filenames.iter() {
        let next_lf = newest_balance(filename)?;
        //let next_df = next_lf.clone().fetch(1)?;
        //if let Some(last_df) = last { 
        //        last_df.frame_equal_schema(&next_df)? 
        //}
        dataframes.push(next_lf);
        //last = Some(next_df);
    }

    let uargs =UnionArgs {
    parallel: true ,
    rechunk: true,
    to_supertypes: true } ;
    let combined_df = polars::prelude::concat(&dataframes, uargs)?;
    let ldf = combined_df.with_column(col(AMOUNT).cum_sum(false).alias(TOTAL));
    let col_date = col(DATE);
    let col_amount = col(AMOUNT);
    let col_account_name = col(ACCOUNT_NAME);
    let col_total = col(TOTAL);
    
    let ldf = ldf.select([
        col_account_name, 
        col_amount, 
        col_date, 
        col_total]);
    //CsvWriter::new()
    let out_filename = "balances.csv";
    let mut file = File::create(out_filename).expect("could not create file");
    let mut df = ldf.collect()?;
    
    let columns: &[Series] = df.get_columns();

    assert_eq!(columns[0].name(), ACCOUNT_NAME);
    assert_eq!(columns[1].name(), AMOUNT);
    assert_eq!(columns[2].name(), DATE);
    assert_eq!(columns[3].name(), TOTAL);

    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b'\t')
        .with_float_precision(Some(2))
        .finish(&mut df)?;
    println!("wrote {}", out_filename);
    Ok(())
}

