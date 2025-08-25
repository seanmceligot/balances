use anyhow::Context;
use anyhow::Ok;
use anyhow::Result;
use numfmt::Formatter;
use numfmt::Precision;
use polars::chunked_array::ops::SortOptions;
use polars::prelude::*;

use std::collections::BTreeMap;
use std::env;
use std::fs::read_to_string;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use comfy_table::Table;

use polars_arrow::temporal_conversions::date32_to_date;

static ACCOUNT_NAME: &str = "Account Name";
static AMOUNT: &str = "Amount";
static DATE: &str = "Date";
static TOTAL: &str = "Total";
static ACCOUNT_TYPE: &str = "account_type";

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            return Path::new(&home).join(stripped);
        }
    }
    PathBuf::from(path)
}
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
fn read_filenames_from_file(filenames_file: &PathBuf) -> Result<Vec<PathBuf>> {
    println!("read filenames_file: {:?}", filenames_file);
    let text = read_to_string(filenames_file.clone())
        .with_context(|| format!("Failed to read '{:?}'", filenames_file.clone()))?;

    let base_dir = filenames_file
        .parent()
        .unwrap_or_else(|| Path::new(""));

    let mut files = Vec::new();
    for filename in text.lines().map(|f| f.trim()) {
        if filename.is_empty() {
            continue;
        }
        let path = base_dir.join(filename);

        // Just check if file can be opened, otherwise warn and skip
        if let Err(e) = File::open(&path) {
            eprintln!("Warning: could not open {}: {}", path.display(), e);
            continue;
        }

        files.push(path);
    }
    Ok(files)
}
// Sample CVS
// 
// "Date","Amount","Account Name"
// "2021-10-19","10,900.44","My Checking"
// "2021-10-19","11,900.44","My Checking"
// "2021-10-19","9,900.44","My Checking"

fn newest_balance(filename: &PathBuf) -> Result<LazyFrame> {
    let filename = PlPath::Local(Arc::from(filename.clone()));

    // Build a Polars schema using DataType (Polars 0.50 API)
    let mut fields = PlIndexMap::new(); // <PlSmallStr, DataType>
    fields.insert(PlSmallStr::from_str(DATE), DataType::Date);
    fields.insert(PlSmallStr::from_str(AMOUNT), DataType::Float64);
    fields.insert(PlSmallStr::from_str(ACCOUNT_NAME), DataType::String);
    let polars_schema: Schema = Schema::from(fields);
    println!("read csv: {:?}", filename);
    let df = LazyCsvReader::new(filename)
        .with_has_header(true)
        .with_schema(Some(Arc::new(polars_schema)))
        .finish()?;
    let sorted: LazyFrame = df.sort( [DATE], Default::default());
    Ok(sorted.last())
}

// categories.csv
// Account Name,account_type
fn load_categories(categories_csv_path: &PathBuf) -> Result<LazyFrame> {
    let categories_csv_path = PlPath::Local(Arc::from(categories_csv_path.clone()));
    // Build schema as Schema<DataType> for CSV reader
    let mut fields = PlIndexMap::new();
    fields.insert(PlSmallStr::from_str(ACCOUNT_NAME), DataType::String);
    fields.insert(PlSmallStr::from_str("account_type"), DataType::String);
    let polars_schema: Schema = Schema::from(fields);
    let ldf = LazyCsvReader::new(categories_csv_path)
        .with_has_header(true)
        .with_schema(Some(Arc::new(polars_schema)))
        .finish()?;
    Ok(ldf)
}
fn main() -> Result<()> {
    configure_the_environment();
    let data_dir = expand_tilde("~/.local/share/fi/balances/");
    let balances_file_list = data_dir.join("balances.txt");
    let mut dataframes = Vec::new();
    let filenames = read_filenames_from_file(&balances_file_list)?;

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

    let uargs = UnionArgs {
        parallel: true,
        rechunk: true,
        to_supertypes: true,
        diagonal: false, from_partitioned_ds: false, maintain_order: true
    };
    let combined_df = polars::prelude::concat(&dataframes, uargs)?;
    let ldf = combined_df.with_column(col(AMOUNT).cum_sum(false).alias(TOTAL));
    let col_date = col(DATE);
    let col_amount = col(AMOUNT);
    let col_account_name = col(ACCOUNT_NAME);
    let col_total = col(TOTAL);

    let ldf = ldf.select([col_account_name, col_amount, col_date, col_total]);
    let categories_csv_path = data_dir.join("categories.csv");
    let categories = load_categories(&categories_csv_path)?;
    let c = categories.collect()?.lazy();
    // join categories to ldf on Account Name
    let ldf = ldf.left_join(c, col(ACCOUNT_NAME), col(ACCOUNT_NAME));

    let out_filename = "balances.csv";
    let ldf2 = ldf.clone();
    let ldf3 = ldf.clone();
    let mut df = ldf.collect()?;

    let columns: &[Column] = df.get_columns();

    assert_eq!(columns[0].name(), ACCOUNT_NAME);
    assert_eq!(columns[1].name(), AMOUNT);
    assert_eq!(columns[2].name(), DATE);
    assert_eq!(columns[3].name(), TOTAL);
    assert_eq!(columns[4].name(), ACCOUNT_TYPE);

    write_csv(&mut df, out_filename)?;

    println!("subtotals pre account type");
    show_subtotals(&ldf2)?;

    let df = ldf3.collect()?;
    let row_wise = (0..df.height()).map(|x| df.get_row(x).unwrap());

    // print header with column names. cut off each name at 20 characters
    let mut table = Table::new();
    let column_names = columns_names(&df);
    table.set_header(column_names);
    for row in row_wise {
        table.add_row(row.0.into_iter()
            .map(|any| any_to_string(any))
            .map(|mb| mb.unwrap_or_default())
        );
    }
    println!("{table}");

    Ok(())
}
fn any_to_string(any: AnyValue) -> Option<String> {
    match any {
        AnyValue::String(s) => Some(String::from(s)),
        AnyValue::Float64(f) => {
            let mut format = Formatter::new() 
                .separator(',').unwrap()
                .prefix("$").unwrap()
                .precision(Precision::Decimals(2));
            let fstr = format.fmt2(f);
            Some(String::from(fstr))
        },
        AnyValue::Date(d) => Some(date32_to_date(d).format("%Y-%m-%d").to_string()),
        AnyValue::Null => None,
        _ => None,
    }
}   
pub fn configure_the_environment() {
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
                                                           //env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    env::set_var("POLARS_FMT_MAX_ROWS", "999999"); // maximum number of rows shown when formatting DataFrames.
    env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.
    // set thousands separator to ,
    env::set_var("POLARS_FMT_THOUSANDS_SEP", ",");
}

fn columns_names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect()
}
fn show_subtotals(ldf2: &LazyFrame) -> Result<()> {
    let g = ldf2.clone().group_by_stable([
        ACCOUNT_TYPE]);

    let subtotals = g.agg([
        col(AMOUNT).sum()
    ]);
    let mut df = subtotals.collect()?;
    //println!("\n{}", df);
    let row_wise = (0..df.height()).map(|x| df.get_row(x).unwrap());

    // print header with column names. cut off each name at 20 characters
    let mut table = Table::new();
    let column_names = columns_names(&df);
    table.set_header(column_names);
    for row in row_wise {
        table.add_row(row.0.into_iter()
            .map(|any| any_to_string(any))
            .map(|mb| mb.unwrap_or_default())
        );
    }
    println!("== subtotals ==");
    println!("{table}");
    write_csv(&mut df, "subtotals.csv")?;
    Ok(())
}
fn write_csv(df: &mut DataFrame, out_filename: &str) -> Result<()> {
    let mut file = File::create(out_filename).expect("could not create file");
    CsvWriter::new(&mut file)
        .include_header(true)
        //.with_separator(b'\t')
        .with_float_precision(Some(2))
        .finish(df)?;
    println!("wrote {}", out_filename);
    println!("\n{}", df);
    Ok(())
}
