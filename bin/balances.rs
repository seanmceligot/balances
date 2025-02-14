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
fn read_filenames_from_file(filenames_file: &str) -> Result<Vec<PathBuf>> {
    let text = read_to_string(filenames_file)?;
    let vec_of_pathbuf = text.lines().map(|line| PathBuf::from(line.trim())).collect();
    Ok(vec_of_pathbuf)
}

fn newest_balance(filename: &PathBuf) -> Result<LazyFrame> {
    let fields = vec![
        ArrowField::new(DATE, ArrowDataType::Date32, false),
        ArrowField::new(AMOUNT, ArrowDataType::Float64, false),
        ArrowField::new(ACCOUNT_NAME, ArrowDataType::Utf8, false),
    ];

    let metadata: BTreeMap<String, String> = BTreeMap::new();
    let arrow_schema = ArrowSchema { fields, metadata };
    let polars_schema = Schema::from(Arc::new(arrow_schema));
    println!("read csv: {:?}", filename);
    let df = LazyCsvReader::new(Path::new(filename))
        .has_header(true)
        .with_schema(Some(Arc::new(polars_schema)))
        .finish()?;
    let sorted: LazyFrame = df.sort(
        DATE,
        SortOptions {
            descending: false,
            nulls_last: false,
            multithreaded: true,
            maintain_order: true,
        },
    );
    Ok(sorted.last())
}

// categories.csv
// Account Name,account_type
fn load_categories() -> Result<LazyFrame> {
    let fields = vec![
        ArrowField::new(ACCOUNT_NAME, ArrowDataType::Utf8, false),
        ArrowField::new("account_type", ArrowDataType::Utf8, false),
    ];
    let metadata: BTreeMap<String, String> = BTreeMap::new();
    let arrow_schema = ArrowSchema { fields, metadata };
    let polars_schema = Schema::from(Arc::new(arrow_schema));
    let ldf = LazyCsvReader::new("categories.csv")
        .has_header(true)
        .with_schema(Some(Arc::new(polars_schema)))
        .finish()?;
    Ok(ldf)
}

fn main() -> Result<()> {
    configure_the_environment();

    let mut dataframes = Vec::new();
    let filenames = read_filenames_from_file("balances.txt")?;

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
    };
    let combined_df = polars::prelude::concat(&dataframes, uargs)?;
    let ldf = combined_df.with_column(col(AMOUNT).cum_sum(false).alias(TOTAL));
    let col_date = col(DATE);
    let col_amount = col(AMOUNT);
    let col_account_name = col(ACCOUNT_NAME);
    let col_total = col(TOTAL);

    let ldf = ldf.select([col_account_name, col_amount, col_date, col_total]);

    let categories = load_categories()?;
    let c = categories.collect()?.lazy();
    // join categories to ldf on Account Name
    let ldf = ldf.left_join(c, col(ACCOUNT_NAME), col(ACCOUNT_NAME));

    let out_filename = "balances.csv";
    let ldf2 = ldf.clone();
    let ldf3 = ldf.clone();
    let mut df = ldf.collect()?;

    let columns: &[Series] = df.get_columns();

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
        AnyValue::Utf8(s) => Some(String::from(s)),
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
