use crate::AppWindow;
use crate::GlobalState;
use base64::{engine::general_purpose, Engine as _};
use duckdb::types::Value;
use duckdb::Connection;
//use native_dialog::{MessageDialog, MessageType};
use slint::ComponentHandle;
use slint::{ModelRc, SharedString, StandardListViewItem, TableColumn, VecModel};

use crate::model::{Filename, PageNumber, PageSize, QueryResult, SortIndex, SortOrder};
use crate::utils::{date32_to_ymd, get_file_extension, timeunit_to_hms, timeunit_to_ymd_hms};
use anyhow::{Context, Error};
use std::time::Instant;

/// Retrieves, processes, and returns data from a parquet file with pagination and sorting capabilities.
///
/// # Arguments
///
/// * `filename` - The path to the parquet file to be read
/// * `page_number` - The page number to retrieve (1-indexed)
/// * `page_size` - The number of records per page
/// * `sort_index` - The column index to sort by (1-indexed, or -1 for no sorting)
/// * `sort_order` - Sort in ascending (1) or descending (2) order or unsorted (0)
///
/// # Returns
///
/// * `Result<QueryResult, Error>` - A QueryResult containing column metadata and row data on success,
///   or an Error if the operation fails
///
/// # Errors
///
/// Returns an error if:
/// * Page number is less than 1
/// * Sort index is 0 (must be greater than 0 for columns or -1 for no sorting)
/// * DuckDB connection fails
/// * Query preparation or execution fails
/// * Column information retrieval fails
///
/// # Example
///
/// ```
/// let result = fetch_data(
///     Filename("data.parquet".into()),
///     PageNumber(1),
///     PageSize(10),
///     SortIndex(1),
///     SortOrder(1)
/// )?;
/// ```
pub fn fetch_data(
    filename: Filename,
    page_number: PageNumber,
    page_size: PageSize,
    sort_index: SortIndex,
    sort_order: SortOrder,
) -> Result<QueryResult, Error> {
    if page_number.0 < 1 {
        return Err(Error::msg("Page number must be greater than 0"));
    }

    // Create or get the DuckDB connection
    let conn = Connection::open_in_memory()
        .with_context(|| format!("Failed to set up duckdb connection"))?;

    // Build the SQL query with sorting and filtering
    // Get the file extension and determine the appropriate scan function
    let extension = get_file_extension(&filename.0);
    let scan_function = match extension.as_str() {
        "parquet" => "parquet_scan",
        "csv" => "read_csv_auto",
        _ => {
            return Err(Error::msg("Unsupported or unknown file type"));
        }
    };

    let mut query = format!("SELECT * FROM {}('{}')", scan_function, filename.0);

    // second query is needed to get the column names and types
    let query2 = format!("SELECT * FROM {}('{}') LIMIT 1", scan_function, filename.0);

    // third query is needed to get the total number of rows
    let query3 = format!(
        "SELECT count(1) count FROM {}('{}')",
        scan_function, filename.0
    );

    // Add sorting if needed
    let sort_direction: &str = match sort_order.0 {
        1 => "ASC",  // ascending
        2 => "DESC", // descending
        _ => "",     // unsorted
    };

    // we won't do sorting if the index value is invaild
    if sort_index.0 > 0 {
        query.push_str(&format!(" ORDER BY {} {}", sort_index.0, sort_direction));
    }

    // Add pagination
    let offset = (page_number.0 - 1) * page_size.0;
    query.push_str(&format!(" LIMIT {} OFFSET {}", page_size.0, offset));

    let start = Instant::now();

    // Execute the query
    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to create context with '{}'", filename.0))?;

    // second statement
    let mut stmt2 = conn
        .prepare(&query2)
        .with_context(|| format!("Failed to create metadata context with '{}'", filename.0))?;

    let rows = &mut stmt
        .query([])
        .with_context(|| format!("Failed to execute query"))?;

    let _ = stmt2
        .query([])
        .with_context(|| format!("Failed to execute metadata query"))?;

    // get the column count from the second statement. We can't use the first statement because the let rows =... takes
    // ownership of it.
    let column_count = stmt2.column_count();

    // get the column metadata
    let mut column_names: Vec<TableColumn> = Vec::new();
    // get the column names and types
    for i in 0..column_count {
        // get the column name and type
        let column_name = stmt2
            .column_name(i)
            .with_context(|| format!("Failed to get the column name at index '{}'", i))?
            .to_string()
            .clone();

        let column_type = stmt2
            .column_type(i)
            .to_string()
            .split('(')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();

        let display_name = format!("{}\n({})", column_name, column_type);
        let mut table_column = TableColumn::default();
        table_column.title = SharedString::from(display_name.as_str());
        table_column.min_width = 50.0;
        table_column.width = 100.0;
        column_names.push(table_column);
    }

    // get the data from the query
    let mut row_list: Vec<Vec<StandardListViewItem>> = Vec::new();

    while let Some(row) = rows.next().with_context(|| format!("Failed to get row"))? {
        // get the items from each row
        let mut row_data: Vec<StandardListViewItem> = Vec::new();
        for i in 0..column_count {
            let value = match row.get(i) {
                Ok(Value::Null) => "NULL".to_string(),
                Ok(Value::Boolean(b)) => b.to_string(),
                Ok(Value::TinyInt(n)) => n.to_string(),
                Ok(Value::SmallInt(n)) => n.to_string(),
                Ok(Value::Int(n)) => n.to_string(),
                Ok(Value::BigInt(n)) => n.to_string(),
                Ok(Value::HugeInt(n)) => n.to_string(),

                Ok(Value::UTinyInt(n)) => n.to_string(),
                Ok(Value::USmallInt(n)) => n.to_string(),
                Ok(Value::UInt(n)) => n.to_string(),
                Ok(Value::UBigInt(n)) => n.to_string(),

                Ok(Value::Float(f)) => f.to_string(),
                Ok(Value::Double(d)) => d.to_string(),
                Ok(Value::Decimal(s)) => s.to_string(),

                Ok(Value::Text(s)) => s,
                Ok(Value::Blob(b)) => {
                    let base64_str = general_purpose::STANDARD.encode(b);
                    let truncated_str = if base64_str.len() > 25 {
                        format!("{}...", &base64_str[..25])
                    } else {
                        format!("{}", &base64_str)
                    };
                    truncated_str
                }
                Ok(Value::Date32(date)) => date32_to_ymd(date),
                Ok(Value::Timestamp(unit, i64timestamp)) => timeunit_to_ymd_hms(unit, i64timestamp),
                Ok(Value::Time64(unit, u64timestamp)) => timeunit_to_hms(unit, u64timestamp),
                Ok(Value::Interval {
                    months: _,
                    days: _,
                    nanos: _,
                }) => "Interval".to_string(), // TODO

                Ok(Value::List(v)) => format!("{:#?}", v).replace("\n", "").replace(" ", ""),
                Ok(Value::Enum(s)) => s,
                Ok(Value::Struct(om)) => format!("{:#?}", om).replace("\n", "").replace(" ", ""),
                Ok(Value::Array(v)) => format!("{:#?}", v).replace("\n", "").replace(" ", ""),
                Ok(Value::Map(om)) => format!("{:#?}", om).replace("\n", "").replace(" ", ""),
                Ok(Value::Union(u)) => format!("{:#?}", u).replace("\n", "").replace(" ", ""),

                Err(e) => format!("Error: {}", e),
            };
            row_data.push(StandardListViewItem::from(value.as_str()));
        }
        row_list.push(row_data);
    }

    let duration = start.elapsed();

    // total row count
    let mut stmt3 = conn
        .prepare(&query3)
        .with_context(|| format!("Failed to create rowcount context with '{}'", filename.0))?;

    let rows = &mut stmt3
        .query([])
        .with_context(|| format!("Failed to execute query"))?;

    let row_count = match rows.next().with_context(|| format!("Failed to get row"))? {
        Some(row) => {
            let row_count = row
                .get(0)
                .with_context(|| format!("Failed to get row count"))?;
            row_count
        }
        None => -1,
    };

    Ok(QueryResult {
        column_names,
        rows: row_list,
        row_count: row_count,
        duration: duration,
    })
}

pub fn update_table_async(
    ui: &slint::Weak<AppWindow>,
    load_table_columns: bool,
    filename: Filename,
    page_number: PageNumber,
    page_size: PageSize,
    sort_index: SortIndex,
    sort_order: SortOrder,
) -> Result<(), Error> {
    let filename_clone = filename.clone();
    // fetch the data
    match fetch_data(
        filename,
        PageNumber(page_number.0),
        PageSize(page_size.0),
        SortIndex(sort_index.0),
        SortOrder(sort_order.0),
    ) {
        Ok(results) => {
            let ui_clone = ui.clone();
            update_table_ui(
                ui_clone,
                load_table_columns,
                results,
                page_size,
                filename_clone,
            );
            stop_page_loading(ui.clone());
            Ok(())
        }
        Err(_e) => {
            let ui_clone = ui.clone();
            stop_page_loading(ui_clone);
            let error_message: String = format!("Error reading file '{}'", &filename_clone.0);
            Err(Error::msg(error_message))
        }
    }
}

pub fn set_ui_defaults(ui: &AppWindow) {
    ui.global::<GlobalState>().set_page_loading(true);
    ui.global::<GlobalState>()
        .set_filename(SharedString::from(""));
    ui.global::<GlobalState>().set_pagination_enabled(false);
    ui.global::<GlobalState>().set_record_count(10);
    ui.global::<GlobalState>().set_sort_index(-1);
    ui.global::<GlobalState>().set_sort_order(0);
    ui.global::<GlobalState>().set_max_pages(1);
    ui.global::<GlobalState>().set_page_number(1);
    ui.global::<GlobalState>()
        .set_error_message(SharedString::from(""));
    ui.global::<GlobalState>().set_has_error(false);

    // clear the table headers
    let column_names: Vec<TableColumn> = Vec::new();
    let model_columns: ModelRc<TableColumn> = ModelRc::new(VecModel::from(column_names));
    // set the column header names on the ui
    ui.global::<GlobalState>().set_column_names(model_columns);

    // TODO: clear the table content
    let model_data: Vec<ModelRc<StandardListViewItem>> = Vec::new();
    ui.global::<GlobalState>()
        .set_row_data(ModelRc::new(VecModel::from(model_data)));
}

fn stop_page_loading(ui: slint::Weak<AppWindow>) {
    let _ = ui.upgrade_in_event_loop(move |handle| {
        handle.global::<GlobalState>().set_page_loading(false)
    });
}

fn update_table_ui(
    ui: slint::Weak<AppWindow>,
    load_table_columns: bool,
    results: QueryResult,
    page_size: PageSize,
    filename: Filename,
) {
    let _ = ui.upgrade_in_event_loop(move |handle| {
        let row_count = results.row_count;
        let mut model_data = Vec::new();
        // Convert the results to the model format
        for row_result in results.rows.into_iter() {
            model_data.push(ModelRc::new(VecModel::from(row_result)));
        }
        if load_table_columns {
            let model_columns: ModelRc<TableColumn> =
                ModelRc::new(VecModel::from(results.column_names.clone()));
            // set the column header names on the ui
            handle
                .global::<GlobalState>()
                .set_column_names(model_columns);
        }

        // set the row data on the ui
        handle
            .global::<GlobalState>()
            .set_row_data(ModelRc::new(VecModel::from(model_data)));

        // set the total records on the ui
        handle.global::<GlobalState>().set_record_count(row_count);
        // enable pagination
        handle.global::<GlobalState>().set_pagination_enabled(true);
        // set the max pages on the ui
        handle
            .global::<GlobalState>()
            .set_max_pages((row_count / page_size.0) + 1);
        // set the duration on the ui
        handle
            .global::<GlobalState>()
            .set_duration(format!("{:?}", results.duration).into());
        handle.global::<GlobalState>().set_filename(filename.0);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    // Helper function to create a test parquet file
    fn create_test_parquet_file(path: &str) -> Result<(), Error> {
        // We'll use DuckDB to create a parquet file for testing
        let conn = Connection::open_in_memory()?;

        // Create a simple table
        conn.execute(
            "CREATE TABLE test_data AS SELECT 
             1 as id, 'Product A' as name, 'Electronics' as category, 19.99 as price",
            [],
        )?;

        // Add more rows
        conn.execute(
            "INSERT INTO test_data VALUES
             (2, 'Product B', 'Clothing', 29.99),
             (3, 'Product C', 'Food', 9.99),
             (4, 'Product D', 'Books', 14.99),
             (5, 'Product E', 'Electronics', 99.99)",
            [],
        )?;

        // Make sure the directory exists
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Export to parquet
        conn.execute(
            &format!("COPY test_data TO '{}' (FORMAT PARQUET)", path),
            [],
        )?;

        Ok(())
    }

    // Helper function to create a test csv file
    fn create_test_csv_file(path: &str) -> Result<(), Error> {
        // We'll use DuckDB to create a parquet file for testing
        let conn = Connection::open_in_memory()?;

        // Create a simple table
        conn.execute(
            "CREATE TABLE test_data AS SELECT 
         1 as id, 'Product A' as name, 'Electronics' as category, 19.99 as price",
            [],
        )?;

        // Add more rows
        conn.execute(
            "INSERT INTO test_data VALUES
         (2, 'Product B', 'Clothing', 29.99),
         (3, 'Product C', 'Food', 9.99),
         (4, 'Product D', 'Books', 14.99),
         (5, 'Product E', 'Electronics', 99.99)",
            [],
        )?;

        // Make sure the directory exists
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Export to csv
        conn.execute(&format!("COPY test_data TO '{}'", path), [])?;

        Ok(())
    }

    #[test]
    fn test_fetch_data_parquet() -> Result<(), Error> {
        // Create a temporary test file in the target directory
        let test_file_path = "target/test_data.parquet";

        // Create the test parquet file
        create_test_parquet_file(test_file_path)?;

        // Make sure the file exists
        assert!(
            Path::new(test_file_path).exists(),
            "Test parquet file was not created"
        );

        // Test with default parameters
        let result = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(10),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;

        // Verify the results

        let column_count = result.column_names.len();
        assert_eq!(
            column_count, 4,
            "Expected 4 columns but got {}",
            column_count
        );
        let total_rows = result.rows.len();
        assert_eq!(total_rows, 5, "Expected 5 rows but got {}", total_rows);

        // Check column names
        // the namee construct is the ccombination of the name and the type
        let expected_columns = vec![
            "id\n(Int32)",
            "name\n(Utf8)",
            "category\n(Utf8)",
            "price\n(Decimal128)",
        ];
        for col in expected_columns {
            let name_exists = result
                .column_names
                .iter()
                .any(|metadata| metadata.title.as_str() == col);
            assert!(
                name_exists,
                "Column '{}' not found in column_names: {:?}",
                col, result.column_names
            );
        }

        // Test pagination - page 1 with 2 items per page
        let page1 = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(2),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;
        let total_rows = page1.rows.len();
        assert_eq!(
            total_rows, 2,
            "Expected 2 rows in page 1 but got {}",
            total_rows
        );

        // Test pagination - page 2 with 2 items per page
        let page2 = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(2),
            PageSize(2),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;
        let total_rows = page2.rows.len();
        assert_eq!(
            total_rows, 2,
            "Expected 2 rows in page 2 but got {}",
            total_rows
        );

        // Test sorting by id in descending order (column index 0)
        let sorted = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(10),
            SortIndex(1),
            SortOrder(2), // 2 is descending
        )?;
        // First row should have id = 5
        assert_eq!(
            sorted.rows[0][0].text,
            "5", //expected_id,
            "Expected first row id to be 5 but got {}",
            //sorted.rows[0][0]
            format!("{:?}", sorted.rows[0][0])
        );

        // Clean up the test file
        fs::remove_file(test_file_path)?;

        Ok(())
    }

    #[test]
    fn test_fetch_data_csv() -> Result<(), Error> {
        // Create a temporary test file in the target directory
        let test_file_path = "target/test_data.csv";

        // Create the test csv file
        create_test_csv_file(test_file_path)?;

        // Make sure the file exists
        assert!(
            Path::new(test_file_path).exists(),
            "Test csv file was not created"
        );

        // Test with default parameters
        let result = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(10),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;

        // Verify the results

        let column_count = result.column_names.len();
        assert_eq!(
            column_count, 4,
            "Expected 4 columns but got {}",
            column_count
        );
        let total_rows = result.rows.len();
        assert_eq!(total_rows, 5, "Expected 5 rows but got {}", total_rows);

        // Check column names
        // the namee construct is the ccombination of the name and the type
        let expected_columns = vec![
            "id\n(Int64)",
            "name\n(Utf8)",
            "category\n(Utf8)",
            "price\n(Float64)",
        ];
        for col in expected_columns {
            let name_exists = result
                .column_names
                .iter()
                .any(|metadata| metadata.title.as_str() == col);
            assert!(
                name_exists,
                "Column '{}' not found in column_names: {:?}",
                col, result.column_names
            );
        }

        // Test pagination - page 1 with 2 items per page
        let page1 = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(2),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;
        let total_rows = page1.rows.len();
        assert_eq!(
            total_rows, 2,
            "Expected 2 rows in page 1 but got {}",
            total_rows
        );

        // Test pagination - page 2 with 2 items per page
        let page2 = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(2),
            PageSize(2),
            SortIndex(-1),
            SortOrder(1), // 1 is ascending
        )?;
        let total_rows = page2.rows.len();
        assert_eq!(
            total_rows, 2,
            "Expected 2 rows in page 2 but got {}",
            total_rows
        );

        // Test sorting by id in descending order (column index 0)
        let sorted = fetch_data(
            Filename(test_file_path.into()),
            PageNumber(1),
            PageSize(10),
            SortIndex(1),
            SortOrder(2), // 2 is descending
        )?;
        // First row should have id = 5
        assert_eq!(
            sorted.rows[0][0].text,
            "5", //expected_id,
            "Expected first row id to be 5 but got {}",
            //sorted.rows[0][0]
            format!("{:?}", sorted.rows[0][0])
        );

        // Clean up the test file
        fs::remove_file(test_file_path)?;

        Ok(())
    }
}
