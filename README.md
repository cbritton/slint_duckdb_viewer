# slint_duckdb_viewer
Slint front end to duckdb to show structured files

This is a slint front end to show structured files such as CSV and parquet.  DuckDB drives the content for the data.  The backend is rust.

This is basically a learning experience with slint and how to interact with DuckDB and tokio to create a simple, but responsive UI.

To keep memory consumption to a minimum, pagination is used to navigate between pages.
Currently, only CSV and parquet files are supported and this has only been tested on Windows 11.

#MadeWithSlint