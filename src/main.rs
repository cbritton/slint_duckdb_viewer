// Prevent console window in addition to Slint window in Windows release builds when, e.g., starting the app via file manager. Ignored on other platforms.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::Error;

use clap::Parser;

use native_dialog::{FileDialog, MessageDialog, MessageType};
use slint::SharedString;
use std::path::Path;
use std::process;

mod loader;
mod model;
mod utils;

use loader::{set_ui_defaults, update_table_async};
use model::{Filename, PageNumber, PageSize, SortIndex, SortOrder};
use utils::file_exists;

#[derive(Parser)]
#[command(
    name = "slint_duckdb_viewer",
    author = "Chris Britton <c.p.britton@gmail.com>",
    version = "0.1.0",
    about = "Slint DuckDB File Viewer"
)]
struct CLIArgs {
    #[arg(short, long, help = "File to open", required = false)]
    filename: Option<String>,
}

// Include the UI components from the Slint file
slint::include_modules!();

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = CLIArgs::parse();

    let ui = AppWindow::new()?;
    let ui_handle1 = ui.as_weak();

    // Set initial page to home
    ui.set_current_page(SharedString::from("home"));

    ctrlc::set_handler(move || {
        process::exit(0);
    })?;

    // Handle exit request
    ui.on_exit_app({
        move || {
            process::exit(0);
        }
    });

    ui.on_show_error_dialog({
        move |error_message| {
            let _ = MessageDialog::new()
                .set_title("Error Reading File")
                .set_text(&format!("{}", error_message))
                .set_type(MessageType::Error)
                .show_alert()
                .map_err(|e| println!("Failed to show error dialog: {}", e));
        }
    });

    ui.global::<GlobalState>().on_update_table_async({
        let ui_handle = ui.as_weak();
        move || {
            let ui = ui_handle.unwrap();
            // get the data from the ui to send to the loader
            let filename = ui.global::<GlobalState>().get_filename();
            let page_number = ui.global::<GlobalState>().get_page_number();
            let page_size = ui.global::<GlobalState>().get_page_size();
            let sort_index = ui.global::<GlobalState>().get_sort_index();
            let sort_order = ui.global::<GlobalState>().get_sort_order();
            let value = ui_handle.clone();
            tokio::spawn(async move {
                match update_table_async(
                    &value,
                    false,
                    Filename(filename),
                    PageNumber(page_number),
                    PageSize(page_size),
                    SortIndex(sort_index),
                    SortOrder(sort_order),
                ) {
                    Ok(_) => {}
                    Err(_e) => {
                        // TODO: show error dialog
                    }
                }
            });
        }
    });

    ui.global::<GlobalState>().on_open_file_async({
        let ui_handle = ui.as_weak();
        move || {
            let ui = ui_handle.unwrap();

            // open the file dialog. if the user selects a file then proceed
            // don't change the ui if the user cancels
            ui.set_current_page(SharedString::from("home"));

            // open the file dialog for the user to select a file
            let result = FileDialog::new()
                .add_filter("Parquet files", &["parquet"])
                .add_filter("CSV", &["csv"])
                .show_open_single_file();

            // get the filename
            let filename: String = match result {
                Ok(path) => {
                    // load data from file
                    match path {
                        Some(path) => path.display().to_string(),
                        None => {
                            // ignore
                            return;
                        }
                    }
                }
                Err(_e) => {
                    eprintln!("Failed to open file dialog");
                    return;
                }
            };

            // the user has selected an existing file.
            // set the default values on the ui. This will clear out any previous data
            set_ui_defaults(&ui);

            // call the update ui async function
            let value = ui_handle.clone();
            tokio::spawn(async move {
                match update_table_async(
                    &value,
                    true,
                    Filename(SharedString::from(filename.as_str())),
                    PageNumber(1),
                    PageSize(20),
                    SortIndex(-1),
                    SortOrder(0),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        // set the error message on the ui and this should make the error dialog show
                        let _ = value.upgrade_in_event_loop(move |handle| {
                            handle
                                .global::<GlobalState>()
                                .set_error_message(SharedString::from(e.to_string()));
                            handle.global::<GlobalState>().set_has_error(true);
                        });
                    }
                }
            });
        }
    });

    // check that a file was provided and that it exists
    // if no file was provided or it is not a file, then do nothing
    if let Some(filename) = args.filename {
        if file_exists(&filename) {
            let ui = ui_handle1.unwrap();
            let path = Path::new(&filename);
            set_ui_defaults(&ui);

            // load the data from the file
            let filename = Filename(SharedString::from(format!("{}", path.display())));
            let value = ui_handle1.clone();
            match update_table_async(
                &value,
                true,
                filename,
                PageNumber(1),
                PageSize(20),
                SortIndex(-1),
                SortOrder(0),
            ) {
                Ok(_) => {}
                Err(e) => {
                    // TODO: show error dialog
                    println!("Error: {}", e)
                }
            }
        } else {
            eprintln!("File '{}' does not exist.", filename);
        }
    }

    ui.run()?;
    Ok(())
}
