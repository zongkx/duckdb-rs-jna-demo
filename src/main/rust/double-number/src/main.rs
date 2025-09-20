use crate::writer::DuckDBCsvWriter;
use anyhow::{anyhow, Context, Result};
// 确保导入了 Context trait
use chrono::Utc;
use duckdb::Connection;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub mod d;
mod writer;

static APP_NAME: &str = "My Rust App";

#[derive(Serialize, Debug)]
struct TestData {
    timestamp: i64,
    value: i32,
}

pub fn main() -> anyhow::Result<()> {
    // 1. Setup logging for better visibility during testing.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    // 2. Create a temporary directory for our test files.
    let temp_dir = PathBuf::from("./temp_duckdb_test");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }
    fs::create_dir_all(&temp_dir)?;

    // 3. Create an in-memory DuckDB connection.
    let db_conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
    let csv_file_name = "test_data.json";

    // 4. Instantiate our writer with need_organize set to true.
    let writer = DuckDBCsvWriter::new(temp_dir.to_str().unwrap(), db_conn, csv_file_name, true)?;

    // 5. Generate and write data in batches.
    let first_batch_size = 800;
    let second_batch_size = 500;
    let mut total_rows = 0;

    // First batch: it should not trigger the `organize` function.
    let data_batch1: Vec<TestData> = (0..first_batch_size)
        .map(|_| TestData {
            timestamp: Utc::now().timestamp_millis(),
            value: rand::random::<i32>(),
        })
        .collect();

    log::info!("Writing first batch of {} rows...", first_batch_size);
    writer.write_rows(&data_batch1)?;
    total_rows += first_batch_size;
    log::info!("Current rows in CSV: {}", total_rows);

    Ok(())
}
