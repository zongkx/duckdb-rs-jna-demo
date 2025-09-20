use crate::writer::DuckDBCsvWriter;
use anyhow::Context;
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
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let temp_dir = PathBuf::from("./temp_duckdb_test");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }
    fs::create_dir_all(&temp_dir)?;
    let db_conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
    let csv_file_name = "test_data.json";
    let writer = DuckDBCsvWriter::new(temp_dir.to_str().unwrap(), db_conn, csv_file_name, true)?;

    for i in 0..500 {
        let data_batch1: Vec<TestData> = (0..1)
            .map(|_| TestData {
                timestamp: Utc::now().timestamp_millis(),
                value: rand::random::<i32>(),
            })
            .collect();
        writer.write_rows(&data_batch1)?;
    }
    Ok(())
}
