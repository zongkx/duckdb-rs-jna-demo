use anyhow::Context;
use serde::Serialize;

mod client;
pub mod d;
mod writer;

static APP_NAME: &str = "My Rust App";

#[derive(Serialize, Debug)]
struct TestData {
    timestamp: i64,
    value: i32,
}

#[no_mangle]
pub extern "C" fn doubleNumber(n: i32) -> i32 {
    n * 2
}
