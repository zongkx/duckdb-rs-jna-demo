pub  mod d;
mod writer;

static APP_NAME: &str = "My Rust App";



#[no_mangle]
pub extern "C" fn doubleNumber(n: i32) -> i32 {
    n * 2
}

