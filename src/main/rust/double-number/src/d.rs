
use duckdb::{Connection, Result };
use std::ffi::{c_char, CString};
use crate::APP_NAME;

#[no_mangle]
pub extern "C" fn getVersion() -> *mut c_char {
    let result = std::panic::catch_unwind(|| -> Result<String> {
        // 在这里，编译器知道 Result<String> 等同于 Result<String, duckdb::Error>
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare("SELECT version()")?;
        let version: String = stmt.query_row([], |row| row.get(0))?;
        Ok(version)
    });
    match result {
        Ok(Ok(version)) => {
            CString::new(version).unwrap().into_raw()
        }
        _ => {
            std::ptr::null_mut()
        }
    }
}