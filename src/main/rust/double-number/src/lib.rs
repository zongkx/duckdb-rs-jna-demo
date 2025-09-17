use std::ffi::{c_char, CString};
use duckdb::{Connection, Result};


#[no_mangle]
pub extern "C" fn doubleNumber(n: i32) -> i32 {
    n * 2
}

#[no_mangle]
pub extern "C" fn getVersion() -> *mut c_char {
    let result = std::panic::catch_unwind(|| {
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare("SELECT version()")?;
        let version: String = stmt.query_row([], |row| row.get(0))?;
        Ok(version) as Result<String>
    });
    match result {
        Ok(Ok(version)) => {
            // 将Rust String转换为C-style的字符串
            // 使用CString::new来处理内部的空字节
            CString::new(version).unwrap().into_raw()
        }
        _ => {
            // 失败时返回一个空指针
            std::ptr::null_mut()
        }
    }
}
