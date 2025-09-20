use std::os::raw::{c_char, c_int};
use std::slice;

// 使用 Option<String> 来安全地拥有字符串数据
static mut DIR: Option<String> = None;

#[no_mangle]
// 接受 C 风格的字符串指针和长度
pub extern "C" fn init(dir_ptr: *const c_char, len: usize) -> c_int {
    // 确保在不安全块中处理原始指针和静态可变变量
    unsafe {
        // 检查指针是否为空
        if dir_ptr.is_null() {
            return -1; // 空指针错误
        }

        // 将原始指针和长度转换为 Rust 的字节切片
        let dir_slice = slice::from_raw_parts(dir_ptr as *const u8, len);

        // 尝试将字节切片转换为有效的 UTF-8 字符串
        match String::from_utf8(dir_slice.to_vec()) {
            Ok(s) => {
                // 将所有权转移给全局静态变量
                DIR = Some(s);
                println!("Rust DIR initialized to: {}", DIR.as_ref().unwrap());
                0 // 成功
            }
            Err(_) => {
                // 无效的 UTF-8 编码
                -2
            }
        }
    }
}
