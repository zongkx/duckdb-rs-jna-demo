
use anyhow::{anyhow, Result};
use duckdb::{Connection};
use log::{debug, error};
use rand::Rng;
use serde::Serialize;
use serde_json::to_string;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex};
use std::time::Instant;

// DuckDB写入器的核心结构体
pub struct DuckDBCsvWriter {
    base_path: PathBuf,
    csv_file: PathBuf,
    row_counter: AtomicUsize,
    db_connection: Arc<Mutex<Connection>>,
    need_organize: bool,
    limit: usize,
    current_writer: Arc<Mutex<Option<io::BufWriter<File>>>>,
}
pub const COPY_TO_SQL: &'static str = "copy (select * from read_json ('%s',\
     {auto_detect: true, format: 'newline_delimited'}) ) to '%s' (format parquet, compression lz4_raw ,PARQUET_VERSION v2) ";
impl DuckDBCsvWriter {


    pub fn new(base_path: &str, db_connection: Arc<Mutex<Connection>>, csv_file_name: &str, need_organize: bool) -> Result<Self> {
        let base_path_buf = PathBuf::from(base_path);
        let csv_file_buf = base_path_buf.join(csv_file_name);

        let mut rng = rand::thread_rng();
        let limit = rng.gen_range(1000..=2999);

        // 初始化writer
        let writer = Self::create_new_writer(&csv_file_buf, false)?;

        Ok(Self {
            base_path: base_path_buf,
            csv_file: csv_file_buf,
            row_counter: AtomicUsize::new(0),
            db_connection,
            need_organize,
            limit,
            current_writer: Arc::new(Mutex::new(Some(writer))),
        })
    }

    pub fn write_rows<T: Serialize>(&self, list: &[T]) -> Result<()> {
        let mut writer_guard = self.current_writer.lock().unwrap();
        let writer = writer_guard.as_mut().ok_or_else(|| anyhow!("Writer is closed"))?;

        for item in list {
            let json_string = to_string(item)?;
            writeln!(writer, "{}", json_string)?;
        }
        writer.flush()?;

        let new_count = self.row_counter.fetch_add(list.len(), Ordering::Relaxed) + list.len();

        if self.need_organize && new_count >= self.limit {
            self.organize()?;
        }
        Ok(())
    }

    fn organize(&self) -> Result<()> {
        let start_time = Instant::now();
        let parquet_file_name = self.get_parquet_file_name()?;

        // 关闭并清空当前写入器，确保所有数据都已写入文件
        let mut writer_guard = self.current_writer.lock().unwrap();
        writer_guard.take();

        // 使用 DuckDB 合并文件
        let statement_result = self.db_connection.lock().unwrap()
            .execute(&format!("{} {} {}", COPY_TO_SQL, self.csv_file.display(), parquet_file_name.display()),
                     &[] as &[&dyn duckdb::ToSql]);

        let success = statement_result.is_ok();

        if !success {
            error!("duckdb merge error: {:?}", statement_result.unwrap_err());
            fs::remove_file(&parquet_file_name).ok();
        }

        // 无论成功与否，重置计数器并创建新的写入器
        self.row_counter.store(0, Ordering::Relaxed);
        *writer_guard = Some(Self::create_new_writer(&self.csv_file, success)?);

        debug!("文件合并任务结束: {}, 耗时: {}ms", self.csv_file.display(), start_time.elapsed().as_millis());

        Ok(())
    }

    fn create_new_writer(path: &Path, overwrite: bool) -> Result<io::BufWriter<File>> {
        fs::create_dir_all(path.parent().unwrap_or(Path::new("")))?;

        let file = if overwrite || !path.exists() {
            File::create(path)?
        } else {
            OpenOptions::new().append(true).create(true).open(path)?
        };

        Ok(io::BufWriter::new(file))
    }

    fn get_parquet_file_name(&self) -> Result<PathBuf> {
        let timestamp = chrono::Utc::now().timestamp_millis();
        Ok(self.base_path.join(format!("{}.parquet", timestamp)))
    }
}

