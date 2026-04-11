//! Node.js/TypeScript bindings via napi-rs.
//!
//! Provides async I/O operations for QVD files, matching the Python API.

use napi::bindgen_prelude::*;
use napi::Task;
use napi_derive::napi;
use std::collections::HashSet;

use crate::concat::{OnConflict, SchemaMode};
use crate::exists::ExistsIndex;
use crate::reader;
use crate::streaming;
use crate::writer;

// ── Helpers ──────────────────────────────────────────────────────

fn to_napi_err(e: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, format!("{}", e))
}

fn parse_schema_mode(s: &str) -> Result<SchemaMode> {
    match s.to_lowercase().as_str() {
        "strict" => Ok(SchemaMode::Strict),
        "union" => Ok(SchemaMode::Union),
        _ => Err(Error::new(
            Status::InvalidArg,
            format!("Invalid schema mode '{}', expected 'strict' or 'union'", s),
        )),
    }
}

fn parse_on_conflict(s: &str) -> Result<OnConflict> {
    match s.to_lowercase().as_str() {
        "replace" => Ok(OnConflict::Replace),
        "skip" => Ok(OnConflict::Skip),
        "error" => Ok(OnConflict::Error),
        _ => Err(Error::new(
            Status::InvalidArg,
            format!(
                "Invalid on_conflict '{}', expected 'replace', 'skip', or 'error'",
                s
            ),
        )),
    }
}

// ── QvdTable ─────────────────────────────────────────────────────

#[napi]
pub struct JsQvdTable {
    inner: reader::QvdTable,
}

#[napi]
impl JsQvdTable {
    /// Number of rows.
    #[napi(getter)]
    pub fn num_rows(&self) -> u32 {
        self.inner.num_rows() as u32
    }

    /// Number of columns.
    #[napi(getter)]
    pub fn num_cols(&self) -> u32 {
        self.inner.num_cols() as u32
    }

    /// Table name from QVD metadata.
    #[napi(getter)]
    pub fn table_name(&self) -> String {
        self.inner.header.table_name.clone()
    }

    /// Column names.
    #[napi(getter)]
    pub fn columns(&self) -> Vec<String> {
        self.inner
            .header
            .fields
            .iter()
            .map(|f| f.field_name.clone())
            .collect()
    }

    /// Get a single cell value by row and column index.
    #[napi]
    pub fn get(&self, row: u32, col: u32) -> Result<Option<String>> {
        let row = row as usize;
        let col = col as usize;
        if row >= self.inner.num_rows() || col >= self.inner.num_cols() {
            return Err(Error::new(Status::InvalidArg, "Index out of bounds"));
        }
        Ok(self.inner.get(row, col).as_string())
    }

    /// Get a single cell value by row index and column name.
    #[napi]
    pub fn get_by_name(&self, row: u32, col_name: String) -> Result<Option<String>> {
        match self.inner.get_by_name(row as usize, &col_name) {
            Some(val) => Ok(val.as_string()),
            None => Err(Error::new(
                Status::InvalidArg,
                format!("Column '{}' not found", col_name),
            )),
        }
    }

    /// Get all values of a column by index.
    #[napi]
    pub fn column_values(&self, col: u32) -> Result<Vec<Option<String>>> {
        let col = col as usize;
        if col >= self.inner.num_cols() {
            return Err(Error::new(
                Status::InvalidArg,
                "Column index out of bounds",
            ));
        }
        Ok(self.inner.column_strings(col))
    }

    /// Get all values of a column by name.
    #[napi]
    pub fn column_values_by_name(&self, col_name: String) -> Result<Vec<Option<String>>> {
        let col = self
            .inner
            .header
            .fields
            .iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("Column '{}' not found", col_name),
                )
            })?;
        Ok(self.inner.column_strings(col))
    }

    /// Convert to an array of row objects: [{col1: val1, col2: val2, ...}, ...]
    #[napi]
    pub fn to_json(&self) -> Vec<serde_json::Value> {
        let mut rows = Vec::with_capacity(self.inner.num_rows());
        for row in 0..self.inner.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col, field) in self.inner.header.fields.iter().enumerate() {
                let val = self.inner.get(row, col).as_string();
                obj.insert(
                    field.field_name.clone(),
                    match val {
                        Some(s) => serde_json::Value::String(s),
                        None => serde_json::Value::Null,
                    },
                );
            }
            rows.push(serde_json::Value::Object(obj));
        }
        rows
    }

    /// Get first N rows as array of objects.
    #[napi]
    pub fn head(&self, n: Option<u32>) -> Vec<serde_json::Value> {
        let n = n.unwrap_or(10).min(self.inner.num_rows() as u32) as usize;
        let mut rows = Vec::with_capacity(n);
        for row in 0..n {
            let mut obj = serde_json::Map::new();
            for (col, field) in self.inner.header.fields.iter().enumerate() {
                let val = self.inner.get(row, col).as_string();
                obj.insert(
                    field.field_name.clone(),
                    match val {
                        Some(s) => serde_json::Value::String(s),
                        None => serde_json::Value::Null,
                    },
                );
            }
            rows.push(serde_json::Value::Object(obj));
        }
        rows
    }

    /// Get unique symbols for a column.
    #[napi]
    pub fn symbols(&self, col_name: String) -> Result<Vec<String>> {
        let col = self
            .inner
            .header
            .fields
            .iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("Column '{}' not found", col_name),
                )
            })?;
        Ok(self.inner.symbols[col]
            .iter()
            .map(|s| s.to_string_repr())
            .collect())
    }

    /// Number of unique symbols in a column.
    #[napi]
    pub fn num_symbols(&self, col_name: String) -> Result<u32> {
        let col = self
            .inner
            .header
            .fields
            .iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("Column '{}' not found", col_name),
                )
            })?;
        Ok(self.inner.symbols[col].len() as u32)
    }

    /// Filter rows where column matches any of the given values.
    #[napi]
    pub fn filter_by_values(&self, col_name: String, values: Vec<String>) -> Result<JsQvdTable> {
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let matching = self.inner.filter_by_values(&col_name, &refs);
        let filtered = self.inner.subset_rows(&matching);
        Ok(JsQvdTable { inner: filtered })
    }

    /// Create a new table from a subset of row indices.
    #[napi]
    pub fn subset_rows(&self, row_indices: Vec<u32>) -> JsQvdTable {
        let indices: Vec<usize> = row_indices.iter().map(|&i| i as usize).collect();
        JsQvdTable {
            inner: self.inner.subset_rows(&indices),
        }
    }

    /// Normalize for maximum Qlik Sense compatibility.
    #[napi]
    pub fn normalize(&mut self) {
        self.inner.normalize();
    }

    /// Concatenate with another table (pure append).
    #[napi]
    pub fn concatenate(
        &self,
        other: &JsQvdTable,
        schema: Option<String>,
    ) -> Result<JsQvdTable> {
        let mode = parse_schema_mode(schema.as_deref().unwrap_or("strict"))?;
        let result = crate::concat::concatenate_with_schema(&self.inner, &other.inner, mode)
            .map_err(to_napi_err)?;
        Ok(JsQvdTable { inner: result })
    }

    /// Concatenate with PK-based deduplication.
    #[napi]
    pub fn concatenate_pk(
        &self,
        other: &JsQvdTable,
        pk: Vec<String>,
        on_conflict: Option<String>,
        schema: Option<String>,
    ) -> Result<JsQvdTable> {
        let mode = parse_schema_mode(schema.as_deref().unwrap_or("strict"))?;
        let conflict = parse_on_conflict(on_conflict.as_deref().unwrap_or("replace"))?;
        let pk_refs: Vec<&str> = pk.iter().map(|s| s.as_str()).collect();
        let result = crate::concat::concatenate_with_pk_schema(
            &self.inner,
            &other.inner,
            &pk_refs,
            conflict,
            mode,
        )
        .map_err(to_napi_err)?;
        Ok(JsQvdTable { inner: result })
    }
}

// ── JsExistsIndex ────────────────────────────────────────────────

#[napi]
pub struct JsExistsIndex {
    values: HashSet<String>,
}

#[napi]
impl JsExistsIndex {
    /// Build from a QvdTable column.
    #[napi(factory)]
    pub fn from_column(table: &JsQvdTable, col_name: String) -> Result<Self> {
        let col = table
            .inner
            .header
            .fields
            .iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("Column '{}' not found", col_name),
                )
            })?;
        let mut values = HashSet::with_capacity(table.inner.symbols[col].len());
        for sym in &table.inner.symbols[col] {
            values.insert(sym.to_string_repr());
        }
        Ok(JsExistsIndex { values })
    }

    /// Build from an explicit list of values.
    #[napi(factory)]
    pub fn from_values(values: Vec<String>) -> Self {
        let set: HashSet<String> = values.into_iter().collect();
        JsExistsIndex { values: set }
    }

    /// Check if a value exists (O(1)).
    #[napi]
    pub fn exists(&self, value: String) -> bool {
        self.values.contains(&value)
    }

    /// Check multiple values.
    #[napi]
    pub fn exists_many(&self, values: Vec<String>) -> Vec<bool> {
        values
            .iter()
            .map(|v| self.values.contains(v.as_str()))
            .collect()
    }

    /// Number of unique values.
    #[napi(getter)]
    pub fn len(&self) -> u32 {
        self.values.len() as u32
    }

    /// Whether the index is empty.
    #[napi(getter)]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

// ── Async tasks ──────────────────────────────────────────────────

pub struct ReadQvdTask {
    path: String,
}

#[napi]
impl Task for ReadQvdTask {
    type Output = reader::QvdTable;
    type JsValue = JsQvdTable;

    fn compute(&mut self) -> Result<Self::Output> {
        reader::read_qvd_file(&self.path).map_err(to_napi_err)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(JsQvdTable { inner: output })
    }
}

pub struct WriteQvdTask {
    table: reader::QvdTable,
    path: String,
}

#[napi]
impl Task for WriteQvdTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        writer::write_qvd_file(&self.table, &self.path).map_err(to_napi_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

pub struct ReadFilteredTask {
    path: String,
    filter_col: String,
    values: Vec<String>,
    select: Option<Vec<String>>,
    chunk_size: usize,
}

#[napi]
impl Task for ReadFilteredTask {
    type Output = reader::QvdTable;
    type JsValue = JsQvdTable;

    fn compute(&mut self) -> Result<Self::Output> {
        let index =
            ExistsIndex::from_values(&self.values.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let mut stream = streaming::open_qvd_stream(&self.path).map_err(to_napi_err)?;
        let select_refs: Option<Vec<&str>> = self
            .select
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        stream
            .read_filtered(
                &self.filter_col,
                &index,
                select_refs.as_deref(),
                self.chunk_size,
            )
            .map_err(to_napi_err)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(JsQvdTable { inner: output })
    }
}

pub struct ConcatenateQvdTask {
    path_a: String,
    path_b: String,
    output_path: String,
    schema: SchemaMode,
}

#[napi]
impl Task for ConcatenateQvdTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let a = reader::read_qvd_file(&self.path_a).map_err(to_napi_err)?;
        let b = reader::read_qvd_file(&self.path_b).map_err(to_napi_err)?;
        let merged = crate::concat::concatenate_with_schema(&a, &b, self.schema).map_err(to_napi_err)?;
        writer::write_qvd_file(&merged, &self.output_path).map_err(to_napi_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

pub struct ConcatenatePkQvdTask {
    path_a: String,
    path_b: String,
    output_path: String,
    pk: Vec<String>,
    on_conflict: OnConflict,
    schema: SchemaMode,
}

#[napi]
impl Task for ConcatenatePkQvdTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let a = reader::read_qvd_file(&self.path_a).map_err(to_napi_err)?;
        let b = reader::read_qvd_file(&self.path_b).map_err(to_napi_err)?;
        let pk_refs: Vec<&str> = self.pk.iter().map(|s| s.as_str()).collect();
        let merged = crate::concat::concatenate_with_pk_schema(
            &a,
            &b,
            &pk_refs,
            self.on_conflict,
            self.schema,
        )
        .map_err(to_napi_err)?;
        writer::write_qvd_file(&merged, &self.output_path).map_err(to_napi_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── Module-level functions ───────────────────────────────────────

/// Read a QVD file asynchronously. Returns Promise<QvdTable>.
#[napi]
pub fn read_qvd(path: String) -> AsyncTask<ReadQvdTask> {
    AsyncTask::new(ReadQvdTask { path })
}

/// Read a QVD file synchronously (blocks the event loop — use for scripts/CLI).
#[napi]
pub fn read_qvd_sync(path: String) -> Result<JsQvdTable> {
    let table = reader::read_qvd_file(&path).map_err(to_napi_err)?;
    Ok(JsQvdTable { inner: table })
}

/// Save a QvdTable to a file asynchronously. Returns Promise<void>.
#[napi]
pub fn save_qvd(table: &JsQvdTable, path: String) -> AsyncTask<WriteQvdTask> {
    AsyncTask::new(WriteQvdTask {
        table: table.inner.clone(),
        path,
    })
}

/// Save a QvdTable to a file synchronously.
#[napi]
pub fn save_qvd_sync(table: &JsQvdTable, path: String) -> Result<()> {
    writer::write_qvd_file(&table.inner, &path).map_err(to_napi_err)
}

/// Filter rows where column value exists in the index. Returns matching row indices.
#[napi]
pub fn filter_exists(table: &JsQvdTable, col_name: String, index: &JsExistsIndex) -> Result<Vec<u32>> {
    let col_idx = table
        .inner
        .header
        .fields
        .iter()
        .position(|f| f.field_name == col_name)
        .ok_or_else(|| {
            Error::new(
                Status::InvalidArg,
                format!("Column '{}' not found", col_name),
            )
        })?;

    let symbol_matches: Vec<bool> = table.inner.symbols[col_idx]
        .iter()
        .map(|sym| index.values.contains(&sym.to_string_repr()))
        .collect();

    let mut matching = Vec::new();
    for row in 0..table.inner.num_rows() {
        let sym_idx = table.inner.row_indices[col_idx][row];
        if sym_idx >= 0 {
            let si = sym_idx as usize;
            if si < symbol_matches.len() && symbol_matches[si] {
                matching.push(row as u32);
            }
        }
    }
    Ok(matching)
}

/// Read QVD with streaming EXISTS() filter. Returns Promise<QvdTable>.
#[napi]
pub fn read_qvd_filtered(
    path: String,
    filter_col: String,
    values: Vec<String>,
    select: Option<Vec<String>>,
    chunk_size: Option<u32>,
) -> AsyncTask<ReadFilteredTask> {
    AsyncTask::new(ReadFilteredTask {
        path,
        filter_col,
        values,
        select,
        chunk_size: chunk_size.unwrap_or(65536) as usize,
    })
}

/// Concatenate two QVD files and write result. Returns Promise<void>.
#[napi]
pub fn concatenate_qvd(
    path_a: String,
    path_b: String,
    output_path: String,
    schema: Option<String>,
) -> Result<AsyncTask<ConcatenateQvdTask>> {
    let mode = parse_schema_mode(schema.as_deref().unwrap_or("strict"))?;
    Ok(AsyncTask::new(ConcatenateQvdTask {
        path_a,
        path_b,
        output_path,
        schema: mode,
    }))
}

/// Concatenate two QVD files with PK dedup and write result. Returns Promise<void>.
#[napi]
pub fn concatenate_pk_qvd(
    path_a: String,
    path_b: String,
    output_path: String,
    pk: Vec<String>,
    on_conflict: Option<String>,
    schema: Option<String>,
) -> Result<AsyncTask<ConcatenatePkQvdTask>> {
    let mode = parse_schema_mode(schema.as_deref().unwrap_or("strict"))?;
    let conflict = parse_on_conflict(on_conflict.as_deref().unwrap_or("replace"))?;
    Ok(AsyncTask::new(ConcatenatePkQvdTask {
        path_a,
        path_b,
        output_path,
        pk,
        on_conflict: conflict,
        schema: mode,
    }))
}
