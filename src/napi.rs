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

/// In-memory representation of a QVD file.
///
/// Load via {@link readQvd} (async) or {@link readQvdSync}, persist via
/// {@link saveQvd}/{@link saveQvdSync}. Use the `columnValues*` / `toJson` /
/// `head` methods to inspect data, and `concatenate*` / `filterByValues` /
/// `subsetRows` to derive new tables without mutating the source.
#[napi]
pub struct JsQvdTable {
    inner: reader::QvdTable,
}

#[napi]
impl JsQvdTable {
    /// Number of rows in the table.
    ///
    /// Safe as a `u32` — Qlik limits QVD tables to ~2 billion rows.
    ///
    /// @returns Row count.
    #[napi(getter)]
    pub fn num_rows(&self) -> u32 {
        self.inner.num_rows() as u32
    }

    /// Number of columns in the table.
    ///
    /// @returns Column count.
    #[napi(getter)]
    pub fn num_cols(&self) -> u32 {
        self.inner.num_cols() as u32
    }

    /// Table name as stored in the QVD header metadata.
    ///
    /// @returns Table name.
    #[napi(getter)]
    pub fn table_name(&self) -> String {
        self.inner.header.table_name.clone()
    }

    /// Column names in declaration order.
    ///
    /// @returns Column names.
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
    ///
    /// @param row - Zero-based row index.
    /// @param col - Zero-based column index.
    /// @returns Cell value as string, or `null` for NULL.
    /// @throws If `row` or `col` is out of bounds.
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
    ///
    /// @param row - Zero-based row index.
    /// @param colName - Column name.
    /// @returns Cell value as string, or `null` for NULL.
    /// @throws If `colName` does not exist in the table.
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
    ///
    /// @param col - Zero-based column index.
    /// @returns Column values, with `null` for NULL.
    /// @throws If `col` is out of bounds.
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
    ///
    /// @param colName - Column name.
    /// @returns Column values, with `null` for NULL.
    /// @throws If `colName` does not exist in the table.
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

    /// Convert the table to an array of row objects.
    ///
    /// Each row is rendered as `{ columnName: value | null, ... }`.
    ///
    /// @returns One object per row, keyed by column name.
    ///
    /// @example
    /// ```typescript
    /// const rows = table.toJson();
    /// // [{ Country: "DE", Sales: "100" }, ...]
    /// ```
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

    /// Return the first `n` rows as an array of objects.
    ///
    /// @param n - Number of rows to return. Defaults to `10`. Capped at the
    /// table's row count.
    /// @returns One object per row, keyed by column name.
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

    /// Get the unique symbols (distinct values) of a column.
    ///
    /// @param colName - Column name.
    /// @returns Distinct symbol values in QVD storage order.
    /// @throws If `colName` does not exist in the table.
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
    ///
    /// @param colName - Column name.
    /// @returns Distinct symbol count.
    /// @throws If `colName` does not exist in the table.
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

    /// Filter rows where a column value matches any of the given values.
    ///
    /// @param colName - Column to filter on.
    /// @param values - Values that should be kept.
    /// @returns New table containing only the matching rows. The source table
    /// is not modified.
    ///
    /// @example
    /// ```typescript
    /// const filtered = table.filterByValues("%Action_ID", ["7", "9"]);
    /// ```
    #[napi]
    pub fn filter_by_values(&self, col_name: String, values: Vec<String>) -> Result<JsQvdTable> {
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let matching = self.inner.filter_by_values(&col_name, &refs);
        let filtered = self.inner.subset_rows(&matching);
        Ok(JsQvdTable { inner: filtered })
    }

    /// Create a new table from a subset of row indices.
    ///
    /// @param rowIndices - Zero-based row indices to keep, in the desired
    /// output order. Indices may be repeated.
    /// @returns New table containing only the selected rows.
    ///
    /// @example
    /// ```typescript
    /// const rows = filterExists(table, "ClientID", idx);
    /// const subset = table.subsetRows(rows);
    /// ```
    #[napi]
    pub fn subset_rows(&self, row_indices: Vec<u32>) -> JsQvdTable {
        let indices: Vec<usize> = row_indices.iter().map(|&i| i as usize).collect();
        JsQvdTable {
            inner: self.inner.subset_rows(&indices),
        }
    }

    /// Normalize the table for maximum Qlik Sense compatibility.
    ///
    /// Converts `DualInt` → `Int` and `DualDouble` → `Double` and sets proper
    /// `NumberFormat`, `Tags` and `BitWidth` on each field. Modifies the table
    /// in place.
    #[napi]
    pub fn normalize(&mut self) {
        self.inner.normalize();
    }

    /// Concatenate another table into this one (pure append, no deduplication).
    ///
    /// @param other - Table whose rows are appended.
    /// @param schema - `"strict"` errors on column mismatch, `"union"` fills
    /// missing columns with NULL. Defaults to `"strict"`.
    /// @returns New merged table. Inputs are not modified.
    /// @throws On invalid `schema` value or column mismatch under `"strict"`.
    ///
    /// @example
    /// ```typescript
    /// const merged = tableA.concatenate(tableB);
    /// const mergedUnion = tableA.concatenate(tableB, "union");
    /// ```
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

    /// Concatenate another table with primary-key deduplication.
    ///
    /// @param other - Table whose rows are appended.
    /// @param pk - Primary-key columns. Pass `[col]` for a single key or
    /// `[col1, col2, ...]` for a composite key.
    /// @param onConflict - Behaviour when a PK collision is detected.
    /// `"replace"` lets new rows win, `"skip"` keeps existing rows, `"error"`
    /// throws. Defaults to `"replace"`.
    /// @param schema - `"strict"` errors on column mismatch, `"union"` fills
    /// missing columns with NULL. Defaults to `"strict"`.
    /// @returns New merged table. Inputs are not modified.
    /// @throws On invalid `onConflict`/`schema` values, missing PK column, or
    /// PK collision when `onConflict` is `"error"`.
    ///
    /// @example
    /// ```typescript
    /// const merged = existing.concatenatePk(newRows, ["orderId"]);
    /// const skipped = existing.concatenatePk(
    ///   newRows,
    ///   ["a", "b"],
    ///   "skip",
    ///   "union",
    /// );
    /// ```
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

/// Fast O(1) lookup index over a set of string values.
///
/// Used by {@link filterExists} and by the streaming entry point
/// {@link readQvdFiltered}. Build either from a {@link JsQvdTable} column via
/// {@link JsExistsIndex.fromColumn}, or from an explicit list via
/// {@link JsExistsIndex.fromValues}.
#[napi]
pub struct JsExistsIndex {
    values: HashSet<String>,
}

#[napi]
impl JsExistsIndex {
    /// Build a {@link JsExistsIndex} from a {@link JsQvdTable} column.
    ///
    /// @param table - Source table.
    /// @param colName - Column to index.
    /// @returns New index containing the column's distinct values.
    /// @throws If `colName` is not a column of `table`.
    ///
    /// @example
    /// ```typescript
    /// const idx = JsExistsIndex.fromColumn(table, "ClientID");
    /// ```
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

    /// Build a {@link JsExistsIndex} from an explicit list of values.
    ///
    /// @param values - Values to index. Duplicates are deduplicated.
    /// @returns New index containing the provided values.
    ///
    /// @example
    /// ```typescript
    /// const idx = JsExistsIndex.fromValues(["7", "9"]);
    /// ```
    #[napi(factory)]
    pub fn from_values(values: Vec<String>) -> Self {
        let set: HashSet<String> = values.into_iter().collect();
        JsExistsIndex { values: set }
    }

    /// Check whether a single value is in the index.
    ///
    /// O(1) hash lookup.
    ///
    /// @param value - Value to look up.
    /// @returns `true` if present, otherwise `false`.
    #[napi]
    pub fn exists(&self, value: String) -> bool {
        self.values.contains(&value)
    }

    /// Check multiple values at once.
    ///
    /// @param values - Values to look up.
    /// @returns One flag per input value, in the same order.
    #[napi]
    pub fn exists_many(&self, values: Vec<String>) -> Vec<bool> {
        values
            .iter()
            .map(|v| self.values.contains(v.as_str()))
            .collect()
    }

    /// Number of unique values in the index.
    ///
    /// @returns Unique value count.
    #[napi(getter)]
    pub fn len(&self) -> u32 {
        self.values.len() as u32
    }

    /// Whether the index is empty.
    ///
    /// @returns `true` if the index contains no values.
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

/// Read a QVD file asynchronously.
///
/// Resolves with a {@link JsQvdTable} loaded from disk. Heavy I/O runs on a
/// worker thread — the event loop stays free.
///
/// @param path - Path to the `.qvd` file.
/// @returns Promise resolving to the loaded table.
/// @throws If the file cannot be opened or is not a valid QVD.
///
/// @example
/// ```typescript
/// import { readQvd } from "qvdrs";
///
/// const table = await readQvd("data.qvd");
/// console.log(table.numRows);
/// ```
#[napi]
pub fn read_qvd(path: String) -> AsyncTask<ReadQvdTask> {
    AsyncTask::new(ReadQvdTask { path })
}

/// Read a QVD file synchronously.
///
/// Blocks the event loop while the file is being read — use only for scripts
/// or CLIs. Prefer {@link readQvd} in servers.
///
/// @param path - Path to the `.qvd` file.
/// @returns Loaded table.
/// @throws If the file cannot be opened or is not a valid QVD.
#[napi]
pub fn read_qvd_sync(path: String) -> Result<JsQvdTable> {
    let table = reader::read_qvd_file(&path).map_err(to_napi_err)?;
    Ok(JsQvdTable { inner: table })
}

/// Save a {@link JsQvdTable} to a file asynchronously.
///
/// If the table has not been modified since loading, the output is a
/// byte-identical roundtrip of the source file.
///
/// @param table - Table to persist.
/// @param path - Destination `.qvd` path.
/// @returns Promise resolving when the file has been written.
/// @throws If the file cannot be written.
#[napi]
pub fn save_qvd(table: &JsQvdTable, path: String) -> AsyncTask<WriteQvdTask> {
    AsyncTask::new(WriteQvdTask {
        table: table.inner.clone(),
        path,
    })
}

/// Save a {@link JsQvdTable} to a file synchronously.
///
/// Blocks the event loop while writing. Prefer {@link saveQvd} in servers.
///
/// @param table - Table to persist.
/// @param path - Destination `.qvd` path.
/// @throws If the file cannot be written.
#[napi]
pub fn save_qvd_sync(table: &JsQvdTable, path: String) -> Result<()> {
    writer::write_qvd_file(&table.inner, &path).map_err(to_napi_err)
}

/// Filter rows where a column value is in a {@link JsExistsIndex}.
///
/// @param table - Table to filter.
/// @param colName - Column to look up in `index`.
/// @param index - Lookup index.
/// @returns Row indices of matching rows, in ascending order.
/// @throws If `colName` is not a column of `table`.
///
/// @example
/// ```typescript
/// const idx = JsExistsIndex.fromValues(["7", "9"]);
/// const rows = filterExists(table, "%Action_ID", idx);
/// const subset = table.subsetRows(rows);
/// ```
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

/// Read a QVD file asynchronously with a streaming EXISTS() filter.
///
/// Only matching rows are loaded into memory — much faster and lighter than
/// reading the entire file for large inputs.
///
/// @param path - Path to the `.qvd` file.
/// @param filterCol - Column name to filter on.
/// @param values - Values to keep. A row is kept iff its `filterCol` value is
/// in this list.
/// @param select - Subset of columns to load. Pass `undefined` to load all
/// columns.
/// @param chunkSize - Streaming chunk size in rows. Defaults to `65536`.
/// @returns Promise resolving to a table containing only the matching rows.
/// @throws If the file cannot be read or `filterCol` does not exist.
///
/// @example
/// ```typescript
/// import { readQvdFiltered } from "qvdrs";
///
/// const table = await readQvdFiltered(
///   "large.qvd",
///   "%Action_ID",
///   ["7", "9"],
///   ["%Client_ID", "Date_BK", "%Action_ID"],
/// );
/// ```
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

/// Concatenate two QVD files into a new QVD file asynchronously (pure append).
///
/// @param pathA - Path to the existing `.qvd` file.
/// @param pathB - Path to the file whose rows are appended.
/// @param outputPath - Destination `.qvd` path.
/// @param schema - `"strict"` errors on column mismatch, `"union"` fills
/// missing columns with NULL. Defaults to `"strict"`.
/// @returns Promise resolving when the merged file has been written.
/// @throws On invalid `schema`, unreadable input, or column mismatch under
/// `"strict"`.
///
/// @example
/// ```typescript
/// import { concatenateQvd } from "qvdrs";
///
/// await concatenateQvd("existing.qvd", "new_data.qvd", "merged.qvd");
/// ```
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

/// Concatenate two QVD files asynchronously with primary-key deduplication.
///
/// @param pathA - Path to the existing `.qvd` file.
/// @param pathB - Path to the file whose rows are appended.
/// @param outputPath - Destination `.qvd` path.
/// @param pk - Primary-key columns. Pass `[col]` for a single key or
/// `[col1, col2, ...]` for a composite key.
/// @param onConflict - Behaviour when a PK collision is detected.
/// `"replace"` lets new rows win, `"skip"` keeps existing rows, `"error"`
/// throws. Defaults to `"replace"`.
/// @param schema - `"strict"` errors on column mismatch, `"union"` fills
/// missing columns with NULL. Defaults to `"strict"`.
/// @returns Promise resolving when the merged file has been written.
/// @throws On invalid `onConflict`/`schema` values, unreadable input,
/// missing PK column, or PK collision when `onConflict` is `"error"`.
///
/// @example
/// ```typescript
/// import { concatenatePkQvd } from "qvdrs";
///
/// await concatenatePkQvd(
///   "existing.qvd",
///   "new.qvd",
///   "out.qvd",
///   ["orderId"],
/// );
/// ```
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
