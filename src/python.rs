use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyImportError};
use pyo3::types::{PyDict, PyList};
use std::collections::HashSet;

use arrow::pyarrow::{ToPyArrow, FromPyArrow};
use arrow::record_batch::RecordBatch;

use crate::reader;
use crate::writer;

/// Python wrapper around QvdTable.
#[pyclass(name = "QvdTable")]
pub struct PyQvdTable {
    inner: reader::QvdTable,
}

#[pymethods]
impl PyQvdTable {
    /// Read a QVD file from disk.
    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let table = reader::read_qvd_file(path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Save the QVD table to a file (byte-identical roundtrip if unmodified).
    fn save(&self, path: &str) -> PyResult<()> {
        writer::write_qvd_file(&self.inner, path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))
    }

    /// Table name from metadata.
    #[getter]
    fn table_name(&self) -> &str {
        &self.inner.header.table_name
    }

    /// Number of rows.
    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of columns.
    #[getter]
    fn num_cols(&self) -> usize {
        self.inner.num_cols()
    }

    /// Column names as a list of strings.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.inner.header.fields.iter().map(|f| f.field_name.clone()).collect()
    }

    /// Get a single cell value by row and column index.
    fn get(&self, row: usize, col: usize) -> PyResult<Option<String>> {
        if row >= self.inner.num_rows() || col >= self.inner.num_cols() {
            return Err(PyValueError::new_err("Index out of bounds"));
        }
        Ok(self.inner.get(row, col).as_string())
    }

    /// Get a single cell value by row index and column name.
    fn get_by_name(&self, row: usize, col_name: &str) -> PyResult<Option<String>> {
        match self.inner.get_by_name(row, col_name) {
            Some(val) => Ok(val.as_string()),
            None => Err(PyValueError::new_err(format!("Column '{}' not found", col_name))),
        }
    }

    /// Get all values of a column as a list of strings (None for NULL).
    fn column_values(&self, col: usize) -> PyResult<Vec<Option<String>>> {
        if col >= self.inner.num_cols() {
            return Err(PyValueError::new_err("Column index out of bounds"));
        }
        Ok(self.inner.column_strings(col))
    }

    /// Get all values of a column by name.
    fn column_values_by_name(&self, col_name: &str) -> PyResult<Vec<Option<String>>> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.column_strings(col))
    }

    /// Convert to a Python dict of {column_name: [values...]}.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (col_idx, field) in self.inner.header.fields.iter().enumerate() {
            let values = self.inner.column_strings(col_idx);
            let py_list = PyList::new(py, values.iter().map(|v| v.as_deref()))?;
            dict.set_item(&field.field_name, py_list)?;
        }
        Ok(dict)
    }

    /// Get unique symbols (distinct values) for a column.
    fn symbols(&self, col_name: &str) -> PyResult<Vec<String>> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.symbols[col].iter().map(|s| s.to_string_repr()).collect())
    }

    /// Number of unique values (symbols) in a column.
    fn num_symbols(&self, col_name: &str) -> PyResult<usize> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.symbols[col].len())
    }

    /// Get first N rows as a list of dicts.
    #[pyo3(signature = (n=None))]
    fn head<'a>(&self, py: Python<'a>, n: Option<usize>) -> PyResult<Bound<'a, PyList>> {
        let n = n.unwrap_or(10).min(self.inner.num_rows());
        let rows = PyList::empty(py);
        for row in 0..n {
            let dict = PyDict::new(py);
            for (col, field) in self.inner.header.fields.iter().enumerate() {
                let val = self.inner.get(row, col).as_string();
                dict.set_item(&field.field_name, val)?;
            }
            rows.append(dict)?;
        }
        Ok(rows)
    }

    /// Load a Parquet file and convert it to a QvdTable in memory.
    #[staticmethod]
    fn from_parquet(path: &str) -> PyResult<Self> {
        let table = crate::parquet::read_parquet_to_qvd(path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Save this QvdTable as a Parquet file.
    /// compression: "none", "snappy", "gzip", "lz4", "zstd" (default: "snappy")
    #[pyo3(signature = (path, compression=None))]
    fn save_as_parquet(&self, path: &str, compression: Option<&str>) -> PyResult<()> {
        let comp = crate::parquet::ParquetCompression::parse(compression.unwrap_or("snappy"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        crate::parquet::write_qvd_table_to_parquet(&self.inner, path, comp)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))
    }

    /// Convert to a PyArrow RecordBatch (zero-copy via Arrow C Data Interface).
    ///
    /// Requires `pyarrow` to be installed.
    ///
    /// ```python
    /// table = qvd.read_qvd("data.qvd")
    /// batch = table.to_arrow()
    /// ```
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let batch = crate::parquet::qvd_to_record_batch(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        batch.to_pyarrow(py)
    }

    /// Create a QvdTable from a PyArrow RecordBatch.
    ///
    /// ```python
    /// table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
    /// table.save("output.qvd")
    /// ```
    #[staticmethod]
    #[pyo3(signature = (batch, table_name=None))]
    fn from_arrow(batch: &Bound<'_, PyAny>, table_name: Option<&str>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(batch)
            .map_err(|e| PyValueError::new_err(format!("Invalid RecordBatch: {}", e)))?;
        let table = crate::parquet::record_batch_to_qvd(&batch, table_name.unwrap_or("table"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Convert to a pandas DataFrame.
    ///
    /// Requires `pyarrow` and `pandas` to be installed.
    ///
    /// ```python
    /// df = qvd.read_qvd("data.qvd").to_pandas()
    /// ```
    fn to_pandas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let batch = crate::parquet::qvd_to_record_batch(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        let pyarrow_batch = batch.to_pyarrow(py)?;
        let pa = py.import("pyarrow")
            .map_err(|_| PyImportError::new_err("pyarrow is required: pip install pyarrow"))?;
        let pa_table_cls = pa.getattr("Table")?;
        let table = pa_table_cls.call_method1("from_batches", (vec![pyarrow_batch],))?;
        let df = table.call_method0("to_pandas")?;
        Ok(df)
    }

    /// Convert to a Polars DataFrame.
    ///
    /// Requires `pyarrow` and `polars` to be installed.
    ///
    /// ```python
    /// df = qvd.read_qvd("data.qvd").to_polars()
    /// ```
    fn to_polars<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let batch = crate::parquet::qvd_to_record_batch(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        let pyarrow_batch = batch.to_pyarrow(py)?;
        let pa = py.import("pyarrow")
            .map_err(|_| PyImportError::new_err("pyarrow is required: pip install pyarrow"))?;
        let pa_table_cls = pa.getattr("Table")?;
        let table = pa_table_cls.call_method1("from_batches", (vec![pyarrow_batch],))?;
        let pl = py.import("polars")
            .map_err(|_| PyImportError::new_err("polars is required: pip install polars"))?;
        let df = pl.call_method1("from_arrow", (table,))?;
        Ok(df)
    }

    /// Filter rows where column matches any of the given values.
    /// Returns a new QvdTable with only matching rows.
    ///
    /// ```python
    /// filtered = table.filter_by_values("%Action_ID", ["7", "9"])
    /// ```
    fn filter_by_values(&self, col_name: &str, values: Vec<String>) -> PyResult<PyQvdTable> {
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let matching = self.inner.filter_by_values(col_name, &refs);
        if matching.is_empty() {
            // Return empty table with same structure
            let filtered = self.inner.subset_rows(&[]);
            return Ok(PyQvdTable { inner: filtered });
        }
        let filtered = self.inner.subset_rows(&matching);
        Ok(PyQvdTable { inner: filtered })
    }

    /// Create a new QvdTable from a subset of row indices.
    ///
    /// ```python
    /// rows = qvd.filter_exists(table, "ClientID", idx)
    /// subset = table.subset_rows(rows)
    /// ```
    fn subset_rows(&self, row_indices: Vec<usize>) -> PyResult<PyQvdTable> {
        let filtered = self.inner.subset_rows(&row_indices);
        Ok(PyQvdTable { inner: filtered })
    }

    /// Normalize the table for maximum Qlik Sense compatibility.
    /// Converts DualInt→Int, DualDouble→Double, sets proper NumberFormat/Tags/BitWidth.
    fn normalize(&mut self) {
        self.inner.normalize();
    }

    fn __repr__(&self) -> String {
        format!(
            "QvdTable(table='{}', rows={}, cols={})",
            self.inner.header.table_name,
            self.inner.num_rows(),
            self.inner.num_cols()
        )
    }

    fn __len__(&self) -> usize {
        self.inner.num_rows()
    }
}

/// Python wrapper around ExistsIndex for fast O(1) lookups.
#[pyclass(name = "ExistsIndex")]
pub struct PyExistsIndex {
    values: HashSet<String>,
    col_name: String,
}

#[pymethods]
impl PyExistsIndex {
    /// Build an ExistsIndex from a QvdTable column.
    ///
    /// ```python
    /// idx = qvd.ExistsIndex(table, "ClientID")
    /// ```
    #[new]
    fn new(table: &PyQvdTable, col_name: &str) -> PyResult<Self> {
        let col = table.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        let mut values = HashSet::with_capacity(table.inner.symbols[col].len());
        for sym in &table.inner.symbols[col] {
            values.insert(sym.to_string_repr());
        }

        Ok(PyExistsIndex {
            values,
            col_name: col_name.to_string(),
        })
    }

    /// Build an ExistsIndex from an explicit list of values.
    ///
    /// ```python
    /// idx = qvd.ExistsIndex.from_values(["7", "9"])
    /// ```
    #[staticmethod]
    fn from_values(values: Vec<String>) -> Self {
        let set: HashSet<String> = values.into_iter().collect();
        PyExistsIndex {
            values: set,
            col_name: "<values>".to_string(),
        }
    }

    /// Check if a value exists. O(1) hash lookup.
    fn exists(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    /// Check multiple values at once, returns list of bools.
    fn exists_many(&self, values: Vec<String>) -> Vec<bool> {
        values.iter().map(|v| self.values.contains(v.as_str())).collect()
    }

    /// Number of unique values in the index.
    fn __len__(&self) -> usize {
        self.values.len()
    }

    /// Check if a value exists using `in` operator.
    fn __contains__(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    fn __repr__(&self) -> String {
        format!("ExistsIndex(field='{}', values={})", self.col_name, self.values.len())
    }
}

/// Filter rows from a QVD table where a column value exists in an ExistsIndex.
/// Returns list of matching row indices.
#[pyfunction]
fn filter_exists(table: &PyQvdTable, col_name: &str, index: &PyExistsIndex) -> PyResult<Vec<usize>> {
    let col_idx = table.inner.header.fields.iter()
        .position(|f| f.field_name == col_name)
        .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;

    // Fast path: pre-check which symbols match, then scan indices
    let symbol_matches: Vec<bool> = table.inner.symbols[col_idx]
        .iter()
        .map(|sym| index.values.contains(&sym.to_string_repr()))
        .collect();

    let mut matching_rows = Vec::new();
    for row in 0..table.inner.num_rows() {
        let sym_idx = table.inner.row_indices[col_idx][row];
        if sym_idx >= 0 {
            let sym_idx = sym_idx as usize;
            if sym_idx < symbol_matches.len() && symbol_matches[sym_idx] {
                matching_rows.push(row);
            }
        }
    }
    Ok(matching_rows)
}

/// Read a QVD file and return a QvdTable.
#[pyfunction]
fn read_qvd(path: &str) -> PyResult<PyQvdTable> {
    PyQvdTable::load(path)
}

/// Read a QVD file and return a PyArrow RecordBatch directly.
#[pyfunction]
fn read_qvd_to_arrow<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let table = reader::read_qvd_file(path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let batch = crate::parquet::qvd_to_record_batch(&table)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    batch.to_pyarrow(py)
}

/// Read a QVD file and return a pandas DataFrame directly.
#[pyfunction]
fn read_qvd_to_pandas<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let t = PyQvdTable::load(path)?;
    t.to_pandas(py)
}

/// Read a QVD file and return a Polars DataFrame directly.
#[pyfunction]
fn read_qvd_to_polars<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let t = PyQvdTable::load(path)?;
    t.to_polars(py)
}

/// Convert a Parquet file to a QVD file.
#[pyfunction]
fn convert_parquet_to_qvd(parquet_path: &str, qvd_path: &str) -> PyResult<()> {
    crate::parquet::convert_parquet_to_qvd(parquet_path, qvd_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Convert a QVD file to a Parquet file.
/// compression: "none", "snappy", "gzip", "lz4", "zstd" (default: "snappy")
#[pyfunction]
#[pyo3(signature = (qvd_path, parquet_path, compression=None))]
fn convert_qvd_to_parquet(qvd_path: &str, parquet_path: &str, compression: Option<&str>) -> PyResult<()> {
    let comp = crate::parquet::ParquetCompression::parse(compression.unwrap_or("snappy"))
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    crate::parquet::convert_qvd_to_parquet(qvd_path, parquet_path, comp)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Read a QVD file with streaming EXISTS() filter and optional column selection.
/// Only matching rows are loaded into memory — much faster and lighter for large files.
///
/// ```python
/// idx = qvd.ExistsIndex.from_values(["7", "9"])
/// table = qvd.read_qvd_filtered("large.qvd", "%Action_ID", idx,
///                                 select=["%Client_ID", "Date_BK", "%Action_ID"])
/// table.save("filtered.qvd")
/// ```
#[pyfunction]
#[pyo3(signature = (path, filter_col, index, select=None, chunk_size=None))]
fn read_qvd_filtered(
    path: &str,
    filter_col: &str,
    index: &PyExistsIndex,
    select: Option<Vec<String>>,
    chunk_size: Option<usize>,
) -> PyResult<PyQvdTable> {
    let rust_index = crate::exists::ExistsIndex::from_values(
        &index.values.iter().map(|s| s.as_str()).collect::<Vec<_>>()
    );
    let mut stream = crate::streaming::open_qvd_stream(path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let select_refs: Option<Vec<&str>> = select.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
    let chunk = chunk_size.unwrap_or(65536);
    let table = stream.read_filtered(filter_col, &rust_index, select_refs.as_deref(), chunk)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    Ok(PyQvdTable { inner: table })
}

/// Register a QVD file as a DuckDB table. One call — then query with SQL.
///
/// ```python
/// import qvd, duckdb
/// conn = duckdb.connect()
/// qvd.register_duckdb(conn, "sales", "sales.qvd")
/// conn.sql("SELECT * FROM sales WHERE amount > 100").show()
/// ```
#[pyfunction]
fn register_duckdb<'py>(conn: &Bound<'py, PyAny>, table_name: &str, path: &str) -> PyResult<()> {
    let py = conn.py();
    let table = reader::read_qvd_file(path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let batch = crate::parquet::qvd_to_record_batch(&table)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let pyarrow_batch = batch.to_pyarrow(py)?;

    // Convert RecordBatch to PyArrow Table (DuckDB prefers Table for register)
    let pa = py.import("pyarrow")
        .map_err(|_| PyImportError::new_err("pyarrow is required: pip install pyarrow"))?;
    let pa_table_cls = pa.getattr("Table")?;
    let arrow_table = pa_table_cls.call_method1("from_batches", (vec![pyarrow_batch],))?;

    conn.call_method1("register", (table_name, arrow_table))?;
    Ok(())
}

/// Register a QVD file as a DuckDB table with streaming EXISTS() filter.
/// Only matching rows are loaded — memory-efficient for large files.
///
/// ```python
/// import qvd, duckdb
/// conn = duckdb.connect()
/// idx = qvd.ExistsIndex.from_values(["7", "9"])
/// qvd.register_duckdb_filtered(conn, "cal79", "large.qvd",
///                               "%Action_ID", idx,
///                               select=["%Client_ID", "Date_BK", "%Action_ID"])
/// conn.sql("SELECT COUNT(*) FROM cal79").show()
/// ```
#[pyfunction]
#[pyo3(signature = (conn, table_name, path, filter_col, index, select=None, chunk_size=None))]
fn register_duckdb_filtered<'py>(
    conn: &Bound<'py, PyAny>,
    table_name: &str,
    path: &str,
    filter_col: &str,
    index: &PyExistsIndex,
    select: Option<Vec<String>>,
    chunk_size: Option<usize>,
) -> PyResult<()> {
    let py = conn.py();
    let rust_index = crate::exists::ExistsIndex::from_values(
        &index.values.iter().map(|s| s.as_str()).collect::<Vec<_>>()
    );
    let mut stream = crate::streaming::open_qvd_stream(path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let select_refs: Option<Vec<&str>> = select.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
    let chunk = chunk_size.unwrap_or(65536);
    let qvd_table = stream.read_filtered(filter_col, &rust_index, select_refs.as_deref(), chunk)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;

    let batch = crate::parquet::qvd_to_record_batch(&qvd_table)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let pyarrow_batch = batch.to_pyarrow(py)?;

    let pa = py.import("pyarrow")
        .map_err(|_| PyImportError::new_err("pyarrow is required: pip install pyarrow"))?;
    let pa_table_cls = pa.getattr("Table")?;
    let arrow_table = pa_table_cls.call_method1("from_batches", (vec![pyarrow_batch],))?;

    conn.call_method1("register", (table_name, arrow_table))?;
    Ok(())
}

/// Register QVD files from one or more directories as DuckDB tables.
/// Table names are derived from file names (without .qvd extension).
///
/// Args:
///   conn: DuckDB connection
///   folder_paths: single path (str) or list of paths
///   recursive: scan subdirectories (default: False)
///   glob: file pattern to match (default: "*.qvd")
///   max_file_size_mb: skip files larger than this (default: 500 MB)
///
/// Returns: list of registered table names
///
/// ```python
/// import qvd, duckdb
/// conn = duckdb.connect()
///
/// # Single folder
/// tables = qvd.register_duckdb_folder(conn, "/path/to/qvd_files/")
///
/// # Multiple folders
/// tables = qvd.register_duckdb_folder(conn, ["/data/sales/", "/data/crm/"])
///
/// # Recursive with pattern
/// tables = qvd.register_duckdb_folder(conn, "/data/", recursive=True, glob="client_*.qvd")
///
/// conn.sql("SELECT * FROM sales JOIN customers ON ...").show()
/// ```
#[pyfunction]
#[pyo3(signature = (conn, folder_paths, recursive=None, glob=None, max_file_size_mb=None))]
fn register_duckdb_folder<'py>(
    conn: &Bound<'py, PyAny>,
    folder_paths: &Bound<'py, PyAny>,
    recursive: Option<bool>,
    glob: Option<&str>,
    max_file_size_mb: Option<u64>,
) -> PyResult<Vec<String>> {
    let py = conn.py();
    let pa = py.import("pyarrow")
        .map_err(|_| PyImportError::new_err("pyarrow is required: pip install pyarrow"))?;
    let pa_table_cls = pa.getattr("Table")?;

    let recursive = recursive.unwrap_or(false);
    let glob_pattern = glob.unwrap_or("*.qvd");
    let max_size = max_file_size_mb.unwrap_or(500) * 1_048_576;

    // Accept single string or list of strings
    let paths: Vec<String> = if let Ok(s) = folder_paths.extract::<String>() {
        vec![s]
    } else if let Ok(list) = folder_paths.extract::<Vec<String>>() {
        list
    } else {
        return Err(PyValueError::new_err("folder_paths must be a string or list of strings"));
    };

    let mut registered = Vec::new();
    let mut errors = Vec::new();

    for folder in &paths {
        collect_qvd_files(
            std::path::Path::new(folder),
            recursive,
            glob_pattern,
            max_size,
            py,
            conn,
            &pa_table_cls,
            &mut registered,
            &mut errors,
        )?;
    }

    if !errors.is_empty() {
        let py_warnings = py.import("warnings")?;
        for err in &errors {
            py_warnings.call_method1("warn", (err.as_str(),))?;
        }
    }

    registered.sort();
    Ok(registered)
}

fn collect_qvd_files<'py>(
    dir: &std::path::Path,
    recursive: bool,
    glob_pattern: &str,
    max_size: u64,
    py: Python<'py>,
    conn: &Bound<'py, PyAny>,
    pa_table_cls: &Bound<'py, PyAny>,
    registered: &mut Vec<String>,
    errors: &mut Vec<String>,
) -> PyResult<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            errors.push(format!("Cannot read '{}': {}", dir.display(), e));
            return Ok(());
        }
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();

        if path.is_dir() && recursive {
            collect_qvd_files(&path, recursive, glob_pattern, max_size, py, conn, pa_table_cls, registered, errors)?;
            continue;
        }

        if !path.is_file() { continue; }

        let name_lower = path.file_name()
            .map(|n| n.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        if !name_lower.ends_with(".qvd") { continue; }

        // Check glob pattern: supports "prefix*", "*suffix", "prefix*suffix", "*mid*"
        if glob_pattern != "*.qvd" {
            let pattern_lower = glob_pattern.to_lowercase();
            // Strip .qvd from pattern if present — we match against the full filename
            let matches = if let Some(star_pos) = pattern_lower.find('*') {
                let prefix = &pattern_lower[..star_pos];
                let suffix = &pattern_lower[star_pos + 1..];
                if prefix.is_empty() && suffix.is_empty() {
                    // Pattern is just "*"
                    true
                } else if prefix.is_empty() && suffix.starts_with('.') {
                    // Pattern like "*.qvd"
                    name_lower.ends_with(suffix)
                } else if prefix.is_empty() {
                    // Pattern like "*actions*" or "*actions*.qvd"
                    // Remove trailing .qvd from suffix if present, then check contains
                    let inner = suffix.strip_suffix(".qvd").or(suffix.strip_suffix('*')).unwrap_or(suffix);
                    name_lower.contains(inner)
                } else if suffix.is_empty() || suffix == ".qvd" {
                    // Pattern like "prefix*.qvd" or "prefix*"
                    name_lower.starts_with(prefix)
                } else {
                    // Pattern like "prefix*suffix"
                    name_lower.starts_with(prefix) && name_lower.ends_with(suffix)
                }
            } else {
                name_lower == pattern_lower
            };
            if !matches { continue; }
        }

        // Check file size
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        if size > max_size {
            errors.push(format!("Skipped '{}': {} MB exceeds limit", path.display(), size / 1_048_576));
            continue;
        }

        let table_name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let path_str = match path.to_str() {
            Some(s) => s,
            None => { errors.push(format!("Invalid path: {}", path.display())); continue; }
        };

        // Read QVD → Arrow → register in DuckDB
        let qvd_table = match reader::read_qvd_file(path_str) {
            Ok(t) => t,
            Err(e) => { errors.push(format!("{}: {}", table_name, e)); continue; }
        };
        let batch = match crate::parquet::qvd_to_record_batch(&qvd_table) {
            Ok(b) => b,
            Err(e) => { errors.push(format!("{}: {}", table_name, e)); continue; }
        };
        let pyarrow_batch = match batch.to_pyarrow(py) {
            Ok(b) => b,
            Err(e) => { errors.push(format!("{}: {}", table_name, e)); continue; }
        };
        let arrow_table = match pa_table_cls.call_method1("from_batches", (vec![pyarrow_batch],)) {
            Ok(t) => t,
            Err(e) => { errors.push(format!("{}: {}", table_name, e)); continue; }
        };

        match conn.call_method1("register", (&table_name, arrow_table)) {
            Ok(_) => registered.push(table_name),
            Err(e) => errors.push(format!("{}: {}", table_name, e)),
        }
    }

    Ok(())
}

/// Python module definition.
#[pymodule]
fn qvd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyQvdTable>()?;
    m.add_class::<PyExistsIndex>()?;
    m.add_function(wrap_pyfunction!(read_qvd, m)?)?;
    m.add_function(wrap_pyfunction!(filter_exists, m)?)?;
    m.add_function(wrap_pyfunction!(convert_parquet_to_qvd, m)?)?;
    m.add_function(wrap_pyfunction!(convert_qvd_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_qvd_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(read_qvd_to_pandas, m)?)?;
    m.add_function(wrap_pyfunction!(read_qvd_to_polars, m)?)?;
    m.add_function(wrap_pyfunction!(read_qvd_filtered, m)?)?;
    m.add_function(wrap_pyfunction!(register_duckdb, m)?)?;
    m.add_function(wrap_pyfunction!(register_duckdb_filtered, m)?)?;
    m.add_function(wrap_pyfunction!(register_duckdb_folder, m)?)?;
    Ok(())
}
