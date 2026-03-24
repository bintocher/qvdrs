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
    Ok(())
}
