use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyImportError};
use pyo3::types::{PyDict, PyList};
use std::collections::HashSet;

use arrow::pyarrow::{ToPyArrow, FromPyArrow};
use arrow::record_batch::RecordBatch;

use crate::reader;
use crate::writer;

/// In-memory representation of a QVD file.
///
/// Loaded via :func:`read_qvd` or :meth:`QvdTable.load`, persisted via
/// :meth:`save`, and interoperable with PyArrow, pandas, Polars and Parquet
/// through the conversion methods on this class.
#[pyclass(name = "QvdTable")]
pub struct PyQvdTable {
    inner: reader::QvdTable,
}

#[pymethods]
impl PyQvdTable {
    /// Read a QVD file from disk.
    ///
    /// Args:
    ///     path (str): Path to the ``.qvd`` file.
    ///
    /// Returns:
    ///     QvdTable: Loaded table.
    ///
    /// Raises:
    ///     ValueError: If the file cannot be opened or is not a valid QVD.
    ///
    /// Example:
    ///     >>> table = QvdTable.load("data.qvd")
    #[staticmethod]
    fn load(path: &str) -> PyResult<Self> {
        let table = reader::read_qvd_file(path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Save the table back to a QVD file.
    ///
    /// If the table has not been modified since loading, the output is a
    /// byte-identical roundtrip of the source file.
    ///
    /// Args:
    ///     path (str): Destination path for the ``.qvd`` file.
    ///
    /// Raises:
    ///     ValueError: If the file cannot be written.
    fn save(&self, path: &str) -> PyResult<()> {
        writer::write_qvd_file(&self.inner, path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))
    }

    /// Table name as stored in the QVD header metadata.
    ///
    /// Returns:
    ///     str: Table name.
    #[getter]
    fn table_name(&self) -> &str {
        &self.inner.header.table_name
    }

    /// Number of rows in the table.
    ///
    /// Returns:
    ///     int: Row count.
    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of columns in the table.
    ///
    /// Returns:
    ///     int: Column count.
    #[getter]
    fn num_cols(&self) -> usize {
        self.inner.num_cols()
    }

    /// Column names in declaration order.
    ///
    /// Returns:
    ///     list[str]: Column names.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.inner.header.fields.iter().map(|f| f.field_name.clone()).collect()
    }

    /// Get a single cell value by row and column index.
    ///
    /// Args:
    ///     row (int): Zero-based row index.
    ///     col (int): Zero-based column index.
    ///
    /// Returns:
    ///     str | None: Cell value as string, or ``None`` for NULL.
    ///
    /// Raises:
    ///     ValueError: If ``row`` or ``col`` is out of bounds.
    fn get(&self, row: usize, col: usize) -> PyResult<Option<String>> {
        if row >= self.inner.num_rows() || col >= self.inner.num_cols() {
            return Err(PyValueError::new_err("Index out of bounds"));
        }
        Ok(self.inner.get(row, col).as_string())
    }

    /// Get a single cell value by row index and column name.
    ///
    /// Args:
    ///     row (int): Zero-based row index.
    ///     col_name (str): Column name.
    ///
    /// Returns:
    ///     str | None: Cell value as string, or ``None`` for NULL.
    ///
    /// Raises:
    ///     ValueError: If ``col_name`` does not exist in the table.
    fn get_by_name(&self, row: usize, col_name: &str) -> PyResult<Option<String>> {
        match self.inner.get_by_name(row, col_name) {
            Some(val) => Ok(val.as_string()),
            None => Err(PyValueError::new_err(format!("Column '{}' not found", col_name))),
        }
    }

    /// Get all values of a column by index.
    ///
    /// Args:
    ///     col (int): Zero-based column index.
    ///
    /// Returns:
    ///     list[str | None]: Column values, with ``None`` for NULL.
    ///
    /// Raises:
    ///     ValueError: If ``col`` is out of bounds.
    fn column_values(&self, col: usize) -> PyResult<Vec<Option<String>>> {
        if col >= self.inner.num_cols() {
            return Err(PyValueError::new_err("Column index out of bounds"));
        }
        Ok(self.inner.column_strings(col))
    }

    /// Get all values of a column by name.
    ///
    /// Args:
    ///     col_name (str): Column name.
    ///
    /// Returns:
    ///     list[str | None]: Column values, with ``None`` for NULL.
    ///
    /// Raises:
    ///     ValueError: If ``col_name`` does not exist in the table.
    fn column_values_by_name(&self, col_name: &str) -> PyResult<Vec<Option<String>>> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.column_strings(col))
    }

    /// Convert the table to a column-oriented ``dict``.
    ///
    /// Returns:
    ///     dict[str, list[str | None]]: Mapping ``column_name -> list of values``.
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (col_idx, field) in self.inner.header.fields.iter().enumerate() {
            let values = self.inner.column_strings(col_idx);
            let py_list = PyList::new(py, values.iter().map(|v| v.as_deref()))?;
            dict.set_item(&field.field_name, py_list)?;
        }
        Ok(dict)
    }

    /// Get the unique symbols (distinct values) of a column.
    ///
    /// Args:
    ///     col_name (str): Column name.
    ///
    /// Returns:
    ///     list[str]: Distinct symbol values, in QVD storage order.
    ///
    /// Raises:
    ///     ValueError: If ``col_name`` does not exist in the table.
    fn symbols(&self, col_name: &str) -> PyResult<Vec<String>> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.symbols[col].iter().map(|s| s.to_string_repr()).collect())
    }

    /// Number of unique values (symbols) in a column.
    ///
    /// Args:
    ///     col_name (str): Column name.
    ///
    /// Returns:
    ///     int: Distinct symbol count.
    ///
    /// Raises:
    ///     ValueError: If ``col_name`` does not exist in the table.
    fn num_symbols(&self, col_name: &str) -> PyResult<usize> {
        let col = self.inner.header.fields.iter()
            .position(|f| f.field_name == col_name)
            .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", col_name)))?;
        Ok(self.inner.symbols[col].len())
    }

    /// Return the first ``n`` rows as a list of ``dict``s.
    ///
    /// Args:
    ///     n (int, optional): Number of rows to return. Default is 10. Capped at
    ///         the table's row count.
    ///
    /// Returns:
    ///     list[dict[str, str | None]]: One dict per row, keyed by column name.
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

    /// Load a Parquet file and convert it to a :class:`QvdTable` in memory.
    ///
    /// Args:
    ///     path (str): Path to the Parquet file.
    ///
    /// Returns:
    ///     QvdTable: Loaded table.
    ///
    /// Raises:
    ///     ValueError: If the file cannot be read or has an unsupported schema.
    #[staticmethod]
    fn from_parquet(path: &str) -> PyResult<Self> {
        let table = crate::parquet::read_parquet_to_qvd(path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Save this table as a Parquet file.
    ///
    /// Args:
    ///     path (str): Destination ``.parquet`` path.
    ///     compression (Literal["none", "snappy", "gzip", "lz4", "zstd"], optional):
    ///         Compression codec. Default is ``"snappy"``.
    ///
    /// Raises:
    ///     ValueError: If the codec name is invalid or the file cannot be written.
    #[pyo3(signature = (path, compression=None))]
    fn save_as_parquet(&self, path: &str, compression: Option<&str>) -> PyResult<()> {
        let comp = crate::parquet::ParquetCompression::parse(compression.unwrap_or("snappy"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        crate::parquet::write_qvd_table_to_parquet(&self.inner, path, comp)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))
    }

    /// Convert to a PyArrow ``RecordBatch`` (zero-copy via Arrow C Data Interface).
    ///
    /// Requires ``pyarrow`` to be installed.
    ///
    /// Returns:
    ///     pyarrow.RecordBatch: Arrow representation of the table.
    ///
    /// Raises:
    ///     ValueError: If the table cannot be converted to Arrow.
    ///
    /// Example:
    ///     >>> table = qvd.read_qvd("data.qvd")
    ///     >>> batch = table.to_arrow()
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let batch = crate::parquet::qvd_to_record_batch(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        batch.to_pyarrow(py)
    }

    /// Create a :class:`QvdTable` from a PyArrow ``RecordBatch``.
    ///
    /// Args:
    ///     batch (pyarrow.RecordBatch): Source Arrow batch.
    ///     table_name (str, optional): Name to store in the QVD header. Default is
    ///         ``"table"``.
    ///
    /// Returns:
    ///     QvdTable: New table backed by the Arrow data.
    ///
    /// Raises:
    ///     ValueError: If the input is not a valid ``RecordBatch`` or contains
    ///         unsupported types.
    ///
    /// Example:
    ///     >>> table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
    ///     >>> table.save("output.qvd")
    #[staticmethod]
    #[pyo3(signature = (batch, table_name=None))]
    fn from_arrow(batch: &Bound<'_, PyAny>, table_name: Option<&str>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(batch)
            .map_err(|e| PyValueError::new_err(format!("Invalid RecordBatch: {}", e)))?;
        let table = crate::parquet::record_batch_to_qvd(&batch, table_name.unwrap_or("table"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: table })
    }

    /// Convert to a pandas ``DataFrame``.
    ///
    /// Requires ``pyarrow`` and ``pandas`` to be installed.
    ///
    /// Returns:
    ///     pandas.DataFrame: Table contents as a pandas frame.
    ///
    /// Raises:
    ///     ImportError: If ``pyarrow`` is not installed.
    ///     ValueError: If conversion to Arrow fails.
    ///
    /// Example:
    ///     >>> df = qvd.read_qvd("data.qvd").to_pandas()
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

    /// Convert to a Polars ``DataFrame``.
    ///
    /// Requires ``pyarrow`` and ``polars`` to be installed.
    ///
    /// Returns:
    ///     polars.DataFrame: Table contents as a Polars frame.
    ///
    /// Raises:
    ///     ImportError: If ``pyarrow`` or ``polars`` is not installed.
    ///     ValueError: If conversion to Arrow fails.
    ///
    /// Example:
    ///     >>> df = qvd.read_qvd("data.qvd").to_polars()
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

    /// Filter rows where a column value matches any of the given values.
    ///
    /// Args:
    ///     col_name (str): Column to filter on.
    ///     values (list[str]): Values that should be kept.
    ///
    /// Returns:
    ///     QvdTable: New table containing only the matching rows. The original
    ///     table is not modified.
    ///
    /// Example:
    ///     >>> filtered = table.filter_by_values("%Action_ID", ["7", "9"])
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

    /// Create a new table from a subset of row indices.
    ///
    /// Args:
    ///     row_indices (list[int]): Zero-based row indices to keep, in the desired
    ///         output order. Indices may be repeated.
    ///
    /// Returns:
    ///     QvdTable: New table containing only the selected rows.
    ///
    /// Example:
    ///     >>> rows = qvd.filter_exists(table, "ClientID", idx)
    ///     >>> subset = table.subset_rows(rows)
    fn subset_rows(&self, row_indices: Vec<usize>) -> PyResult<PyQvdTable> {
        let filtered = self.inner.subset_rows(&row_indices);
        Ok(PyQvdTable { inner: filtered })
    }

    /// Normalize the table for maximum Qlik Sense compatibility.
    ///
    /// Converts ``DualInt`` → ``Int`` and ``DualDouble`` → ``Double`` and sets
    /// proper ``NumberFormat``, ``Tags`` and ``BitWidth`` on each field. Modifies
    /// the table in place.
    fn normalize(&mut self) {
        self.inner.normalize();
    }

    /// Concatenate another table into this one (pure append, no deduplication).
    ///
    /// Args:
    ///     other (QvdTable): Table whose rows are appended.
    ///     schema (Literal["strict", "union"]): ``"strict"`` errors on column
    ///         mismatch, ``"union"`` fills missing columns with NULL. Default is
    ///         ``"strict"``.
    ///
    /// Returns:
    ///     QvdTable: New merged table. Inputs are not modified.
    ///
    /// Raises:
    ///     ValueError: On invalid ``schema`` value or column mismatch under
    ///         ``"strict"``.
    ///
    /// Example:
    ///     >>> merged = table_a.concatenate(table_b)
    ///     >>> merged_union = table_a.concatenate(table_b, schema="union")
    #[pyo3(signature = (other, schema="strict"))]
    fn concatenate(&self, other: &PyQvdTable, schema: &str) -> PyResult<PyQvdTable> {
        let mode = parse_schema_mode(schema)?;
        let result = crate::concat::concatenate_with_schema(&self.inner, &other.inner, mode)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: result })
    }

    /// Concatenate another table with primary-key deduplication.
    ///
    /// Args:
    ///     other (QvdTable): Table whose rows are appended.
    ///     pk (str | list[str]): Primary-key column, or list of columns for a
    ///         composite key.
    ///     on_conflict (Literal["replace", "skip", "error"]): Behaviour when a PK
    ///         collision is detected. ``"replace"`` lets new rows win, ``"skip"``
    ///         keeps existing rows, ``"error"`` raises. Default is ``"replace"``.
    ///     schema (Literal["strict", "union"]): ``"strict"`` errors on column
    ///         mismatch, ``"union"`` fills missing columns with NULL. Default is
    ///         ``"strict"``.
    ///
    /// Returns:
    ///     QvdTable: New merged table. Inputs are not modified.
    ///
    /// Raises:
    ///     ValueError: On invalid ``on_conflict``/``schema`` values, missing PK
    ///         column, or PK collision when ``on_conflict="error"``.
    ///
    /// Example:
    ///     >>> merged = existing.concatenate_pk(new_rows, pk="order_id")
    ///     >>> merged = existing.concatenate_pk(
    ///     ...     new_rows, pk=["a", "b"], on_conflict="skip", schema="union",
    ///     ... )
    #[pyo3(signature = (other, pk, on_conflict="replace", schema="strict"))]
    fn concatenate_pk(
        &self,
        other: &PyQvdTable,
        pk: &Bound<'_, PyAny>,
        on_conflict: &str,
        schema: &str,
    ) -> PyResult<PyQvdTable> {
        let pk_columns = parse_pk_arg(pk)?;
        let pk_refs: Vec<&str> = pk_columns.iter().map(|s| s.as_str()).collect();
        let strategy = parse_on_conflict(on_conflict)?;
        let mode = parse_schema_mode(schema)?;
        let result = crate::concat::concatenate_with_pk_schema(&self.inner, &other.inner, &pk_refs, strategy, mode)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        Ok(PyQvdTable { inner: result })
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

/// Fast O(1) lookup index over a set of string values.
///
/// Used by :func:`filter_exists` and by the streaming entry points
/// :func:`read_qvd_filtered` and :func:`register_duckdb_filtered`. Build either
/// from a :class:`QvdTable` column (``ExistsIndex(table, col_name)``) or from an
/// explicit list (:meth:`ExistsIndex.from_values`).
#[pyclass(name = "ExistsIndex")]
pub struct PyExistsIndex {
    values: HashSet<String>,
    col_name: String,
}

#[pymethods]
impl PyExistsIndex {
    /// Build an :class:`ExistsIndex` from a :class:`QvdTable` column.
    ///
    /// Args:
    ///     table (QvdTable): Source table.
    ///     col_name (str): Column to index.
    ///
    /// Raises:
    ///     ValueError: If ``col_name`` is not a column of ``table``.
    ///
    /// Example:
    ///     >>> idx = qvd.ExistsIndex(table, "ClientID")
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

    /// Build an :class:`ExistsIndex` from an explicit list of values.
    ///
    /// Args:
    ///     values (list[str]): Values to index. Duplicates are deduplicated.
    ///
    /// Returns:
    ///     ExistsIndex: New index containing the provided values.
    ///
    /// Example:
    ///     >>> idx = qvd.ExistsIndex.from_values(["7", "9"])
    #[staticmethod]
    fn from_values(values: Vec<String>) -> Self {
        let set: HashSet<String> = values.into_iter().collect();
        PyExistsIndex {
            values: set,
            col_name: "<values>".to_string(),
        }
    }

    /// Check whether a single value is in the index.
    ///
    /// Args:
    ///     value (str): Value to look up.
    ///
    /// Returns:
    ///     bool: ``True`` if present, otherwise ``False``.
    fn exists(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    /// Check multiple values at once.
    ///
    /// Args:
    ///     values (list[str]): Values to look up.
    ///
    /// Returns:
    ///     list[bool]: One flag per input value, in the same order.
    fn exists_many(&self, values: Vec<String>) -> Vec<bool> {
        values.iter().map(|v| self.values.contains(v.as_str())).collect()
    }

    /// Number of unique values in the index.
    fn __len__(&self) -> usize {
        self.values.len()
    }

    /// Support for the ``in`` operator.
    fn __contains__(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    fn __repr__(&self) -> String {
        format!("ExistsIndex(field='{}', values={})", self.col_name, self.values.len())
    }
}

/// Filter rows where a column value is in an :class:`ExistsIndex`.
///
/// Args:
///     table (QvdTable): Table to filter.
///     col_name (str): Column to look up in ``index``.
///     index (ExistsIndex): Lookup index.
///
/// Returns:
///     list[int]: Row indices of matching rows, in ascending order.
///
/// Raises:
///     ValueError: If ``col_name`` is not a column of ``table``.
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

/// Read a QVD file and return a :class:`QvdTable`.
///
/// Args:
///     path (str): Path to the ``.qvd`` file.
///
/// Returns:
///     QvdTable: Loaded table.
///
/// Raises:
///     ValueError: If the file cannot be opened or is not a valid QVD.
///
/// Example:
///     >>> import qvd
///     >>> table = qvd.read_qvd("data.qvd")
#[pyfunction]
fn read_qvd(path: &str) -> PyResult<PyQvdTable> {
    PyQvdTable::load(path)
}

/// Read a QVD file and return a PyArrow ``RecordBatch`` directly.
///
/// Args:
///     path (str): Path to the ``.qvd`` file.
///
/// Returns:
///     pyarrow.RecordBatch: Arrow batch with the file contents.
///
/// Raises:
///     ValueError: If the file cannot be read or converted to Arrow.
#[pyfunction]
fn read_qvd_to_arrow<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let table = reader::read_qvd_file(path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let batch = crate::parquet::qvd_to_record_batch(&table)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    batch.to_pyarrow(py)
}

/// Read a QVD file and return a pandas ``DataFrame`` directly.
///
/// Requires ``pyarrow`` and ``pandas`` to be installed.
///
/// Args:
///     path (str): Path to the ``.qvd`` file.
///
/// Returns:
///     pandas.DataFrame: File contents as a pandas frame.
///
/// Raises:
///     ImportError: If ``pyarrow`` is not installed.
///     ValueError: If the file cannot be read.
#[pyfunction]
fn read_qvd_to_pandas<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let t = PyQvdTable::load(path)?;
    t.to_pandas(py)
}

/// Read a QVD file and return a Polars ``DataFrame`` directly.
///
/// Requires ``pyarrow`` and ``polars`` to be installed.
///
/// Args:
///     path (str): Path to the ``.qvd`` file.
///
/// Returns:
///     polars.DataFrame: File contents as a Polars frame.
///
/// Raises:
///     ImportError: If ``pyarrow`` or ``polars`` is not installed.
///     ValueError: If the file cannot be read.
#[pyfunction]
fn read_qvd_to_polars<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let t = PyQvdTable::load(path)?;
    t.to_polars(py)
}

/// Convert a Parquet file to a QVD file.
///
/// Args:
///     parquet_path (str): Source ``.parquet`` path.
///     qvd_path (str): Destination ``.qvd`` path.
///
/// Raises:
///     ValueError: If the input cannot be read or the output cannot be written.
#[pyfunction]
fn convert_parquet_to_qvd(parquet_path: &str, qvd_path: &str) -> PyResult<()> {
    crate::parquet::convert_parquet_to_qvd(parquet_path, qvd_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Convert a QVD file to a Parquet file.
///
/// Args:
///     qvd_path (str): Source ``.qvd`` path.
///     parquet_path (str): Destination ``.parquet`` path.
///     compression (Literal["none", "snappy", "gzip", "lz4", "zstd"], optional):
///         Compression codec. Default is ``"snappy"``.
///
/// Raises:
///     ValueError: If the codec name is invalid or the conversion fails.
#[pyfunction]
#[pyo3(signature = (qvd_path, parquet_path, compression=None))]
fn convert_qvd_to_parquet(qvd_path: &str, parquet_path: &str, compression: Option<&str>) -> PyResult<()> {
    let comp = crate::parquet::ParquetCompression::parse(compression.unwrap_or("snappy"))
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    crate::parquet::convert_qvd_to_parquet(qvd_path, parquet_path, comp)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Read a QVD file with a streaming EXISTS() filter and optional column projection.
///
/// Only matching rows are loaded into memory — much faster and lighter for large files.
///
/// Args:
///     path (str): Path to the QVD file.
///     filter_col (str): Column name to filter on.
///     index (ExistsIndex): Lookup index built via :meth:`ExistsIndex.from_values`
///         or from another table's column.
///     select (list[str], optional): Subset of columns to load. Default loads all
///         columns.
///     chunk_size (int, optional): Streaming chunk size in rows. Default is
///         ``65536``.
///
/// Returns:
///     QvdTable: Filtered table containing only the matching rows.
///
/// Raises:
///     ValueError: If the file cannot be read or ``filter_col`` does not exist.
///
/// Example:
///     >>> import qvd
///     >>> idx = qvd.ExistsIndex.from_values(["7", "9"])
///     >>> table = qvd.read_qvd_filtered(
///     ...     "large.qvd", "%Action_ID", idx,
///     ...     select=["%Client_ID", "Date_BK", "%Action_ID"],
///     ... )
///     >>> table.save("filtered.qvd")
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

/// Register a QVD file as a DuckDB table.
///
/// After registration, the table is queryable via SQL on the given connection.
/// Requires ``pyarrow`` and a DuckDB connection.
///
/// Args:
///     conn (duckdb.DuckDBPyConnection): Target DuckDB connection.
///     table_name (str): Name under which the table is registered.
///     path (str): Path to the ``.qvd`` file.
///
/// Raises:
///     ImportError: If ``pyarrow`` is not installed.
///     ValueError: If the file cannot be read or registered.
///
/// Example:
///     >>> import qvd, duckdb
///     >>> conn = duckdb.connect()
///     >>> qvd.register_duckdb(conn, "sales", "sales.qvd")
///     >>> conn.sql("SELECT * FROM sales WHERE amount > 100").show()
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

/// Register a QVD file as a DuckDB table with a streaming EXISTS() filter.
///
/// Only matching rows are loaded — memory-efficient for large files.
///
/// Args:
///     conn (duckdb.DuckDBPyConnection): Target DuckDB connection.
///     table_name (str): Name under which the table is registered.
///     path (str): Path to the ``.qvd`` file.
///     filter_col (str): Column name to filter on.
///     index (ExistsIndex): Lookup index.
///     select (list[str], optional): Subset of columns to load. Default loads all
///         columns.
///     chunk_size (int, optional): Streaming chunk size in rows. Default is
///         ``65536``.
///
/// Raises:
///     ImportError: If ``pyarrow`` is not installed.
///     ValueError: If the file cannot be read, ``filter_col`` is missing, or
///         registration fails.
///
/// Example:
///     >>> import qvd, duckdb
///     >>> conn = duckdb.connect()
///     >>> idx = qvd.ExistsIndex.from_values(["7", "9"])
///     >>> qvd.register_duckdb_filtered(
///     ...     conn, "cal79", "large.qvd", "%Action_ID", idx,
///     ...     select=["%Client_ID", "Date_BK", "%Action_ID"],
///     ... )
///     >>> conn.sql("SELECT COUNT(*) FROM cal79").show()
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
///
/// Each registered table is named after its file (without the ``.qvd``
/// extension). Files larger than ``max_file_size_mb`` are skipped with a
/// :class:`UserWarning`; failures on individual files are reported the same way
/// and do not abort the run.
///
/// Args:
///     conn (duckdb.DuckDBPyConnection): Target DuckDB connection.
///     folder_paths (str | list[str]): Single directory path or list of paths to
///         scan.
///     recursive (bool, optional): Recurse into subdirectories. Default is
///         ``False``.
///     glob (str, optional): File-name pattern. Supports ``"prefix*"``,
///         ``"*suffix"``, ``"prefix*suffix"`` and ``"*mid*"``. Default is
///         ``"*.qvd"``.
///     max_file_size_mb (int, optional): Skip files larger than this many
///         megabytes. Default is ``500``.
///
/// Returns:
///     list[str]: Sorted list of successfully registered table names.
///
/// Raises:
///     ImportError: If ``pyarrow`` is not installed.
///     ValueError: If ``folder_paths`` is neither a string nor a list of strings.
///
/// Example:
///     >>> import qvd, duckdb
///     >>> conn = duckdb.connect()
///     >>> tables = qvd.register_duckdb_folder(conn, "/path/to/qvd_files/")
///     >>> tables = qvd.register_duckdb_folder(
///     ...     conn, ["/data/sales/", "/data/crm/"],
///     ... )
///     >>> tables = qvd.register_duckdb_folder(
///     ...     conn, "/data/", recursive=True, glob="client_*.qvd",
///     ... )
///     >>> conn.sql("SELECT * FROM sales JOIN customers ON ...").show()
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

// ============================================================
// write_arrow: Arrow → QVD in one call
// ============================================================

/// Write a PyArrow ``RecordBatch`` or ``Table`` directly to a QVD file.
///
/// Args:
///     data (pyarrow.RecordBatch | pyarrow.Table): Source Arrow data. A ``Table``
///         is internally combined into a single ``RecordBatch`` via
///         ``combine_chunks``.
///     path (str): Destination ``.qvd`` path.
///     table_name (str, optional): Name to store in the QVD header. Default is
///         ``"table"``.
///
/// Raises:
///     ValueError: If ``data`` is neither a ``RecordBatch`` nor a ``Table``, or
///         is an empty ``Table``.
///
/// Example:
///     >>> qvd.write_arrow(batch, "output.qvd", table_name="sales")
///     >>> qvd.write_arrow(arrow_table, "output.qvd")
#[pyfunction]
#[pyo3(signature = (data, path, table_name=None))]
fn write_arrow(
    data: &Bound<'_, PyAny>,
    path: &str,
    table_name: Option<&str>,
) -> PyResult<()> {
    let batch = extract_record_batch(data)?;
    crate::parquet::write_record_batch_to_qvd(&batch, table_name.unwrap_or("table"), path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Extract an Arrow RecordBatch from either a pyarrow.RecordBatch or pyarrow.Table.
fn extract_record_batch(obj: &Bound<'_, PyAny>) -> PyResult<arrow::record_batch::RecordBatch> {
    use arrow::pyarrow::FromPyArrow;
    // Try RecordBatch first
    if let Ok(batch) = arrow::record_batch::RecordBatch::from_pyarrow_bound(obj) {
        return Ok(batch);
    }
    // Try PyArrow Table → combine_chunks → first batch
    if obj.hasattr("to_batches")? {
        let combined = obj.call_method0("combine_chunks")?;
        let batches = combined.call_method0("to_batches")?;
        let batch_list: Vec<Bound<'_, PyAny>> = batches.extract()?;
        if batch_list.is_empty() {
            return Err(PyValueError::new_err("Empty Arrow Table"));
        }
        return arrow::record_batch::RecordBatch::from_pyarrow_bound(&batch_list[0])
            .map_err(|e| PyValueError::new_err(format!("Invalid Arrow Table: {}", e)));
    }
    Err(PyValueError::new_err("data must be pyarrow.RecordBatch or pyarrow.Table"))
}

// ============================================================
// Top-level concatenate functions
// ============================================================

/// Concatenate two QVD sources into a new QVD file (pure append, no deduplication).
///
/// Args:
///     existing_path (str): Path to the existing ``.qvd`` file.
///     new_rows (str | pyarrow.RecordBatch | pyarrow.Table | QvdTable): Rows to
///         append. A string is interpreted as a path to a ``.qvd`` file.
///     out_path (str): Destination ``.qvd`` path.
///     table_name (str, optional): If provided, overrides the QVD header table
///         name in the output (and is used as the Arrow→QVD fallback name).
///     schema (Literal["strict", "union"]): ``"strict"`` errors on column
///         mismatch, ``"union"`` fills missing columns with NULL. Default is
///         ``"strict"``.
///
/// Raises:
///     ValueError: On invalid ``schema``, unreadable input, unsupported
///         ``new_rows`` type, or column mismatch under ``"strict"``.
///
/// Example:
///     >>> qvd.concatenate_qvd("existing.qvd", "new_data.qvd", "merged.qvd")
///     >>> qvd.concatenate_qvd(
///     ...     "existing.qvd", arrow_batch, "merged.qvd", table_name="sales",
///     ... )
#[pyfunction]
#[pyo3(signature = (existing_path, new_rows, out_path, table_name=None, schema="strict"))]
fn concatenate_qvd(
    existing_path: &str,
    new_rows: &Bound<'_, PyAny>,
    out_path: &str,
    table_name: Option<&str>,
    schema: &str,
) -> PyResult<()> {
    let existing = reader::read_qvd_file(existing_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let new_table = resolve_new_rows(new_rows, table_name)?;
    let mode = parse_schema_mode(schema)?;
    let mut result = crate::concat::concatenate_with_schema(&existing, &new_table, mode)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    if let Some(name) = table_name {
        result.header.table_name = name.to_string();
    }
    writer::write_qvd_file(&result, out_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Concatenate two QVD sources with primary-key deduplication.
///
/// Args:
///     existing_path (str): Path to the existing ``.qvd`` file.
///     new_rows (str | pyarrow.RecordBatch | pyarrow.Table | QvdTable): Rows to
///         append. A string is interpreted as a path to a ``.qvd`` file.
///     out_path (str): Destination ``.qvd`` path.
///     pk (str | list[str]): Primary-key column, or list of columns for a
///         composite key.
///     on_conflict (Literal["replace", "skip", "error"]): Behaviour when a PK
///         collision is detected. ``"replace"`` lets new rows win, ``"skip"``
///         keeps existing rows, ``"error"`` raises. Default is ``"replace"``.
///     table_name (str, optional): If provided, overrides the QVD header table
///         name in the output.
///     schema (Literal["strict", "union"]): ``"strict"`` errors on column
///         mismatch, ``"union"`` fills missing columns with NULL. Default is
///         ``"strict"``.
///
/// Raises:
///     ValueError: On invalid ``on_conflict``/``schema`` values, unreadable
///         input, unsupported ``new_rows`` type, missing PK column, or PK
///         collision when ``on_conflict="error"``.
///
/// Example:
///     >>> qvd.concatenate_pk_qvd(
///     ...     "existing.qvd", "new.qvd", "out.qvd", pk="order_id",
///     ... )
///     >>> qvd.concatenate_pk_qvd(
///     ...     "existing.qvd", batch, "out.qvd",
///     ...     pk=["a", "b"], on_conflict="skip",
///     ... )
#[pyfunction]
#[pyo3(signature = (existing_path, new_rows, out_path, pk, on_conflict="replace", table_name=None, schema="strict"))]
fn concatenate_pk_qvd(
    existing_path: &str,
    new_rows: &Bound<'_, PyAny>,
    out_path: &str,
    pk: &Bound<'_, PyAny>,
    on_conflict: &str,
    table_name: Option<&str>,
    schema: &str,
) -> PyResult<()> {
    let existing = reader::read_qvd_file(existing_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    let new_table = resolve_new_rows(new_rows, table_name)?;
    let pk_columns = parse_pk_arg(pk)?;
    let pk_refs: Vec<&str> = pk_columns.iter().map(|s| s.as_str()).collect();
    let strategy = parse_on_conflict(on_conflict)?;
    let mode = parse_schema_mode(schema)?;
    let mut result = crate::concat::concatenate_with_pk_schema(&existing, &new_table, &pk_refs, strategy, mode)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
    if let Some(name) = table_name {
        result.header.table_name = name.to_string();
    }
    writer::write_qvd_file(&result, out_path)
        .map_err(|e| PyValueError::new_err(format!("{}", e)))
}

// ============================================================
// Helpers for Python bindings
// ============================================================

/// Resolve new_rows argument: str (qvd path), PyArrow RecordBatch, or QvdTable.
fn resolve_new_rows(obj: &Bound<'_, PyAny>, table_name: Option<&str>) -> PyResult<reader::QvdTable> {
    // Case 1: str → path to QVD file
    if let Ok(path) = obj.extract::<String>() {
        return reader::read_qvd_file(&path)
            .map_err(|e| PyValueError::new_err(format!("{}", e)));
    }

    // Case 2: QvdTable (our own type)
    if let Ok(py_table) = obj.extract::<pyo3::PyRef<'_, PyQvdTable>>() {
        // Clone the inner table
        let inner = &py_table.inner;
        return Ok(reader::QvdTable {
            header: inner.header.clone(),
            symbols: inner.symbols.clone(),
            row_indices: inner.row_indices.clone(),
            raw_xml: Vec::new(),
            raw_binary: Vec::new(),
        });
    }

    // Case 3: PyArrow RecordBatch or Table
    // Try RecordBatch first
    if let Ok(batch) = arrow::record_batch::RecordBatch::from_pyarrow_bound(obj) {
        return crate::parquet::record_batch_to_qvd(&batch, table_name.unwrap_or("table"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)));
    }

    // Try PyArrow Table → combine_chunks → to_batches → first batch
    if obj.hasattr("to_batches")? {
        let combined = obj.call_method0("combine_chunks")?;
        let batches = combined.call_method0("to_batches")?;
        let batch_list: Vec<Bound<'_, PyAny>> = batches.extract()?;
        if batch_list.is_empty() {
            return Err(PyValueError::new_err("Empty Arrow Table"));
        }
        let batch = arrow::record_batch::RecordBatch::from_pyarrow_bound(&batch_list[0])
            .map_err(|e| PyValueError::new_err(format!("Invalid Arrow Table: {}", e)))?;
        return crate::parquet::record_batch_to_qvd(&batch, table_name.unwrap_or("table"))
            .map_err(|e| PyValueError::new_err(format!("{}", e)));
    }

    Err(PyValueError::new_err(
        "new_rows must be: str (QVD path), pyarrow.RecordBatch, pyarrow.Table, or QvdTable"
    ))
}

/// Parse pk argument: str or list[str].
fn parse_pk_arg(pk: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = pk.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(list) = pk.extract::<Vec<String>>() {
        if list.is_empty() {
            return Err(PyValueError::new_err("pk must not be empty"));
        }
        return Ok(list);
    }
    Err(PyValueError::new_err("pk must be str or list[str]"))
}

/// Parse schema mode string to enum.
fn parse_schema_mode(s: &str) -> PyResult<crate::concat::SchemaMode> {
    match s {
        "strict" => Ok(crate::concat::SchemaMode::Strict),
        "union" => Ok(crate::concat::SchemaMode::Union),
        _ => Err(PyValueError::new_err(
            format!("schema must be 'strict' or 'union', got '{}'", s)
        )),
    }
}

/// Parse on_conflict string to enum.
fn parse_on_conflict(s: &str) -> PyResult<crate::concat::OnConflict> {
    match s {
        "replace" => Ok(crate::concat::OnConflict::Replace),
        "skip" => Ok(crate::concat::OnConflict::Skip),
        "error" => Ok(crate::concat::OnConflict::Error),
        _ => Err(PyValueError::new_err(
            format!("on_conflict must be 'replace', 'skip', or 'error', got '{}'", s)
        )),
    }
}

/// Module contains the core classes and functions for dealing with QVD files in Python.
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
    m.add_function(wrap_pyfunction!(write_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(concatenate_qvd, m)?)?;
    m.add_function(wrap_pyfunction!(concatenate_pk_qvd, m)?)?;
    Ok(())
}
