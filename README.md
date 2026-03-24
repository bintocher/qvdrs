# qvd

[![Crates.io](https://img.shields.io/crates/v/qvd.svg)](https://crates.io/crates/qvd)
[![PyPI](https://img.shields.io/pypi/v/qvdrs.svg)](https://pypi.org/project/qvdrs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance Rust library for reading, writing and converting Qlik QVD files. With Parquet/Arrow interop, DataFusion SQL, streaming reader, CLI tool, and Python bindings (PyArrow, pandas, Polars).

**First and only QVD crate on crates.io.**

## Features

- **Read/Write QVD** — byte-identical roundtrip, zero-copy where possible
- **Parquet ↔ QVD** — convert in both directions with compression support (snappy, zstd, gzip, lz4)
- **Arrow RecordBatch** — convert QVD to/from Arrow for integration with DataFusion, DuckDB, Polars
- **DataFusion SQL** — register QVD files as tables and query them with SQL
- **DuckDB integration** — use QVD data in DuckDB via Arrow bridge (Rust and Python)
- **Streaming reader** — read QVD files in chunks without loading everything into memory
- **EXISTS() index** — O(1) hash lookup, like Qlik's `EXISTS()` function
- **CLI tool** — `qvd-cli convert`, `inspect`, `head`, `schema`
- **Python bindings** — PyArrow, pandas, Polars support via zero-copy Arrow bridge. 20-35x faster than PyQvd
- **Zero dependencies** for core QVD read/write (Parquet/Arrow/DataFusion/Python are optional features)

## Performance

Tested on 20 real QVD files (11 KB to 2.8 GB):

| File | Size | Rows | Columns | Read | Write |
|------|------|------|---------|------|-------|
| sample_tiny.qvd | 11 KB | 12 | 5 | 0.0s | 0.0s |
| sample_small.qvd | 418 KB | 2,746 | 8 | 0.0s | 0.0s |
| sample_medium.qvd | 41 MB | 465,810 | 12 | 0.5s | 0.0s |
| sample_large.qvd | 587 MB | 5,458,618 | 15 | 6.1s | 0.4s |
| sample_xlarge.qvd | 1.7 GB | 87,617,047 | 6 | 36.8s | 1.6s |
| sample_huge.qvd | 2.8 GB | 11,907,648 | 42 | 24.3s | 2.4s |

All 20 files — **byte-identical roundtrip** (MD5 match).

### vs PyQvd (Pure Python)

| File | PyQvd | qvd (Rust) | Speedup |
|------|-------|------------|---------|
| 10 MB, 1.4M rows | 5.0s | 0.17s | **29x** |
| 41 MB, 466K rows | 8.5s | 0.5s | **16x** |
| 480 MB, 12M rows | 79.4s | 2.3s | **35x** |
| 1.7 GB, 87M rows | >10 min | 29.6s | **>20x** |

## Installation

### Rust

```toml
# Core QVD read/write (zero dependencies)
[dependencies]
qvd = "0.2"

# With Parquet/Arrow support
[dependencies]
qvd = { version = "0.2", features = ["parquet_support"] }

# With DataFusion SQL support
[dependencies]
qvd = { version = "0.2", features = ["datafusion_support"] }
```

### CLI

```bash
cargo install qvd --features cli
```

### Python

```bash
pip install qvdrs
```

Or with uv:

```bash
uv pip install qvdrs
```

## Quick Start — Rust

### Read/Write QVD

```rust
use qvd::{read_qvd_file, write_qvd_file};

let table = read_qvd_file("data.qvd")?;
println!("Rows: {}, Cols: {}", table.num_rows(), table.num_cols());

// Byte-identical roundtrip
write_qvd_file(&table, "output.qvd")?;
```

### Convert Parquet ↔ QVD

```rust
use qvd::{convert_parquet_to_qvd, convert_qvd_to_parquet, ParquetCompression};

// Parquet → QVD
convert_parquet_to_qvd("input.parquet", "output.qvd")?;

// QVD → Parquet (with zstd compression)
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Zstd)?;
```

### Arrow RecordBatch

```rust
use qvd::{read_qvd_file, qvd_to_record_batch, record_batch_to_qvd};

let table = read_qvd_file("data.qvd")?;
let batch = qvd_to_record_batch(&table)?;
// Use with DataFusion, DuckDB, Polars, etc.

// Arrow → QVD
let qvd_table = record_batch_to_qvd(&batch, "my_table")?;
```

### DataFusion SQL (feature `datafusion_support`)

```rust
use datafusion::prelude::*;
use qvd::register_qvd;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Register QVD file as a table
    register_qvd(&ctx, "sales", "sales.qvd")?;

    // Run SQL queries directly on QVD data
    let df = ctx.sql("SELECT Region, SUM(Amount) as total
                      FROM sales
                      GROUP BY Region
                      ORDER BY total DESC").await?;
    df.show().await?;

    Ok(())
}
```

You can also register multiple QVD files and JOIN them:

```rust
register_qvd(&ctx, "orders", "orders.qvd")?;
register_qvd(&ctx, "customers", "customers.qvd")?;

let df = ctx.sql("SELECT c.Name, COUNT(o.OrderID) as order_count
                   FROM orders o
                   JOIN customers c ON o.CustomerID = c.CustomerID
                   GROUP BY c.Name").await?;
```

### DuckDB via Arrow (Rust)

DuckDB can ingest Arrow RecordBatches directly — no file conversion needed:

```rust
use qvd::{read_qvd_file, qvd_to_record_batch};

let table = read_qvd_file("data.qvd")?;
let batch = qvd_to_record_batch(&table)?;

// Pass the Arrow RecordBatch to DuckDB via its Arrow interface
// See: https://docs.rs/duckdb/latest/duckdb/
```

### Streaming Reader

```rust
use qvd::open_qvd_stream;

let mut reader = open_qvd_stream("huge_file.qvd")?;
println!("Total rows: {}", reader.total_rows());

while let Some(chunk) = reader.next_chunk(65536)? {
    // Process 65K rows at a time
    println!("Chunk: {} rows starting at {}", chunk.num_rows, chunk.start_row);
}
```

### EXISTS() — O(1) Lookup

Like Qlik's `EXISTS()` function — build an index of unique values from one table
and use it to check or filter another table in O(1) per row.

```rust
use qvd::{read_qvd_file, ExistsIndex, filter_rows_by_exists, filter_rows_by_exists_fast};

// Build index from the "clients" table
let clients = read_qvd_file("clients.qvd")?;
let index = ExistsIndex::from_column(&clients, "ClientID").unwrap();

// O(1) lookup — does this value exist?
assert!(index.exists("12345"));
println!("Unique clients: {}", index.len());

// Filter another table — get row indices where ClientID exists in the clients table
let facts = read_qvd_file("facts.qvd")?;

// By column name (convenient)
let matching_rows = filter_rows_by_exists(&facts, "ClientID", &index);
println!("Matching rows: {}", matching_rows.len());

// By column index (faster for large tables — pre-computes symbol matches)
let col_idx = 0; // index of "ClientID" column in facts table
let matching_rows = filter_rows_by_exists_fast(&facts, col_idx, &index);

// Access the filtered rows
for &row in &matching_rows {
    let client_id = facts.get(row, col_idx).as_string().unwrap_or_default();
    println!("Row {}: ClientID = {}", row, client_id);
}
```

## Quick Start — Python

### Basic usage

```python
import qvd

# Read QVD
table = qvd.read_qvd("data.qvd")
print(table.columns, table.num_rows)
print(table.head(5))

# Save QVD
table.save("output.qvd")

# Parquet ↔ QVD
qvd.convert_parquet_to_qvd("input.parquet", "output.qvd")
qvd.convert_qvd_to_parquet("input.qvd", "output.parquet", compression="zstd")

# Load Parquet as QvdTable
table = qvd.QvdTable.from_parquet("input.parquet")
table.save("output.qvd")
table.save_as_parquet("output.parquet", compression="snappy")

# EXISTS — O(1) lookup (like Qlik's EXISTS() function)
clients = qvd.read_qvd("clients.qvd")
idx = qvd.ExistsIndex(clients, "ClientID")

# Check if a value exists
print("12345" in idx)           # True/False
print(idx.exists("12345"))      # same thing
print(len(idx))                 # number of unique values

# Check multiple values at once
results = idx.exists_many(["12345", "67890", "99999"])
print(results)  # [True, True, False]

# Filter rows from another table — returns list of matching row indices
facts = qvd.read_qvd("facts.qvd")
matching_rows = qvd.filter_exists(facts, "ClientID", idx)
print(f"Matched {len(matching_rows)} rows out of {facts.num_rows}")
```

### PyArrow

```python
import qvd

# QVD → PyArrow RecordBatch (zero-copy via Arrow C Data Interface)
table = qvd.read_qvd("data.qvd")
batch = table.to_arrow()

# Or directly:
batch = qvd.read_qvd_to_arrow("data.qvd")

# PyArrow → QVD
table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
table.save("output.qvd")
```

### pandas

```python
import qvd

# QVD → pandas DataFrame (via Arrow, zero-copy where possible)
df = qvd.read_qvd("data.qvd").to_pandas()

# Or directly:
df = qvd.read_qvd_to_pandas("data.qvd")

# pandas → QVD (via PyArrow round-trip)
import pyarrow as pa
batch = pa.RecordBatch.from_pandas(df)
table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
table.save("output.qvd")
```

### Polars

```python
import qvd

# QVD → Polars DataFrame
df = qvd.read_qvd("data.qvd").to_polars()

# Or directly:
df = qvd.read_qvd_to_polars("data.qvd")

# Polars → QVD (via PyArrow round-trip)
batch = df.to_arrow()
table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
table.save("output.qvd")
```

### DuckDB (Python)

```python
import qvd
import duckdb

# QVD → DuckDB (via Arrow, zero-copy)
batch = qvd.read_qvd_to_arrow("data.qvd")
result = duckdb.sql("SELECT * FROM batch WHERE amount > 100")

# Or query multiple QVD files:
sales = qvd.read_qvd_to_arrow("sales.qvd")
customers = qvd.read_qvd_to_arrow("customers.qvd")
result = duckdb.sql("""
    SELECT c.Name, SUM(s.Amount) as total
    FROM sales s
    JOIN customers c ON s.CustomerID = c.CustomerID
    GROUP BY c.Name
""")
```

## CLI

Install:

```bash
cargo install qvd --features cli
```

### Convert between formats

```bash
# Parquet → QVD
qvd-cli convert input.parquet output.qvd

# QVD → Parquet (default compression: snappy)
qvd-cli convert input.qvd output.parquet

# QVD → Parquet with specific compression
qvd-cli convert input.qvd output.parquet --compression zstd
qvd-cli convert input.qvd output.parquet --compression gzip
qvd-cli convert input.qvd output.parquet --compression lz4
qvd-cli convert input.qvd output.parquet --compression none

# Rewrite QVD (re-generate from internal representation)
qvd-cli convert input.qvd output.qvd

# Recompress Parquet
qvd-cli convert input.parquet output.parquet --compression zstd
```

### Inspect QVD metadata

```bash
qvd-cli inspect data.qvd
```

Output example:

```
File:       data.qvd
Size:       41.3 MB
Table:      SalesData
Rows:       465,810
Columns:    12
Created:    2024-01-15 10:30:00
Build:      14.0
RecordSize: 89 bytes
Read time:  0.50s

Column                         Symbols BitWidth   Bias FmtType  Tags
--------------------------------------------------------------------------------
OrderID                         465810        20      0      0  $numeric, $integer
CustomerID                       12500        14      0      0  $numeric, $integer
Region                               5         3      0      0  $text
Amount                          389201        19      0      2  $numeric
```

### Preview rows

```bash
# Show first 10 rows (default)
qvd-cli head data.qvd

# Show first 50 rows
qvd-cli head data.qvd --rows 50
```

### Show Arrow schema

```bash
qvd-cli schema data.qvd
```

Output example:

```
Arrow Schema for 'data.qvd':

  OrderID                        Int64
  CustomerID                     Int64
  Region                         Utf8
  Amount                         Float64 (nullable)
  OrderDate                      Date32
```

## Architecture

```
src/
├── lib.rs          — public API, re-exports
├── error.rs        — error types (QvdError, QvdResult)
├── header.rs       — XML header parser/writer (custom, zero-dep)
├── value.rs        — QVD data types (QvdSymbol, QvdValue)
├── symbol.rs       — symbol table binary reader/writer
├── index.rs        — index table bit-stuffing reader/writer
├── reader.rs       — high-level QVD reader
├── writer.rs       — high-level QVD writer + QvdTableBuilder
├── exists.rs       — ExistsIndex with HashSet + filter functions
├── streaming.rs    — streaming chunk-based QVD reader
├── parquet.rs      — Parquet/Arrow ↔ QVD conversion (optional)
├── datafusion.rs   — DataFusion TableProvider for SQL on QVD (optional)
├── python.rs       — PyO3 bindings with PyArrow/pandas/Polars (optional)
└── bin/qvd.rs      — CLI binary (optional)
```

## Feature Flags

| Feature | Dependencies | Description |
|---------|-------------|-------------|
| *(default)* | none | Core QVD read/write |
| `parquet_support` | arrow, parquet, chrono | Parquet/Arrow conversion |
| `datafusion_support` | + datafusion, tokio | SQL queries on QVD via DataFusion |
| `cli` | + clap | CLI binary |
| `python` | + pyo3, arrow/pyarrow | Python bindings with PyArrow/pandas/Polars |

## Author

Stanislav Chernov ([@bintocher](https://github.com/bintocher))

## License

MIT — see [LICENSE](LICENSE)
