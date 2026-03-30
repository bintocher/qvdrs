# qvd

[![Crates.io](https://img.shields.io/crates/v/qvd.svg)](https://crates.io/crates/qvd)
[![PyPI](https://img.shields.io/pypi/v/qvdrs.svg)](https://pypi.org/project/qvdrs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance Rust library for reading, writing and converting Qlik QVD files. Parquet/Arrow interop, DataFusion SQL, DuckDB integration, streaming reader, CLI tool, and Python bindings (PyArrow, pandas, Polars).

**First and only QVD crate on crates.io.**

## Features

- **Read/Write QVD** — byte-identical roundtrip, Qlik Sense-compatible output
- **Parquet ↔ QVD** — bidirectional conversion with compression (snappy, zstd, gzip, lz4). Binary-identical to Qlik Sense output
- **Arrow RecordBatch** — zero-copy QVD ↔ Arrow for DataFusion, DuckDB, Polars
- **DuckDB integration** — register QVD files as SQL tables, query with JOINs, batch folder registration
- **DataFusion SQL** — register QVD files as tables, run SQL queries
- **Streaming reader** — read QVD in chunks without loading everything into memory
- **EXISTS() index** — O(1) hash lookup like Qlik's `EXISTS()`. Streaming filtered reads — 2.5× faster than Qlik Sense
- **`normalize()`** — auto-detect and set proper symbol types, NumberFormat, Tags, BitWidth for Qlik compatibility
- **CLI tool** — `qvd-cli convert`, `inspect`, `head`, `schema`, `filter`
- **Python bindings** — PyArrow, pandas, Polars via zero-copy Arrow bridge
- **Zero dependencies** for core QVD read/write (Parquet/Arrow/DataFusion/Python are optional features)

## Performance

Tested on 399 real QVD files (11 KB — 2.8 GB), all byte-identical roundtrip (MD5 match).

| File | Size | Rows | Columns | Read | Write |
|------|------|------|---------|------|-------|
| sample_tiny.qvd | 11 KB | 12 | 5 | 0.0s | 0.0s |
| sample_small.qvd | 418 KB | 2,746 | 8 | 0.0s | 0.0s |
| sample_medium.qvd | 41 MB | 465,810 | 12 | 0.5s | 0.0s |
| sample_large.qvd | 587 MB | 5,458,618 | 15 | 6.1s | 0.4s |
| sample_xlarge.qvd | 1.7 GB | 87,617,047 | 8 | 23.6s | 1.6s |
| sample_huge.qvd | 2.8 GB | 11,907,648 | 42 | 24.3s | 2.4s |

### Streaming EXISTS() filter — vs Qlik Sense

**1.7 GB QVD, 87.6M rows × 8 columns → filter by 2 values, select 3 columns → 20.4M rows × 3 columns**

Qlik Sense script equivalent:
```qlik
types:
LOAD * INLINE [%Type_ID
7
9];

filtered:
LOAD %Key_ID, DateField_BK, %Type_ID
FROM [lib://data/large_table.qvd](qvd)
WHERE EXISTS(%Type_ID);

STORE filtered INTO [lib://data/result.qvd](qvd);
DROP TABLE filtered;
```

| | Qlik Sense | qvdrs |
|---|---|---|
| **Total (→ QVD)** | **~28s** | **11.4s** |
| **Total (→ Parquet)** | — | **15.5s** |
| **Speedup** | 1× | **2.5×** |

The streaming reader loads only symbol tables into memory, then scans the index table in chunks. For each row, only the filter column is decoded first. Matching rows get selected columns decoded. Non-matching rows are skipped entirely.

### Parquet → QVD conversion

QVD files generated from Parquet are binary-identical to those created by Qlik Sense (same symbol types, NumberFormat, Tags, BitWidth, BitOffset ordering). Verified by MD5 hash comparison of the binary section.

## Installation

### Rust

```toml
# Core QVD read/write (zero dependencies)
[dependencies]
qvd = "0.5.0"

# With Parquet/Arrow support
[dependencies]
qvd = { version = "0.5.0", features = ["parquet_support"] }

# With DataFusion SQL support
[dependencies]
qvd = { version = "0.5.0", features = ["datafusion_support"] }
```

### Python

```bash
pip install qvdrs
```

```bash
uv pip install qvdrs
```

### CLI

```bash
cargo install qvd --features cli
```

Or run without installing via uvx:

```bash
uvx --from qvdrs qvd-cli inspect data.qvd
uvx --from qvdrs qvd-cli convert input.qvd output.parquet
uvx --from qvdrs qvd-cli filter large.qvd output.qvd --column %Type_ID --values 7,9
```

---

## Rust Examples

### Read and write QVD

```rust
use qvd::{read_qvd_file, write_qvd_file};

let table = read_qvd_file("data.qvd")?;
println!("Rows: {}, Cols: {}", table.num_rows(), table.num_cols());
println!("Columns: {:?}", table.column_names());

// Access individual values
for row in 0..5 {
    let val = table.get(row, 0);
    println!("Row {}: {:?}", row, val.as_string());
}

// Byte-identical roundtrip
write_qvd_file(&table, "output.qvd")?;
```

### Convert Parquet ↔ QVD

```rust
use qvd::{convert_parquet_to_qvd, convert_qvd_to_parquet, ParquetCompression};

// Parquet → QVD (Qlik Sense-compatible output)
convert_parquet_to_qvd("input.parquet", "output.qvd")?;

// QVD → Parquet
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Zstd)?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Snappy)?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Gzip)?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Lz4)?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::None)?;
```

### Arrow RecordBatch

```rust
use qvd::{read_qvd_file, qvd_to_record_batch, record_batch_to_qvd, write_qvd_file};

// QVD → Arrow
let table = read_qvd_file("data.qvd")?;
let batch = qvd_to_record_batch(&table)?;
println!("Schema: {:?}", batch.schema());

// Arrow → QVD
let qvd_table = record_batch_to_qvd(&batch, "my_table")?;
write_qvd_file(&qvd_table, "output.qvd")?;
```

### Normalize — Qlik Sense compatibility

`normalize()` auto-detects and sets proper Qlik-compatible metadata on any QvdTable.

```rust
let mut table = qvd::read_qvd_file("data.qvd")?;

// Filter or modify the table
let matching = table.filter_by_values("Region", &["East", "West"]);
let mut subset = table.subset_rows(&matching);

// Normalize for Qlik compatibility before saving
subset.normalize();
qvd::write_qvd_file(&subset, "filtered.qvd")?;
```

What `normalize()` does:
- Converts DualInt → Int, DualDouble → Double (removes redundant string representations)
- Uses Int for float values that are exact integers (like Qlik does)
- Sets NumberFormat: `INTEGER` (`###0`), `REAL` (14 decimals), `ASCII`
- Sets Tags: `$numeric`, `$integer`, `$ascii`, `$text`
- Reserves NULL sentinel in BitWidth (`bits_needed(num_symbols + 1)`)
- Sorts BitOffsets by descending width (optimal packing)

> `normalize()` is called automatically during Parquet/Arrow → QVD conversion. Call it manually only when modifying existing tables.

### Streaming reader

```rust
use qvd::open_qvd_stream;

let mut reader = open_qvd_stream("huge_file.qvd")?;
println!("Total rows: {}", reader.total_rows());
println!("Columns: {:?}", reader.column_names());

// Process in chunks of 64K rows
while let Some(chunk) = reader.next_chunk(65536)? {
    println!("Chunk: {} rows starting at {}", chunk.num_rows, chunk.start_row);
    // chunk.values[col_idx][row_idx] — access individual values
}
```

### EXISTS() — O(1) lookup

Like Qlik's `EXISTS()` function — build an index of unique values and filter another table.

```rust
use qvd::{read_qvd_file, ExistsIndex, filter_rows_by_exists_fast};

// Build index from a table column
let clients = read_qvd_file("clients.qvd")?;
let index = ExistsIndex::from_column(&clients, "ClientID").unwrap();

// O(1) lookup
assert!(index.exists("12345"));
println!("Unique clients: {}", index.len());

// Filter another table
let facts = read_qvd_file("facts.qvd")?;
let col_idx = facts.column_index("ClientID").unwrap();
let matching_rows = filter_rows_by_exists_fast(&facts, col_idx, &index);
println!("Matching rows: {}", matching_rows.len());

// Create subset and save
let filtered = facts.subset_rows(&matching_rows);
qvd::write_qvd_file(&filtered, "filtered_facts.qvd")?;
```

### Streaming EXISTS() — filtered read (recommended for large files)

For large QVD files, `read_filtered()` streams the index table and only loads matching rows into memory.

```rust
use qvd::{open_qvd_stream, ExistsIndex, write_qvd_file};

// Build index from explicit values
let index = ExistsIndex::from_values(&["7", "9"]);

// Or from another table
let clients = read_qvd_file("clients.qvd")?;
let index = ExistsIndex::from_column(&clients, "ClientID").unwrap();
drop(clients); // free memory before opening the large file

// Stream + filter + select columns
let mut stream = open_qvd_stream("large_table.qvd")?;
let filtered = stream.read_filtered(
    "%Type_ID",                                     // filter column
    &index,                                         // EXISTS index
    Some(&["%Key_ID", "DateField_BK", "%Type_ID"]), // select columns (None = all)
    65536,                                          // chunk size
)?;
println!("Matched: {} rows × {} cols", filtered.num_rows(), filtered.num_cols());

// Save as QVD or Parquet
write_qvd_file(&filtered, "output.qvd")?;
```

### DataFusion SQL

```rust
use datafusion::prelude::*;
use qvd::register_qvd;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    register_qvd(&ctx, "sales", "sales.qvd")?;
    register_qvd(&ctx, "customers", "customers.qvd")?;

    // Aggregation
    let df = ctx.sql("SELECT Region, SUM(Amount) as total
                      FROM sales GROUP BY Region ORDER BY total DESC").await?;
    df.show().await?;

    // JOIN across QVD tables
    let df = ctx.sql("SELECT c.Name, COUNT(o.OrderID) as orders
                       FROM sales o
                       JOIN customers c ON o.CustomerID = c.CustomerID
                       GROUP BY c.Name").await?;
    df.show().await?;

    Ok(())
}
```

### Build QVD from scratch

```rust
use qvd::{QvdTableBuilder, write_qvd_file};

let table = QvdTableBuilder::new("my_table")
    .add_column("id", vec![Some("1".into()), Some("2".into()), Some("3".into())])
    .add_column("name", vec![Some("Alice".into()), Some("Bob".into()), None])
    .add_column("score", vec![Some("95.5".into()), Some("87".into()), Some("92".into())])
    .build();

write_qvd_file(&table, "output.qvd")?;
```

---

## Python Examples

### Read and write QVD

```python
import qvd

table = qvd.read_qvd("data.qvd")
print(table)              # QvdTable(table='data', rows=1000, cols=5)
print(table.columns)      # ['ID', 'Name', 'Region', 'Amount', 'Date']
print(table.num_rows)     # 1000
print(table.num_cols)     # 5
print(table.head(5))      # first 5 rows as formatted string

table.save("output.qvd")
```

### Convert Parquet ↔ QVD

```python
import qvd

# Parquet → QVD
qvd.convert_parquet_to_qvd("input.parquet", "output.qvd")

# QVD → Parquet
qvd.convert_qvd_to_parquet("input.qvd", "output.parquet", compression="zstd")

# Load Parquet as QvdTable, inspect, save
table = qvd.QvdTable.from_parquet("input.parquet")
print(table.columns)
table.save("output.qvd")
table.save_as_parquet("output.parquet", compression="snappy")
```

### PyArrow

```python
import qvd
import pyarrow as pa

# QVD → PyArrow (zero-copy via Arrow C Data Interface)
batch = qvd.read_qvd_to_arrow("data.qvd")
print(batch.schema)
print(batch.num_rows)

# Or via QvdTable
table = qvd.read_qvd("data.qvd")
batch = table.to_arrow()

# PyArrow → QVD
table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
table.save("output.qvd")

# Any PyArrow RecordBatch works — from pandas, Polars, DuckDB, etc.
batch = pa.RecordBatch.from_pydict({
    "id": pa.array([1, 2, 3]),
    "name": pa.array(["Alice", "Bob", "Charlie"]),
    "score": pa.array([95.5, 87.0, 92.3]),
})
qvd.QvdTable.from_arrow(batch, "scores").save("scores.qvd")
```

### pandas

```python
import qvd

# QVD → pandas DataFrame
df = qvd.read_qvd_to_pandas("data.qvd")
print(df.head())
print(df.dtypes)

# Or via QvdTable
df = qvd.read_qvd("data.qvd").to_pandas()

# pandas → QVD
import pyarrow as pa
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "my_table").save("output.qvd")
```

### Polars

```python
import qvd

# QVD → Polars DataFrame
df = qvd.read_qvd_to_polars("data.qvd")
print(df.head())
print(df.schema)

# Or via QvdTable
df = qvd.read_qvd("data.qvd").to_polars()

# Polars → QVD
batch = df.to_arrow()
qvd.QvdTable.from_arrow(batch, "my_table").save("output.qvd")
```

### EXISTS() — filter and subset

```python
import qvd

# Build index from a table column
clients = qvd.read_qvd("clients.qvd")
idx = qvd.ExistsIndex(clients, "ClientID")

# O(1) lookup
print("12345" in idx)               # True/False
print(idx.exists("12345"))          # same
print(len(idx))                     # unique values count
print(idx.exists_many(["1", "2"]))  # [True, False]

# Filter another table
facts = qvd.read_qvd("facts.qvd")
matching = qvd.filter_exists(facts, "ClientID", idx)
print(f"Matched {len(matching)} of {facts.num_rows} rows")

# Subset rows and save
filtered = facts.subset_rows(matching)
filtered.save("filtered_facts.qvd")
```

### EXISTS() from explicit values

```python
import qvd

# Build index from a list of values (like LOAD * INLINE in Qlik)
idx = qvd.ExistsIndex.from_values(["7", "9"])

# Streaming filtered read — memory-efficient for large files
filtered = qvd.read_qvd_filtered(
    "large_table.qvd",
    filter_col="%Type_ID",
    exists_index=idx,
    select=["%Key_ID", "DateField_BK", "%Type_ID"],  # only these columns
    chunk_size=65536
)
print(f"{filtered.num_rows} rows × {filtered.num_cols} cols")
filtered.save("output.qvd")
```

### Normalize for Qlik compatibility

```python
import qvd

table = qvd.read_qvd("data.qvd")

# Filter
matching = table.filter_by_values("Region", ["East", "West"])
subset = table.subset_rows(matching)

# Normalize before saving — sets proper types, tags, format
subset.normalize()
subset.save("filtered.qvd")
```

### DuckDB — register single file

```python
import qvd
import duckdb

conn = duckdb.connect()

# Register QVD as a DuckDB table
qvd.register_duckdb(conn, "sales", "sales.qvd")

# SQL queries
conn.sql("SELECT * FROM sales LIMIT 10").show()
conn.sql("SELECT Region, SUM(Amount) as total FROM sales GROUP BY Region").show()
conn.sql("SELECT COUNT(*), MIN(Amount), MAX(Amount), AVG(Amount) FROM sales").show()
```

### DuckDB — register multiple files

```python
import qvd
import duckdb

conn = duckdb.connect()

# Register multiple QVD files
qvd.register_duckdb(conn, "sales", "data/sales.qvd")
qvd.register_duckdb(conn, "customers", "data/customers.qvd")
qvd.register_duckdb(conn, "products", "data/products.qvd")

# JOIN across QVD tables
conn.sql("""
    SELECT c.Name, p.Category, SUM(s.Amount) as total
    FROM sales s
    JOIN customers c ON s.CustomerID = c.CustomerID
    JOIN products p ON s.ProductID = p.ProductID
    GROUP BY c.Name, p.Category
    ORDER BY total DESC
    LIMIT 20
""").show()
```

### DuckDB — register folder

```python
import qvd
import duckdb

conn = duckdb.connect()

# Register all QVD files from a folder (table name = file name without .qvd)
tables = qvd.register_duckdb_folder(conn, "data/qvd_files/")
print(f"Registered {len(tables)} tables: {tables}")

# Register from multiple folders
tables = qvd.register_duckdb_folder(conn,
    folder_paths=["data/sales/", "data/master/"])

# With glob pattern — only matching files
tables = qvd.register_duckdb_folder(conn,
    folder_paths="data/",
    glob="sales_*.qvd")

# Recursive scan of subdirectories
tables = qvd.register_duckdb_folder(conn,
    folder_paths="data/",
    recursive=True)

# Skip large files (default 500 MB limit)
tables = qvd.register_duckdb_folder(conn,
    folder_paths="data/",
    max_file_size_mb=200)

# All options together
tables = qvd.register_duckdb_folder(conn,
    folder_paths=["data/folder1/", "data/folder2/"],
    recursive=True,
    glob="*_2024.qvd",
    max_file_size_mb=500)

# Query any registered table
conn.sql("SELECT * FROM sales_2024 LIMIT 10").show()
conn.sql("SHOW TABLES").show()
```

### DuckDB — register with EXISTS() filter

```python
import qvd
import duckdb

conn = duckdb.connect()

# Register with streaming filter — only matching rows loaded
idx = qvd.ExistsIndex.from_values(["7", "9"])
qvd.register_duckdb_filtered(conn, "filtered", "large_table.qvd",
    filter_col="%Type_ID",
    exists_index=idx,
    select=["%Key_ID", "DateField_BK", "%Type_ID"],
    chunk_size=65536)

conn.sql("SELECT COUNT(*) FROM filtered").show()
conn.sql("SELECT %Type_ID, COUNT(*) as cnt FROM filtered GROUP BY %Type_ID").show()
```

### DuckDB — export results to QVD

```python
import qvd
import duckdb

conn = duckdb.connect()
qvd.register_duckdb(conn, "sales", "sales.qvd")

# Query → pandas
df = conn.sql("SELECT * FROM sales WHERE Amount > 1000").df()

# Query → PyArrow
batch = conn.sql("SELECT * FROM sales").arrow()

# Query result → QVD (aggregation, JOIN, filter — anything)
batch = conn.sql("SELECT Region, SUM(Amount) as total FROM sales GROUP BY Region").arrow()
qvd.QvdTable.from_arrow(batch, "summary").save("summary.qvd")
```

### Database → QVD (PostgreSQL, MySQL, SQLite, Snowflake, etc.)

Any database that can return Arrow/pandas data can save to QVD.
Dates, timestamps, integers, floats — all types are automatically converted.

```python
import qvd
import pyarrow as pa

# === PostgreSQL (via connectorx — fastest) ===
import connectorx as cx
table = cx.read_sql("postgresql://user:pass@host/db", "SELECT * FROM orders", return_type="arrow")
# connectorx returns PyArrow Table, convert to RecordBatch
batch = table.to_batches()[0]  # or combine if multiple batches
qvd.QvdTable.from_arrow(batch, "orders").save("orders.qvd")

# === PostgreSQL (via psycopg + pandas) ===
import psycopg2
import pandas as pd
conn = psycopg2.connect("host=localhost dbname=mydb user=user password=pass")
df = pd.read_sql("SELECT * FROM customers WHERE country = 'DE'", conn)
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "customers_de").save("customers_de.qvd")

# === SQLite ===
import sqlite3
conn = sqlite3.connect("data.db")
df = pd.read_sql("SELECT * FROM products", conn)
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "products").save("products.qvd")

# === DuckDB (local or remote) ===
import duckdb
conn = duckdb.connect("analytics.duckdb")
batch = conn.sql("SELECT * FROM fact_sales WHERE year = 2024").arrow()
qvd.QvdTable.from_arrow(batch, "fact_sales_2024").save("fact_sales_2024.qvd")

# === Snowflake (via snowflake-connector-python) ===
# pip install snowflake-connector-python[pandas]
import snowflake.connector
conn = snowflake.connector.connect(user='...', password='...', account='...')
cur = conn.cursor()
cur.execute("SELECT * FROM WAREHOUSE.SCHEMA.TABLE")
df = cur.fetch_pandas_all()
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "snowflake_data").save("snowflake_data.qvd")

# === BigQuery ===
from google.cloud import bigquery
client = bigquery.Client()
df = client.query("SELECT * FROM dataset.table LIMIT 1000000").to_dataframe()
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "bq_data").save("bq_data.qvd")

# === Any ADBC-compatible database ===
import adbc_driver_postgresql.dbapi
conn = adbc_driver_postgresql.dbapi.connect("postgresql://user:pass@host/db")
cur = conn.cursor()
cur.execute("SELECT * FROM large_table")
batch = cur.fetch_arrow_table().to_batches()[0]
qvd.QvdTable.from_arrow(batch, "adbc_data").save("adbc_data.qvd")
```

### CSV/Excel → QVD

```python
import qvd
import pandas as pd
import pyarrow as pa

# CSV → QVD
df = pd.read_csv("data.csv")
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "csv_data").save("data.qvd")

# Excel → QVD
df = pd.read_excel("report.xlsx", sheet_name="Sheet1")
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "excel_data").save("report.qvd")

# Multiple sheets → multiple QVDs
for sheet in ["Sales", "Customers", "Products"]:
    df = pd.read_excel("report.xlsx", sheet_name=sheet)
    batch = pa.RecordBatch.from_pandas(df)
    qvd.QvdTable.from_arrow(batch, sheet.lower()).save(f"{sheet.lower()}.qvd")
```

---

## CLI

```bash
cargo install qvd --features cli
```

Or via uvx (no install needed):

```bash
uvx --from qvdrs qvd-cli <command> [args]
```

### Convert between formats

```bash
# Parquet → QVD
qvd-cli convert input.parquet output.qvd

# QVD → Parquet (default: snappy)
qvd-cli convert input.qvd output.parquet

# QVD → Parquet with compression
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

```
File:       data.qvd
Size:       41.3 MB
Table:      SalesData
Rows:       465,810
Columns:    12
Created:    2024-01-15 10:30:00
Build:      50699
RecordSize: 89 bytes
Read time:  0.50s

Column                         Symbols BitWidth   Bias FmtType  Tags
--------------------------------------------------------------------------------
OrderID                         465810        20      0 INTEGER  $numeric, $integer
CustomerID                       12500        14      0 INTEGER  $numeric, $integer
Region                               5         3      0 ASCII    $ascii, $text
Amount                          389201        19      0 REAL     $numeric
```

### Preview rows

```bash
qvd-cli head data.qvd
qvd-cli head data.qvd --rows 50
```

### Filter with EXISTS() (streaming)

```bash
# Filter by column values
qvd-cli filter large.qvd output.qvd --column %Type_ID --values 7,9

# Filter + select columns
qvd-cli filter large.qvd output.qvd --column %Type_ID --values 7,9 \
    --select "%Key_ID,DateField_BK,%Type_ID"

# Filter → Parquet
qvd-cli filter large.qvd output.parquet --column %Type_ID --values 7,9 \
    --compression zstd
```

### Show Arrow schema

```bash
qvd-cli schema data.qvd
```

```
Arrow Schema for 'data.qvd':

  OrderID                        Int64
  CustomerID                     Int64
  Region                         Utf8
  Amount                         Float64 (nullable)
  OrderDate                      Date32
```

---

## Architecture

```
src/
├── lib.rs          — public API, re-exports
├── error.rs        — error types (QvdError, QvdResult)
├── header.rs       — XML header parser/writer
├── value.rs        — QVD data types (QvdSymbol, QvdValue)
├── symbol.rs       — symbol table binary reader/writer
├── index.rs        — index table bit-packed reader/writer
├── reader.rs       — QVD reader + normalize()
├── writer.rs       — QVD writer + QvdTableBuilder
├── exists.rs       — ExistsIndex + filter functions
├── streaming.rs    — streaming chunk-based reader with filtered reads
├── parquet.rs      — Parquet/Arrow ↔ QVD conversion (optional)
├── datafusion.rs   — DataFusion TableProvider (optional)
├── python.rs       — PyO3 bindings (optional)
└── bin/qvd.rs      — CLI binary (optional)
```

### QVD file format

A QVD file consists of three sections:

1. **XML header** — metadata: table name, field definitions (name, BitOffset, BitWidth, Bias, NumberFormat, Tags), record count
2. **Symbol tables** — unique values per column, each encoded as Int (0x01), Double (0x02), Text (0x04), DualInt (0x05), or DualDouble (0x06). Dates are stored as DualDouble (Qlik serial number + formatted string)
3. **Index table** — bit-packed rows, each row is `RecordByteSize` bytes. Fields are packed at their `BitOffset` with `BitWidth` bits. The stored value + `Bias` = symbol index. Index = `NoOfSymbols` means NULL

NumberFormat types: `UNKNOWN`, `ASCII`, `INTEGER`, `REAL`, `FIX`, `MONEY`, `DATE`, `TIMESTAMP`.
Tags: `$numeric`, `$integer`, `$text`, `$ascii`, `$timestamp`, `$date`, `$key`.

## Feature Flags

| Feature | Dependencies | Description |
|---------|-------------|-------------|
| *(default)* | none | Core QVD read/write |
| `parquet_support` | arrow, parquet, chrono | Parquet/Arrow conversion |
| `datafusion_support` | + datafusion, tokio | SQL queries via DataFusion |
| `cli` | + clap | CLI binary |
| `python` | + pyo3, arrow/pyarrow | Python bindings |

## Author

Stanislav Chernov ([@bintocher](https://github.com/bintocher))

## License

MIT — see [LICENSE](LICENSE)
