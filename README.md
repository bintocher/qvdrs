# qvd

[![Crates.io](https://img.shields.io/crates/v/qvd.svg)](https://crates.io/crates/qvd)
[![PyPI](https://img.shields.io/pypi/v/qvd-rs.svg)](https://pypi.org/project/qvd-rs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance Rust library for reading, writing and converting Qlik QVD files. With Parquet/Arrow interop, streaming reader, CLI tool, and Python bindings.

**First and only QVD crate on crates.io.**

## Features

- **Read/Write QVD** — byte-identical roundtrip, zero-copy where possible
- **Parquet ↔ QVD** — convert in both directions with compression support (snappy, zstd, gzip, lz4)
- **Arrow RecordBatch** — convert QVD to/from Arrow for integration with DataFusion, DuckDB, Polars
- **Streaming reader** — read QVD files in chunks without loading everything into memory
- **EXISTS() index** — O(1) hash lookup, like Qlik's `EXISTS()` function
- **CLI tool** — `qvd-cli convert`, `inspect`, `head`, `schema`
- **Python bindings** — via PyO3/maturin, 20-35x faster than PyQvd
- **Zero dependencies** for core QVD read/write (Parquet/Arrow/Python are optional features)

## Performance

Tested on 20 real QVD files (11 KB to 2.8 GB):

| File | Size | Rows | Read | Write |
|------|------|------|------|-------|
| test_qvd.qvd | 11 KB | 12 | 0.0s | 0.0s |
| AAPL.qvd | 418 KB | 2,746 | 0.0s | 0.0s |
| kpi_snapshot.qvd | 41 MB | 465,810 | 0.5s | 0.0s |
| client_attribution.qvd | 587 MB | 5,458,618 | 6.1s | 0.4s |
| client_action_calendar.qvd | 1.7 GB | 87,617,047 | 36.8s | 1.6s |
| sportcatalogue.qvd | 2.8 GB | 11,907,648 | 24.3s | 2.4s |

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
qvd = "0.1"

# With Parquet/Arrow support
[dependencies]
qvd = { version = "0.1", features = ["parquet_support"] }
```

### CLI

```bash
cargo install qvd --features cli
```

### Python

```bash
pip install qvd-rs
```

Or with uv:

```bash
uv pip install qvd-rs
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

```rust
use qvd::{read_qvd_file, ExistsIndex, filter_rows_by_exists_fast};

let clients = read_qvd_file("clients.qvd")?;
let index = ExistsIndex::new(&clients, "ClientID");

// O(1) lookup
assert!(index.exists("12345"));

// Filter another table
let facts = read_qvd_file("facts.qvd")?;
let filtered = filter_rows_by_exists_fast(&facts, "ClientID", &index);
```

## Quick Start — Python

```python
import qvd

# Read QVD
table = qvd.read_qvd("data.qvd")
print(table.columns, table.num_rows)
print(table.head(5))

# Save QVD
table.save("output.qvd")

# Parquet → QVD
qvd.convert_parquet_to_qvd("input.parquet", "output.qvd")

# QVD → Parquet
qvd.convert_qvd_to_parquet("input.qvd", "output.parquet", compression="zstd")

# Load Parquet as QvdTable
table = qvd.QvdTable.from_parquet("input.parquet")
table.save("output.qvd")
table.save_as_parquet("output.parquet", compression="snappy")

# EXISTS — O(1) lookup
idx = qvd.ExistsIndex(table, "ClientID")
print("12345" in idx)  # True/False

# Filter rows
rows = qvd.filter_exists(other_table, "ClientID", idx)
```

## CLI

```bash
# Convert Parquet → QVD
qvd-cli convert input.parquet output.qvd

# Convert QVD → Parquet (with compression)
qvd-cli convert input.qvd output.parquet --compression zstd

# Inspect QVD metadata
qvd-cli inspect data.qvd

# Show first 20 rows
qvd-cli head data.qvd --rows 20

# Show Arrow schema
qvd-cli schema data.qvd
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
├── python.rs       — PyO3 bindings (optional)
└── bin/qvd.rs      — CLI binary (optional)
```

## Feature Flags

| Feature | Dependencies | Description |
|---------|-------------|-------------|
| *(default)* | none | Core QVD read/write |
| `parquet_support` | arrow, parquet, chrono | Parquet/Arrow conversion |
| `cli` | + clap | CLI binary |
| `python` | + pyo3 | Python bindings |

## License

MIT
