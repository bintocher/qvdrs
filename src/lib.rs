//! # qvd — High-performance Qlik QVD file library
//!
//! Read, write, and convert [Qlik QVD](https://help.qlik.com/en-US/sense/February2024/Subsystems/Hub/Content/Sense_Hub/Scripting/QVD-files-scripting.htm)
//! files with zero-copy roundtrip fidelity. First and only QVD crate on crates.io.
//!
//! ## Features
//!
//! - **Read/Write QVD** — byte-identical roundtrip (MD5 match on 20 real files up to 2.8 GB)
//! - **Parquet ↔ QVD** — bidirectional conversion with compression (snappy, zstd, gzip, lz4).
//!   Requires feature `parquet_support`.
//! - **Arrow RecordBatch** — convert QVD to/from Arrow for DataFusion, DuckDB, Polars integration.
//!   Requires feature `parquet_support`.
//! - **DataFusion SQL** — register QVD as a table, query with SQL.
//!   Requires feature `datafusion_support`.
//! - **Streaming reader** — read QVD in chunks without loading entire file into memory
//! - **EXISTS() index** — O(1) hash lookup, like Qlik's `EXISTS()` function
//! - **Python bindings** — PyArrow, pandas, Polars via zero-copy Arrow bridge
//! - **Zero dependencies** for core read/write (Parquet/Arrow/DataFusion are optional)
//!
//! ## Quick Start
//!
//! ### Read and write QVD files
//!
//! ```no_run
//! use qvd::{read_qvd_file, write_qvd_file};
//!
//! let table = read_qvd_file("data.qvd").unwrap();
//! println!("Rows: {}, Cols: {}", table.num_rows(), table.num_cols());
//! println!("Columns: {:?}", table.column_names());
//!
//! // Byte-identical roundtrip
//! write_qvd_file(&table, "output.qvd").unwrap();
//! ```
//!
//! ### EXISTS() — O(1) lookup
//!
//! ```no_run
//! use qvd::{read_qvd_file, ExistsIndex, filter_rows_by_exists_fast};
//!
//! let clients = read_qvd_file("clients.qvd").unwrap();
//! let index = ExistsIndex::from_column(&clients, "ClientID").unwrap();
//!
//! assert!(index.exists("12345"));
//!
//! let facts = read_qvd_file("facts.qvd").unwrap();
//! // col_idx = column index for "ClientID" in facts table
//! let col_idx = 0;
//! let filtered = filter_rows_by_exists_fast(&facts, col_idx, &index);
//! ```
//!
//! ### Streaming reader
//!
//! ```no_run
//! use qvd::open_qvd_stream;
//!
//! let mut reader = open_qvd_stream("huge_file.qvd").unwrap();
//! while let Some(chunk) = reader.next_chunk(65536).unwrap() {
//!     println!("Chunk: {} rows", chunk.num_rows);
//! }
//! ```
//!
//! ### Parquet ↔ QVD (feature `parquet_support`)
//!
//! ```ignore
//! use qvd::{convert_parquet_to_qvd, convert_qvd_to_parquet, ParquetCompression};
//!
//! convert_parquet_to_qvd("input.parquet", "output.qvd").unwrap();
//! convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Zstd).unwrap();
//! ```
//!
//! ### Arrow RecordBatch (feature `parquet_support`)
//!
//! ```ignore
//! use qvd::{read_qvd_file, qvd_to_record_batch, record_batch_to_qvd};
//!
//! let table = read_qvd_file("data.qvd").unwrap();
//! let batch = qvd_to_record_batch(&table).unwrap();
//! // Use with DataFusion, DuckDB, Polars...
//! ```
//!
//! ### DataFusion SQL (feature `datafusion_support`)
//!
//! ```ignore
//! use datafusion::prelude::*;
//! use qvd::register_qvd;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ctx = SessionContext::new();
//!     register_qvd(&ctx, "sales", "sales.qvd")?;
//!     let df = ctx.sql("SELECT Region, SUM(Amount) FROM sales GROUP BY Region").await?;
//!     df.show().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Dependencies | Description |
//! |---------|-------------|-------------|
//! | *(default)* | none | Core QVD read/write, streaming, EXISTS |
//! | `parquet_support` | arrow, parquet, chrono | Parquet/Arrow ↔ QVD conversion |
//! | `datafusion_support` | + datafusion, tokio | SQL queries on QVD via DataFusion |
//! | `cli` | + clap | CLI binary `qvd-cli` |
//! | `python` | + pyo3, arrow/pyarrow | Python bindings with PyArrow/pandas/Polars |

/// Error types for QVD operations.
pub mod error;
/// QVD XML header parser and writer.
pub mod header;
/// Binary symbol table reader and writer.
pub mod symbol;
/// QVD value types: [`QvdSymbol`] and [`QvdValue`].
pub mod value;
/// Bit-stuffed index table reader and writer.
pub mod index;
/// High-level QVD file reader. See [`read_qvd_file`] and [`QvdTable`].
pub mod reader;
/// High-level QVD file writer and [`QvdTableBuilder`] for creating QVD files from scratch.
pub mod writer;
/// O(1) EXISTS() index and fast row filtering. See [`ExistsIndex`].
pub mod exists;
/// Streaming chunk-based QVD reader for memory-efficient processing of large files.
/// See [`QvdStreamReader`] and [`open_qvd_stream`].
pub mod streaming;

/// Parquet/Arrow ↔ QVD conversion (requires feature `parquet_support`).
#[cfg(any(feature = "parquet_support", feature = "python"))]
pub mod parquet;

/// DataFusion integration — SQL queries on QVD files (requires feature `datafusion_support`).
#[cfg(feature = "datafusion_support")]
pub mod datafusion;

#[cfg(feature = "python")]
#[doc(hidden)]
pub mod python;

pub use error::{QvdError, QvdResult};
pub use header::QvdTableHeader;
pub use reader::{read_qvd, read_qvd_file, QvdTable};
pub use writer::{write_qvd, write_qvd_file, QvdTableBuilder};
pub use exists::{ExistsIndex, filter_rows_by_exists, filter_rows_by_exists_fast};
pub use value::{QvdSymbol, QvdValue};
pub use streaming::{QvdStreamReader, QvdChunk, open_qvd_stream};

#[cfg(any(feature = "parquet_support", feature = "python"))]
pub use parquet::{
    read_parquet_to_qvd, convert_parquet_to_qvd,
    qvd_to_record_batch, record_batch_to_qvd, write_record_batch_to_qvd,
    convert_qvd_to_parquet, write_qvd_table_to_parquet,
    parquet_to_qvd, qvd_to_parquet,
    ParquetCompression,
};

#[cfg(feature = "datafusion_support")]
pub use crate::datafusion::{QvdTableProvider, register_qvd};
