# Release Notes

---

## v0.6.0

### New Features

- **`write_arrow`** — write PyArrow RecordBatch or Table directly to QVD in a single call. No Parquet roundtrip needed when working with DuckDB, Arrow, or any PyArrow-compatible source. Handles `combine_chunks` internally for multi-chunk Tables.

- **`concatenate` (pure append)** — merge two QVD tables with Qlik CONCATENATE semantics. Columns are matched by name. Available as a Rust function, Python method (`QvdTable.concatenate()`), and top-level Python function (`qvd.concatenate_qvd()`).

- **`concatenate_with_pk` (PK-based upsert/dedup)** — merge with primary key deduplication. **First QVD library in any language with PK-based merge.** Supports composite keys, three conflict strategies (`replace` / `skip` / `error`), and matches Qlik `CONCATENATE + WHERE NOT EXISTS(pk)` semantics.

- **`SchemaMode`** — schema validation for concatenation: `strict` (default, error on column mismatch) or `union` (fill missing columns with NULL).

- **Python bindings** for all new functions: `write_arrow`, `concatenate`, `concatenate_pk`, `concatenate_qvd`, `concatenate_pk_qvd`.

### Testing

- 10 Rust tests + 90 Python tests passing.

### Migration

No breaking changes. All new features are purely additive.

---

## v0.5.0

- **Qlik Sense-compatible QVD output** — binary-identical to Qlik. Generated QVD files are now byte-for-byte identical to files produced by Qlik Sense, ensuring full compatibility.

---

## v0.4.4

- **Native DuckDB integration** — register QVD files as SQL-queryable DuckDB tables with `register_duckdb()`. One call to register, then query with standard SQL.

---

## v0.4.3

- **DuckDB native integration extensions** — added `register_duckdb_folder()` to bulk-register entire directories of QVD files as DuckDB tables. Supports recursive scanning, glob patterns, and file size limits.
- **Python bindings for streaming EXISTS filter** — `read_qvd_filtered()` and `register_duckdb_filtered()` now available from Python. Stream-reads only matching rows for large files.

---

## v0.4.2

- README updates with correct version numbers and `uvx` instructions.
- Crate publish fixes for crates.io.

---

## v0.4.1

- Fix README examples and update CI/CD to Node.js 24.
- Update GitHub Actions to Node.js 24 compatible versions (v5/v6).
- Remove PyQvd comparison from benchmarks.

---

## v0.4.0

- **Streaming EXISTS() filter** — 2.5x faster than Qlik Sense for filtered reads. `ExistsIndex` provides O(1) hash-based lookups matching Qlik's `EXISTS()` function.
- Column selection support in streaming reads.

---

## v0.3.0

- **Fix Qlik Sense incompatibility** — generated QVD files are now readable by Qlik Sense. Fixed critical bug where XML parser picked field-level Offset instead of table-level Offset.

---

## v0.2.0

- **DataFusion SQL engine integration** for server-side SQL queries on QVD data.
- **PyArrow / pandas / Polars conversion** — `to_arrow()`, `to_pandas()`, `to_polars()`, `from_arrow()`.
- **DuckDB integration** via Arrow bridge.

---

## v0.1.1

- Fix QVD to Parquet/Arrow index out of bounds panic.

---

## v0.1.0

- **Initial release** — QVD file reader/writer library.
- Parquet/Arrow conversion, streaming reader, CLI tool.
- CI/CD pipeline for PyPI and crates.io publishing.
