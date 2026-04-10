# API Reference

## Rust

| Function / Type | Description |
|---|---|
| `read_qvd_file(path)` | Read a QVD file into `QvdTable` |
| `write_qvd_file(table, path)` | Write `QvdTable` to QVD file |
| `QvdTableBuilder::new(name)` | Build a QVD table from scratch |
| `QvdTable::normalize()` | Auto-set Qlik-compatible types, tags, format |
| `open_qvd_stream(path)` | Open streaming chunk-based reader |
| `ExistsIndex::from_column(table, col)` | Build O(1) lookup index |
| `filter_rows_by_exists_fast(table, col, idx)` | Filter rows using EXISTS index |
| `concatenate(a, b)` | Append two QVD tables (strict schema) |
| `concatenate_with_schema(a, b, mode)` | Append with SchemaMode::Strict or Union |
| `concatenate_with_pk(a, b, pk, on_conflict)` | PK-based upsert/dedup merge |
| `concatenate_with_pk_schema(a, b, pk, on_conflict, mode)` | PK merge with schema mode |
| `convert_parquet_to_qvd(src, dst)` | Parquet -> QVD |
| `convert_qvd_to_parquet(src, dst, compression)` | QVD -> Parquet |
| `qvd_to_record_batch(table)` | QVD -> Arrow RecordBatch |
| `record_batch_to_qvd(batch, name)` | Arrow RecordBatch -> QVD |
| `write_record_batch_to_qvd(batch, name, path)` | Arrow -> QVD file directly |
| `register_qvd(ctx, name, path)` | Register QVD as DataFusion table |

## Python

| Function / Method | Description |
|---|---|
| `qvd.read_qvd(path)` | Read QVD into `QvdTable` |
| `table.save(path)` | Write QVD to file |
| `qvd.read_qvd_to_arrow(path)` | Read as PyArrow RecordBatch |
| `qvd.read_qvd_to_pandas(path)` | Read as pandas DataFrame |
| `qvd.read_qvd_to_polars(path)` | Read as Polars DataFrame |
| `qvd.read_qvd_filtered(path, col, idx, ...)` | Streaming filtered read |
| `QvdTable.from_arrow(batch, name)` | Create from PyArrow RecordBatch |
| `QvdTable.from_parquet(path)` | Create from Parquet file |
| `qvd.write_arrow(data, path, table_name)` | Write PyArrow RecordBatch/Table to QVD |
| `table.concatenate(other, schema)` | Append (schema: "strict" / "union") |
| `table.concatenate_pk(other, pk, on_conflict, schema)` | PK merge |
| `qvd.concatenate_qvd(existing, new, out, schema)` | File-level append |
| `qvd.concatenate_pk_qvd(existing, new, out, pk, ...)` | File-level PK merge |
| `qvd.ExistsIndex(table, col)` | Build EXISTS index |
| `qvd.filter_exists(table, col, idx)` | Filter rows |
| `qvd.register_duckdb(conn, name, path)` | Register as DuckDB table |
| `qvd.register_duckdb_folder(conn, path)` | Register folder of QVDs |
| `qvd.register_duckdb_filtered(conn, ...)` | Register with EXISTS filter |
| `qvd.convert_parquet_to_qvd(src, dst)` | Parquet -> QVD |
| `qvd.convert_qvd_to_parquet(src, dst, compression)` | QVD -> Parquet |

## Enums

### SchemaMode (Rust) / schema parameter (Python)

| Value | Description |
|---|---|
| `Strict` / `"strict"` | Error if column names differ (default) |
| `Union` / `"union"` | Fill missing columns with NULL (Qlik CONCATENATE behavior) |

### OnConflict (Rust) / on_conflict parameter (Python)

| Value | Description |
|---|---|
| `Replace` / `"replace"` | New rows win on PK collision (default) |
| `Skip` / `"skip"` | Existing rows win |
| `Error` / `"error"` | Return error on any collision |

## Performance

Tested on 399 real QVD files (11 KB -- 2.8 GB), all byte-identical roundtrip (MD5 match).

| File | Size | Rows | Columns | Read | Write |
|------|------|------|---------|------|-------|
| tiny | 11 KB | 12 | 5 | 0.0s | 0.0s |
| small | 418 KB | 2,746 | 8 | 0.0s | 0.0s |
| medium | 41 MB | 465,810 | 12 | 0.5s | 0.0s |
| large | 587 MB | 5,458,618 | 15 | 6.1s | 0.4s |
| xlarge | 1.7 GB | 87,617,047 | 8 | 23.6s | 1.6s |
| huge | 2.8 GB | 11,907,648 | 42 | 24.3s | 2.4s |

## Feature Flags

| Feature | Dependencies | Description |
|---------|-------------|-------------|
| *(default)* | none | Core QVD read/write |
| `parquet_support` | arrow, parquet, chrono | Parquet/Arrow conversion |
| `datafusion_support` | + datafusion, tokio | SQL queries via DataFusion |
| `cli` | + clap | CLI binary |
| `python` | + pyo3, arrow/pyarrow | Python bindings |

## Architecture

```
src/
├── lib.rs        — public API, re-exports
├── reader.rs     — QVD reader + QvdTable + normalize()
├── writer.rs     — QVD writer + QvdTableBuilder
├── concat.rs     — concatenate + PK merge/upsert
├── exists.rs     — ExistsIndex + filter functions
├── streaming.rs  — streaming chunk-based reader
├── parquet.rs    — Parquet/Arrow <-> QVD (optional)
├── datafusion.rs — DataFusion TableProvider (optional)
├── python.rs     — PyO3 bindings (optional)
└── bin/qvd.rs    — CLI binary (optional)
```
