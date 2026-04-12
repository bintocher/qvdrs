# qvd

[![Crates.io](https://img.shields.io/crates/v/qvd.svg)](https://crates.io/crates/qvd)
[![PyPI](https://img.shields.io/pypi/v/qvdrs.svg)](https://pypi.org/project/qvdrs/)
[![npm](https://img.shields.io/npm/v/qvdrs.svg)](https://www.npmjs.com/package/qvdrs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance Rust library for reading, writing, converting and merging Qlik QVD files.

> **Disclaimer:** This project is **not** affiliated with Qlik Technologies or QlikTech. QVD is a proprietary format; all trademarks belong to Qlik. This library was built from scratch using publicly available information. See [LEGAL.md](LEGAL.md) for details.

## Features

- **Read/Write** QVD with byte-identical roundtrip (tested on 399 files up to 2.8 GB)
- **Streaming** chunk-based reader for large files
- **EXISTS()** O(1) index + filtered reads (2.5x faster than Qlik Sense)
- **Concatenate** — pure append with strict/union schema modes
- **PK Merge** — upsert/dedup by primary key (replace/skip/error). First QVD library with this
- **Parquet/Arrow** — bidirectional conversion, `write_arrow()` for direct Arrow-to-QVD
- **DuckDB / DataFusion** — register QVD as SQL tables
- **Python** — PyArrow, pandas, Polars via zero-copy Arrow bridge
- **Node.js / TypeScript** — native bindings via napi-rs, async I/O
- **CLI** — inspect, convert, filter, head

## Quick Start

```toml
# Cargo.toml
qvd = "0.7.0"
```
```rust
let table = qvd::read_qvd_file("data.qvd")?;
qvd::write_qvd_file(&table, "copy.qvd")?;
```

```bash
pip install qvdrs
```
```python
import qvd
table = qvd.read_qvd("data.qvd")
table.save("copy.qvd")
```

```bash
npm install qvdrs
```
```typescript
import { readQvd, saveQvd } from 'qvdrs'
const table = await readQvd('data.qvd')
await saveQvd(table, 'copy.qvd')
```

## Documentation

| | |
|---|---|
| [Rust Examples](docs/rust_examples.md) | Read/write, streaming, EXISTS, Parquet, Arrow, concat, PK merge, DataFusion |
| [Python Examples](docs/python_examples.md) | Read/write, Arrow, pandas, Polars, DuckDB, concat, PK merge, workflows |
| [TypeScript Examples](docs/typescript_examples.md) | Read/write, EXISTS, concat, PK merge, async patterns |
| [API Reference](docs/api_reference.md) | Full API tables for Rust, Python, and TypeScript |
| [Release Notes](RELEASE_NOTES.md) | Changelog for all versions |

## License

MIT — Stanislav Chernov ([@bintocher](https://github.com/bintocher))
