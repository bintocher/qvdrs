# Rust Examples

## Read and Write QVD

```rust
use qvd::{read_qvd_file, write_qvd_file};

let table = read_qvd_file("data.qvd")?;
println!("Rows: {}, Cols: {}", table.num_rows(), table.num_cols());
println!("Columns: {:?}", table.column_names());

for row in 0..5 {
    let val = table.get(row, 0);
    println!("Row {}: {:?}", row, val.as_string());
}

write_qvd_file(&table, "output.qvd")?;
```

## Convert Parquet <-> QVD

```rust
use qvd::{convert_parquet_to_qvd, convert_qvd_to_parquet, ParquetCompression};

convert_parquet_to_qvd("input.parquet", "output.qvd")?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Zstd)?;
convert_qvd_to_parquet("input.qvd", "output.parquet", ParquetCompression::Snappy)?;
```

## Arrow RecordBatch

```rust
use qvd::{read_qvd_file, qvd_to_record_batch, record_batch_to_qvd, write_qvd_file};

let table = read_qvd_file("data.qvd")?;
let batch = qvd_to_record_batch(&table)?;

let qvd_table = record_batch_to_qvd(&batch, "my_table")?;
write_qvd_file(&qvd_table, "output.qvd")?;
```

## Build QVD from Scratch

```rust
use qvd::{QvdTableBuilder, write_qvd_file};

let table = QvdTableBuilder::new("my_table")
    .add_column("id", vec![Some("1".into()), Some("2".into()), Some("3".into())])
    .add_column("name", vec![Some("Alice".into()), Some("Bob".into()), None])
    .add_column("score", vec![Some("95.5".into()), Some("87".into()), Some("92".into())])
    .build();

write_qvd_file(&table, "output.qvd")?;
```

## Normalize for Qlik Compatibility

`normalize()` auto-detects and sets proper Qlik-compatible metadata on any QvdTable.

```rust
let mut table = qvd::read_qvd_file("data.qvd")?;
let matching = table.filter_by_values("Region", &["East", "West"]);
let mut subset = table.subset_rows(&matching);

subset.normalize();
qvd::write_qvd_file(&subset, "filtered.qvd")?;
```

What `normalize()` does:
- Converts DualInt -> Int, DualDouble -> Double (removes redundant string representations)
- Uses Int for float values that are exact integers (like Qlik does)
- Sets NumberFormat: `INTEGER` (`###0`), `REAL` (14 decimals), `ASCII`
- Sets Tags: `$numeric`, `$integer`, `$ascii`, `$text`
- Reserves NULL sentinel in BitWidth (`bits_needed(num_symbols + 1)`)
- Sorts BitOffsets by descending width (optimal packing)

> `normalize()` is called automatically during Parquet/Arrow -> QVD conversion. Call it manually only when modifying existing tables.

## Streaming Reader

```rust
use qvd::open_qvd_stream;

let mut reader = open_qvd_stream("huge_file.qvd")?;
println!("Total rows: {}", reader.total_rows());

while let Some(chunk) = reader.next_chunk(65536)? {
    println!("Chunk: {} rows starting at {}", chunk.num_rows, chunk.start_row);
}
```

## EXISTS() -- O(1) Lookup

```rust
use qvd::{read_qvd_file, ExistsIndex, filter_rows_by_exists_fast};

let clients = read_qvd_file("clients.qvd")?;
let index = ExistsIndex::from_column(&clients, "ClientID").unwrap();

assert!(index.exists("12345"));

let facts = read_qvd_file("facts.qvd")?;
let col_idx = facts.column_index("ClientID").unwrap();
let matching_rows = filter_rows_by_exists_fast(&facts, col_idx, &index);
let filtered = facts.subset_rows(&matching_rows);
qvd::write_qvd_file(&filtered, "filtered_facts.qvd")?;
```

## Streaming EXISTS() -- Filtered Read (Recommended for Large Files)

```rust
use qvd::{open_qvd_stream, ExistsIndex, write_qvd_file};

let index = ExistsIndex::from_values(&["7", "9"]);

let mut stream = open_qvd_stream("large_table.qvd")?;
let filtered = stream.read_filtered(
    "%Type_ID",                                     // filter column
    &index,                                         // EXISTS index
    Some(&["%Key_ID", "DateField_BK", "%Type_ID"]), // select columns (None = all)
    65536,                                          // chunk size
)?;

write_qvd_file(&filtered, "output.qvd")?;
```

## DataFusion SQL

```rust
use datafusion::prelude::*;
use qvd::register_qvd;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    register_qvd(&ctx, "sales", "sales.qvd")?;
    register_qvd(&ctx, "customers", "customers.qvd")?;

    let df = ctx.sql("SELECT Region, SUM(Amount) as total
                      FROM sales GROUP BY Region ORDER BY total DESC").await?;
    df.show().await?;
    Ok(())
}
```

## Concatenate -- Merge QVD Tables

```rust
use qvd::{read_qvd_file, concatenate, write_qvd_file};

let jan = read_qvd_file("data_jan.qvd")?;
let feb = read_qvd_file("data_feb.qvd")?;

let merged = concatenate(&jan, &feb)?;
write_qvd_file(&merged, "data_all.qvd")?;
```

## Concatenate with PK -- Upsert/Dedup Merge

```rust
use qvd::{read_qvd_file, concatenate_with_pk, OnConflict, write_qvd_file};

let existing = read_qvd_file("master.qvd")?;
let delta = read_qvd_file("delta.qvd")?;

// Upsert: new rows win on PK collision
let merged = concatenate_with_pk(&existing, &delta, &["ID"], OnConflict::Replace)?;
write_qvd_file(&merged, "master_updated.qvd")?;

// Skip: existing rows win (like WHERE NOT EXISTS)
let merged = concatenate_with_pk(&existing, &delta, &["ID"], OnConflict::Skip)?;

// Composite primary key
let merged = concatenate_with_pk(&existing, &delta, &["Region", "Date"], OnConflict::Replace)?;
```
