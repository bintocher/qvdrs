# Python Examples

## Read and Write QVD

```python
import qvd

table = qvd.read_qvd("data.qvd")
print(table)              # QvdTable(table='data', rows=1000, cols=5)
print(table.columns)      # ['ID', 'Name', 'Region', 'Amount', 'Date']
print(table.head(5))

table.save("output.qvd")
```

## Convert Parquet <-> QVD

```python
import qvd

qvd.convert_parquet_to_qvd("input.parquet", "output.qvd")
qvd.convert_qvd_to_parquet("input.qvd", "output.parquet", compression="zstd")

table = qvd.QvdTable.from_parquet("input.parquet")
table.save("output.qvd")
```

## PyArrow

```python
import qvd
import pyarrow as pa

# QVD -> PyArrow (zero-copy via Arrow C Data Interface)
batch = qvd.read_qvd_to_arrow("data.qvd")

# PyArrow -> QVD
table = qvd.QvdTable.from_arrow(batch, table_name="my_table")
table.save("output.qvd")

# Any PyArrow RecordBatch works
batch = pa.RecordBatch.from_pydict({
    "id": pa.array([1, 2, 3]),
    "name": pa.array(["Alice", "Bob", "Charlie"]),
})
qvd.QvdTable.from_arrow(batch, "scores").save("scores.qvd")
```

## pandas

```python
import qvd

df = qvd.read_qvd_to_pandas("data.qvd")

# pandas -> QVD
import pyarrow as pa
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "my_table").save("output.qvd")
```

## Polars

```python
import qvd

df = qvd.read_qvd_to_polars("data.qvd")

# Polars -> QVD
batch = df.to_arrow()
qvd.QvdTable.from_arrow(batch, "my_table").save("output.qvd")
```

## EXISTS() -- Filter and Subset

```python
import qvd

clients = qvd.read_qvd("clients.qvd")
idx = qvd.ExistsIndex(clients, "ClientID")

print("12345" in idx)               # True/False
print(len(idx))                     # unique values count

facts = qvd.read_qvd("facts.qvd")
matching = qvd.filter_exists(facts, "ClientID", idx)
filtered = facts.subset_rows(matching)
filtered.save("filtered_facts.qvd")
```

## EXISTS() from Explicit Values

```python
import qvd

idx = qvd.ExistsIndex.from_values(["7", "9"])

filtered = qvd.read_qvd_filtered(
    "large_table.qvd",
    filter_col="%Type_ID",
    exists_index=idx,
    select=["%Key_ID", "DateField_BK", "%Type_ID"],
    chunk_size=65536
)
filtered.save("output.qvd")
```

## Normalize for Qlik Compatibility

```python
import qvd

table = qvd.read_qvd("data.qvd")
matching = table.filter_by_values("Region", ["East", "West"])
subset = table.subset_rows(matching)

subset.normalize()
subset.save("filtered.qvd")
```

## DuckDB -- Register and Query

```python
import qvd
import duckdb

conn = duckdb.connect()

qvd.register_duckdb(conn, "sales", "sales.qvd")
conn.sql("SELECT Region, SUM(Amount) as total FROM sales GROUP BY Region").show()
```

## DuckDB -- Register Folder

```python
import qvd
import duckdb

conn = duckdb.connect()

tables = qvd.register_duckdb_folder(conn, "data/qvd_files/")
tables = qvd.register_duckdb_folder(conn,
    folder_paths=["data/sales/", "data/master/"],
    recursive=True,
    glob="*_2024.qvd",
    max_file_size_mb=500)
```

## DuckDB -- Register with EXISTS() Filter

```python
import qvd
import duckdb

conn = duckdb.connect()

idx = qvd.ExistsIndex.from_values(["7", "9"])
qvd.register_duckdb_filtered(conn, "filtered", "large_table.qvd",
    filter_col="%Type_ID",
    exists_index=idx,
    select=["%Key_ID", "DateField_BK", "%Type_ID"])

conn.sql("SELECT COUNT(*) FROM filtered").show()
```

## DuckDB -- Export Results to QVD

```python
import qvd
import duckdb

conn = duckdb.connect()
qvd.register_duckdb(conn, "sales", "sales.qvd")

batch = conn.sql("SELECT Region, SUM(Amount) as total FROM sales GROUP BY Region").arrow()
qvd.QvdTable.from_arrow(batch, "summary").save("summary.qvd")
```

## write_arrow -- PyArrow Direct to QVD

```python
import qvd

# DuckDB query -> Arrow -> QVD in one step
result = conn.sql("SELECT * FROM sales").arrow()
qvd.write_arrow(result, "summary.qvd", table_name="summary")

# Works with any PyArrow RecordBatch or Table
import pyarrow as pa
batch = pa.RecordBatch.from_pydict({"id": pa.array([1, 2, 3])})
qvd.write_arrow(batch, "people.qvd")
```

## Concatenate -- Merge QVD Tables

```python
import qvd

jan = qvd.read_qvd("data_jan.qvd")
feb = qvd.read_qvd("data_feb.qvd")

merged = jan.concatenate(feb)
merged.save("data_all.qvd")

# Or via top-level function
qvd.concatenate_qvd("data_jan.qvd", "data_feb.qvd", "data_all.qvd")
```

## Concatenate with PK -- Upsert/Dedup Merge

```python
import qvd

existing = qvd.read_qvd("master.qvd")
delta = qvd.read_qvd("delta.qvd")

merged = existing.concatenate_pk(delta, pk="ID", on_conflict="replace")
merged.save("master_updated.qvd")

# Skip: existing rows win (like Qlik WHERE NOT EXISTS)
merged = existing.concatenate_pk(delta, pk="ID", on_conflict="skip")

# Composite primary key
merged = existing.concatenate_pk(delta, pk=["Region", "Date"], on_conflict="replace")
```

## Database -> QVD (PostgreSQL, MySQL, SQLite, Snowflake, etc.)

Any database that can return Arrow/pandas data can save to QVD.

```python
import qvd
import pyarrow as pa

# PostgreSQL (via connectorx)
import connectorx as cx
table = cx.read_sql("postgresql://user:pass@host/db", "SELECT * FROM orders", return_type="arrow")
batch = table.to_batches()[0]
qvd.QvdTable.from_arrow(batch, "orders").save("orders.qvd")

# DuckDB
import duckdb
conn = duckdb.connect("analytics.duckdb")
batch = conn.sql("SELECT * FROM fact_sales WHERE year = 2024").arrow()
qvd.QvdTable.from_arrow(batch, "fact_sales_2024").save("fact_sales_2024.qvd")
```

## CSV/Excel -> QVD

```python
import qvd
import pandas as pd
import pyarrow as pa

df = pd.read_csv("data.csv")
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "csv_data").save("data.qvd")

df = pd.read_excel("report.xlsx", sheet_name="Sheet1")
batch = pa.RecordBatch.from_pandas(df)
qvd.QvdTable.from_arrow(batch, "excel_data").save("report.qvd")
```
