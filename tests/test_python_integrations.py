"""
Integration tests for qvd Python bindings:
- PyArrow RecordBatch conversion
- pandas DataFrame conversion
- Polars DataFrame conversion
- DuckDB via Arrow bridge
- Data type preservation across conversions
- Round-trip: create data → QVD → Arrow/pandas/Polars → verify types & values
"""
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

# ── helpers ──────────────────────────────────────────────────────────

PASSED = 0
FAILED = 0

def ok(name: str):
    global PASSED
    PASSED += 1
    print(f"  PASS  {name}")

def fail(name: str, msg: str):
    global FAILED
    FAILED += 1
    print(f"  FAIL  {name}: {msg}")

def assert_eq(name: str, actual, expected):
    if actual == expected:
        ok(name)
    else:
        fail(name, f"expected {expected!r}, got {actual!r}")

def assert_close(name: str, actual, expected, tol=1e-6):
    if abs(actual - expected) < tol:
        ok(name)
    else:
        fail(name, f"expected {expected!r}, got {actual!r}")

def assert_true(name: str, condition, msg="condition is False"):
    if condition:
        ok(name)
    else:
        fail(name, msg)

# ── create test QVD file ─────────────────────────────────────────────

def create_test_qvd(path: str):
    """Create a QVD file with various data types using pyarrow + qvd."""
    import pyarrow as pa
    import qvd

    # Build a RecordBatch with multiple column types
    batch = pa.RecordBatch.from_pydict({
        "id":          pa.array([1, 2, 3, 4, 5],              type=pa.int64()),
        "name":        pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.utf8()),
        "score":       pa.array([95.5, 87.3, 92.1, 78.9, 100.0], type=pa.float64()),
        "active":      pa.array(["true", "false", "true", "true", "false"], type=pa.utf8()),
        "amount_int":  pa.array([1000, 2500, -300, 0, 42],    type=pa.int64()),
        "rating":      pa.array([4.5, 3.8, None, 5.0, None],  type=pa.float64()),
        "city":        pa.array(["Moscow", "London", "Moscow", None, "London"], type=pa.utf8()),
    })

    table = qvd.QvdTable.from_arrow(batch, table_name="test_data")
    table.save(path)
    print(f"  Created test QVD: {path} ({table.num_rows} rows, {table.num_cols} cols)")
    return batch


# ── Test 1: Basic QVD read/write ─────────────────────────────────────

def test_basic_read_write(qvd_path: str):
    print("\n== Test 1: Basic QVD read/write ==")
    import qvd

    table = qvd.read_qvd(qvd_path)
    assert_eq("num_rows", table.num_rows, 5)
    assert_eq("num_cols", table.num_cols, 7)
    assert_eq("columns", table.columns, ["id", "name", "score", "active", "amount_int", "rating", "city"])
    assert_eq("table_name", table.table_name, "test_data")

    # Cell access
    assert_eq("get(0,1) = Alice", table.get(0, 1), "Alice")
    assert_eq("get_by_name(2, 'score')", table.get_by_name(2, "score"), "92.1")

    # head()
    rows = table.head(2)
    assert_eq("head len", len(rows), 2)
    assert_eq("head[0]['name']", rows[0]["name"], "Alice")

    # Roundtrip save
    with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as f:
        tmp = f.name
    table.save(tmp)
    table2 = qvd.read_qvd(tmp)
    assert_eq("roundtrip rows", table2.num_rows, 5)
    assert_eq("roundtrip cols", table2.num_cols, 7)
    os.unlink(tmp)


# ── Test 2: PyArrow conversion ───────────────────────────────────────

def test_pyarrow(qvd_path: str, original_batch):
    print("\n== Test 2: PyArrow conversion ==")
    import pyarrow as pa
    import qvd

    table = qvd.read_qvd(qvd_path)
    batch = table.to_arrow()

    assert_true("is RecordBatch", isinstance(batch, pa.RecordBatch))
    assert_eq("arrow num_rows", batch.num_rows, 5)
    assert_eq("arrow num_columns", batch.num_columns, 7)

    # Check column names
    schema = batch.schema
    col_names = [schema.field(i).name for i in range(len(schema))]
    assert_eq("arrow columns", col_names, ["id", "name", "score", "active", "amount_int", "rating", "city"])

    # Check data types (QVD stores as symbols, so types may differ)
    print(f"  Arrow schema: {schema}")

    # Check values
    ids = batch.column("id").to_pylist()
    assert_true("ids are numeric", all(isinstance(x, (int, float)) or x is None for x in ids),
                f"ids: {ids}")

    names = batch.column("name").to_pylist()
    assert_eq("names", names, ["Alice", "Bob", "Charlie", "Diana", "Eve"])

    scores = batch.column("score").to_pylist()
    assert_true("scores are numeric", all(isinstance(x, (int, float)) or x is None for x in scores),
                f"scores: {scores}")

    # Check nullable column
    rating = batch.column("rating").to_pylist()
    assert_true("rating has nulls", rating[2] is None, f"rating: {rating}")
    assert_true("rating has nulls 2", rating[4] is None, f"rating: {rating}")

    city = batch.column("city").to_pylist()
    assert_true("city has null", city[3] is None, f"city: {city}")

    # read_qvd_to_arrow convenience function
    batch2 = qvd.read_qvd_to_arrow(qvd_path)
    assert_true("read_qvd_to_arrow is RecordBatch", isinstance(batch2, pa.RecordBatch))
    assert_eq("read_qvd_to_arrow rows", batch2.num_rows, 5)


# ── Test 3: Arrow round-trip ─────────────────────────────────────────

def test_arrow_roundtrip(qvd_path: str):
    print("\n== Test 3: Arrow round-trip (QVD → Arrow → QVD → Arrow) ==")
    import pyarrow as pa
    import qvd

    # QVD → Arrow
    table1 = qvd.read_qvd(qvd_path)
    batch1 = table1.to_arrow()

    # Arrow → QVD
    table2 = qvd.QvdTable.from_arrow(batch1, table_name="roundtrip_test")
    assert_eq("roundtrip table_name", table2.table_name, "roundtrip_test")

    # QVD → Arrow again
    batch2 = table2.to_arrow()

    # Compare
    assert_eq("roundtrip num_rows", batch2.num_rows, batch1.num_rows)
    assert_eq("roundtrip num_columns", batch2.num_columns, batch1.num_columns)

    # Compare values column by column
    for i in range(batch1.num_columns):
        col1 = batch1.column(i).to_pylist()
        col2 = batch2.column(i).to_pylist()
        name = batch1.schema.field(i).name
        # Compare as strings since types may change across roundtrips
        str1 = [str(x) if x is not None else None for x in col1]
        str2 = [str(x) if x is not None else None for x in col2]
        assert_eq(f"roundtrip col '{name}'", str2, str1)


# ── Test 4: pandas conversion ────────────────────────────────────────

def test_pandas(qvd_path: str):
    print("\n== Test 4: pandas conversion ==")
    import pandas as pd
    import qvd

    table = qvd.read_qvd(qvd_path)
    df = table.to_pandas()

    assert_true("is DataFrame", isinstance(df, pd.DataFrame))
    assert_eq("pandas shape", df.shape, (5, 7))
    assert_eq("pandas columns", list(df.columns), ["id", "name", "score", "active", "amount_int", "rating", "city"])

    # Check values
    assert_eq("pandas names", list(df["name"]), ["Alice", "Bob", "Charlie", "Diana", "Eve"])

    # Check nulls
    assert_true("pandas rating nulls", pd.isna(df["rating"].iloc[2]),
                f"rating[2] = {df['rating'].iloc[2]}")
    assert_true("pandas city null", pd.isna(df["city"].iloc[3]),
                f"city[3] = {df['city'].iloc[3]}")

    # read_qvd_to_pandas convenience
    df2 = qvd.read_qvd_to_pandas(qvd_path)
    assert_true("read_qvd_to_pandas is DataFrame", isinstance(df2, pd.DataFrame))
    assert_eq("read_qvd_to_pandas shape", df2.shape, (5, 7))

    # pandas → QVD round-trip
    import pyarrow as pa
    batch = pa.RecordBatch.from_pandas(df)
    table2 = qvd.QvdTable.from_arrow(batch, table_name="from_pandas")
    assert_eq("pandas roundtrip rows", table2.num_rows, 5)


# ── Test 5: Polars conversion ────────────────────────────────────────

def test_polars(qvd_path: str):
    print("\n== Test 5: Polars conversion ==")
    import polars as pl
    import qvd

    table = qvd.read_qvd(qvd_path)
    df = table.to_polars()

    assert_true("is Polars DataFrame", isinstance(df, pl.DataFrame))
    assert_eq("polars shape", df.shape, (5, 7))
    assert_eq("polars columns", df.columns, ["id", "name", "score", "active", "amount_int", "rating", "city"])

    # Check values
    assert_eq("polars names", df["name"].to_list(), ["Alice", "Bob", "Charlie", "Diana", "Eve"])

    # Check nulls
    assert_true("polars rating null", df["rating"][2] is None,
                f"rating[2] = {df['rating'][2]}")
    assert_true("polars city null", df["city"][3] is None,
                f"city[3] = {df['city'][3]}")

    # Print dtypes for inspection
    print(f"  Polars dtypes: {dict(zip(df.columns, df.dtypes))}")

    # read_qvd_to_polars convenience
    df2 = qvd.read_qvd_to_polars(qvd_path)
    assert_true("read_qvd_to_polars is DataFrame", isinstance(df2, pl.DataFrame))
    assert_eq("read_qvd_to_polars shape", df2.shape, (5, 7))


# ── Test 6: DuckDB via Arrow ─────────────────────────────────────────

def test_duckdb(qvd_path: str):
    print("\n== Test 6: DuckDB via Arrow ==")
    import duckdb
    import qvd

    # Single table query
    data = qvd.read_qvd_to_arrow(qvd_path)
    result = duckdb.sql("SELECT COUNT(*) as cnt FROM data").fetchone()
    assert_eq("duckdb count", result[0], 5)

    # Filter
    result = duckdb.sql("SELECT name FROM data WHERE score > 90 ORDER BY name").fetchall()
    names = [r[0] for r in result]
    assert_eq("duckdb filter", names, ["Alice", "Charlie", "Eve"])

    # Aggregation
    result = duckdb.sql("SELECT city, COUNT(*) as cnt FROM data WHERE city IS NOT NULL GROUP BY city ORDER BY city").fetchall()
    assert_eq("duckdb group by", result, [("London", 2), ("Moscow", 2)])

    # NULL handling
    result = duckdb.sql("SELECT COUNT(*) as cnt FROM data WHERE rating IS NULL").fetchone()
    assert_eq("duckdb null count", result[0], 2)

    # JOIN two QVD-sourced tables
    orders = qvd.read_qvd_to_arrow(qvd_path)  # reuse as "orders"
    customers = qvd.read_qvd_to_arrow(qvd_path)  # reuse as "customers"
    result = duckdb.sql("""
        SELECT o.name, c.city
        FROM orders o
        JOIN customers c ON o.id = c.id
        WHERE c.city IS NOT NULL
        ORDER BY o.name
    """).fetchall()
    assert_eq("duckdb join rows", len(result), 4)

    # Numeric operations
    result = duckdb.sql("SELECT SUM(amount_int) as total FROM data").fetchone()
    assert_true("duckdb sum", result[0] is not None, f"sum = {result[0]}")


# ── Test 7: ExistsIndex ──────────────────────────────────────────────

def test_exists_index(qvd_path: str):
    print("\n== Test 7: ExistsIndex ==")
    import qvd

    table = qvd.read_qvd(qvd_path)

    idx = qvd.ExistsIndex(table, "name")
    assert_eq("exists len", len(idx), 5)
    assert_true("Alice in idx", "Alice" in idx)
    assert_true("Bob in idx", idx.exists("Bob"))
    assert_true("Unknown not in idx", "Unknown" not in idx)

    # exists_many
    results = idx.exists_many(["Alice", "Unknown", "Eve"])
    assert_eq("exists_many", results, [True, False, True])

    # filter_exists
    city_idx = qvd.ExistsIndex(table, "city")
    matching = qvd.filter_exists(table, "city", city_idx)
    assert_true("filter_exists returns rows", len(matching) > 0,
                f"matching: {matching}")

    # Symbols
    symbols = table.symbols("name")
    assert_eq("symbols count", len(symbols), 5)
    assert_true("Alice in symbols", "Alice" in symbols)

    num_sym = table.num_symbols("name")
    assert_eq("num_symbols", num_sym, 5)


# ── Test 8: Parquet conversion ───────────────────────────────────────

def test_parquet(qvd_path: str):
    print("\n== Test 8: Parquet ↔ QVD ==")
    import qvd

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        parquet_path = f.name

    # QVD → Parquet
    qvd.convert_qvd_to_parquet(qvd_path, parquet_path, compression="snappy")
    assert_true("parquet created", os.path.exists(parquet_path))
    assert_true("parquet not empty", os.path.getsize(parquet_path) > 0)

    # Parquet → QVD
    with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as f:
        qvd_path2 = f.name

    qvd.convert_parquet_to_qvd(parquet_path, qvd_path2)
    table = qvd.read_qvd(qvd_path2)
    assert_eq("parquet roundtrip rows", table.num_rows, 5)
    assert_eq("parquet roundtrip cols", table.num_cols, 7)

    # from_parquet
    table2 = qvd.QvdTable.from_parquet(parquet_path)
    assert_eq("from_parquet rows", table2.num_rows, 5)

    # save_as_parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        parquet_path2 = f.name
    table2.save_as_parquet(parquet_path2, compression="zstd")
    assert_true("save_as_parquet created", os.path.getsize(parquet_path2) > 0)

    os.unlink(parquet_path)
    os.unlink(parquet_path2)
    os.unlink(qvd_path2)


# ── Test 9: Data types edge cases ────────────────────────────────────

def test_data_types(qvd_path: str):
    print("\n== Test 9: Data type edge cases ==")
    import pyarrow as pa
    import qvd

    # Test with explicit types
    batch = pa.RecordBatch.from_pydict({
        "int32_col":   pa.array([1, 2, 3], type=pa.int32()),
        "int64_col":   pa.array([100000000000, -1, 0], type=pa.int64()),
        "float32_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
        "float64_col": pa.array([1.123456789012, 2.0, -3.14159], type=pa.float64()),
        "string_col":  pa.array(["hello", "", "world"], type=pa.utf8()),
        "nullable_int": pa.array([1, None, 3], type=pa.int64()),
        "nullable_str": pa.array(["a", None, "c"], type=pa.utf8()),
        "all_null":    pa.array([None, None, None], type=pa.utf8()),
    })

    table = qvd.QvdTable.from_arrow(batch, table_name="types_test")

    with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as f:
        tmp = f.name

    table.save(tmp)
    table2 = qvd.read_qvd(tmp)
    batch2 = table2.to_arrow()

    # Verify values survive roundtrip
    assert_eq("int64 values", batch2.column("int64_col").to_pylist(), [100000000000, -1, 0])
    assert_eq("string values", batch2.column("string_col").to_pylist(), ["hello", "", "world"])
    assert_true("nullable_int null preserved", batch2.column("nullable_int").to_pylist()[1] is None)
    assert_true("nullable_str null preserved", batch2.column("nullable_str").to_pylist()[1] is None)
    assert_true("all_null preserved", all(x is None for x in batch2.column("all_null").to_pylist()))

    # Float precision (may lose some precision through QVD symbol storage)
    float_vals = batch2.column("float64_col").to_pylist()
    assert_close("float64 precision", float_vals[0], 1.123456789012, tol=1e-6)

    # Empty string vs null
    str_vals = batch2.column("string_col").to_pylist()
    assert_eq("empty string preserved", str_vals[1], "")

    os.unlink(tmp)


# ── main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("qvdrs Python integration tests")
    print("=" * 60)

    # Check imports
    try:
        import qvd
        print(f"  qvd module loaded")
    except ImportError as e:
        print(f"  FATAL: Cannot import qvd: {e}")
        print("  Install with: pip install qvdrs")
        sys.exit(1)

    imports = {}
    for mod in ["pyarrow", "pandas", "polars", "duckdb"]:
        try:
            imports[mod] = __import__(mod)
            print(f"  {mod} v{imports[mod].__version__}")
        except ImportError:
            print(f"  {mod}: NOT INSTALLED (tests skipped)")

    # Create test QVD
    with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as f:
        qvd_path = f.name

    if "pyarrow" not in imports:
        print("\nFATAL: pyarrow is required for all tests")
        sys.exit(1)

    original_batch = create_test_qvd(qvd_path)

    # Run tests
    test_basic_read_write(qvd_path)
    test_pyarrow(qvd_path, original_batch)
    test_arrow_roundtrip(qvd_path)

    if "pandas" in imports:
        test_pandas(qvd_path)
    else:
        print("\n== Test 4: pandas (SKIPPED — not installed) ==")

    if "polars" in imports:
        test_polars(qvd_path)
    else:
        print("\n== Test 5: Polars (SKIPPED — not installed) ==")

    if "duckdb" in imports:
        test_duckdb(qvd_path)
    else:
        print("\n== Test 6: DuckDB (SKIPPED — not installed) ==")

    test_exists_index(qvd_path)
    test_parquet(qvd_path)
    test_data_types(qvd_path)

    # Cleanup
    os.unlink(qvd_path)

    # Summary
    print("\n" + "=" * 60)
    total = PASSED + FAILED
    print(f"Results: {PASSED}/{total} passed, {FAILED} failed")
    if FAILED > 0:
        print("SOME TESTS FAILED!")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
