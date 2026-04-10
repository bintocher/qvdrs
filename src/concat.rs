//! QVD table concatenation and merge operations.
//!
//! **Layer 0 — [`concatenate`]**: pure row append, no deduplication.
//! **Layer 1 — [`concatenate_with_pk`]**: append with primary-key deduplication (upsert).

use std::collections::HashMap;

use crate::error::{QvdError, QvdResult};
use crate::header::{QvdFieldHeader, QvdTableHeader};
use crate::reader::QvdTable;
use crate::value::QvdSymbol;

/// Schema compatibility mode for concatenation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaMode {
    /// Strict: both tables must have identical column names (in any order).
    /// Error if columns differ. This is the default.
    Strict,
    /// Union: columns are matched by name; missing columns are filled with NULL.
    /// Matches Qlik CONCATENATE behavior.
    Union,
}

impl Default for SchemaMode {
    fn default() -> Self {
        SchemaMode::Strict
    }
}

/// Conflict resolution strategy when a PK collision is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflict {
    /// New rows replace existing rows with the same PK (upsert).
    Replace,
    /// Existing rows are kept; new rows with colliding PK are skipped.
    Skip,
    /// Return an error if any PK collision is detected.
    Error,
}

// ============================================================
// Layer 0: pure concatenate (no PK checks)
// ============================================================

/// Concatenate two QVD tables into one (strict schema check by default).
///
/// By default (`SchemaMode::Strict`), both tables must have the same column names
/// (order may differ). Returns an error if columns don't match.
///
/// With `SchemaMode::Union`, columns are matched by name; missing columns are
/// filled with NULLs (Qlik CONCATENATE behavior).
///
/// The resulting table's symbol tables are merged: identical symbols share one slot.
/// After merge, the table is normalized (bit_width, bit_offset, tags, number_format).
pub fn concatenate(a: &QvdTable, b: &QvdTable) -> QvdResult<QvdTable> {
    concatenate_with_schema(a, b, SchemaMode::Strict)
}

/// Concatenate two QVD tables with explicit schema mode.
pub fn concatenate_with_schema(a: &QvdTable, b: &QvdTable, schema: SchemaMode) -> QvdResult<QvdTable> {
    let a_cols: Vec<&str> = a.header.fields.iter().map(|f| f.field_name.as_str()).collect();
    let b_cols: Vec<&str> = b.header.fields.iter().map(|f| f.field_name.as_str()).collect();

    if schema == SchemaMode::Strict {
        // Check that both tables have the same set of columns
        let mut a_sorted = a_cols.clone();
        let mut b_sorted = b_cols.clone();
        a_sorted.sort();
        b_sorted.sort();

        if a_sorted != b_sorted {
            let a_only: Vec<&&str> = a_cols.iter().filter(|c| !b_cols.contains(c)).collect();
            let b_only: Vec<&&str> = b_cols.iter().filter(|c| !a_cols.contains(c)).collect();
            let mut msg = "Schema mismatch (use schema='union' to allow different columns).".to_string();
            if !a_only.is_empty() {
                msg.push_str(&format!(" Only in A: {:?}.", a_only));
            }
            if !b_only.is_empty() {
                msg.push_str(&format!(" Only in B: {:?}.", b_only));
            }
            return Err(QvdError::Format(msg));
        }
    }

    // Build unified column list (preserve order: all of A's columns, then new from B)
    let mut col_names: Vec<String> = a.header.fields.iter().map(|f| f.field_name.clone()).collect();
    for f in &b.header.fields {
        if !col_names.contains(&f.field_name) {
            col_names.push(f.field_name.clone());
        }
    }

    let num_cols = col_names.len();
    let num_rows_a = a.num_rows();
    let num_rows_b = b.num_rows();
    let total_rows = num_rows_a + num_rows_b;

    let mut merged_symbols: Vec<Vec<QvdSymbol>> = Vec::with_capacity(num_cols);
    let mut merged_indices: Vec<Vec<i64>> = Vec::with_capacity(num_cols);
    let mut merged_fields: Vec<QvdFieldHeader> = Vec::with_capacity(num_cols);

    for col_name in &col_names {
        let a_col = a.header.fields.iter().position(|f| &f.field_name == col_name);
        let b_col = b.header.fields.iter().position(|f| &f.field_name == col_name);

        let (symbols, indices) = merge_column(
            a_col.map(|i| (&a.symbols[i], &a.row_indices[i])),
            num_rows_a,
            b_col.map(|i| (&b.symbols[i], &b.row_indices[i])),
            num_rows_b,
        );

        let num_symbols = symbols.len();
        // Take field header from whichever side has it (prefer A)
        let base_field = if let Some(ai) = a_col {
            a.header.fields[ai].clone()
        } else if let Some(bi) = b_col {
            b.header.fields[bi].clone()
        } else {
            unreachable!()
        };

        merged_fields.push(QvdFieldHeader {
            field_name: col_name.clone(),
            bit_offset: 0,
            bit_width: 0,
            bias: 0,
            number_format: base_field.number_format,
            no_of_symbols: num_symbols,
            offset: 0,
            length: 0,
            comment: base_field.comment,
            tags: base_field.tags,
        });

        merged_symbols.push(symbols);
        merged_indices.push(indices);
    }

    let header = QvdTableHeader {
        qv_build_no: a.header.qv_build_no.clone(),
        creator_doc: a.header.creator_doc.clone(),
        create_utc_time: a.header.create_utc_time.clone(),
        source_create_utc_time: String::new(),
        source_file_utc_time: String::new(),
        source_file_size: "-1".to_string(),
        stale_utc_time: String::new(),
        table_name: a.header.table_name.clone(),
        fields: merged_fields,
        compression: String::new(),
        record_byte_size: 0,
        no_of_records: total_rows,
        offset: 0,
        length: 0,
        lineage: Vec::new(),
        comment: String::new(),
    };

    let mut result = QvdTable {
        header,
        symbols: merged_symbols,
        row_indices: merged_indices,
        raw_xml: Vec::new(),
        raw_binary: Vec::new(),
    };

    result.normalize();
    Ok(result)
}

// ============================================================
// Layer 1: concatenate with PK deduplication
// ============================================================

/// Concatenate two QVD tables with primary-key deduplication.
///
/// - `on_conflict = Replace`: new rows win — existing rows with colliding PK are removed.
/// - `on_conflict = Skip`: existing rows win — new rows with colliding PK are dropped.
/// - `on_conflict = Error`: any PK collision returns an error.
///
/// Schema check is strict by default. Use `concatenate_with_pk_schema` for union mode.
/// `pk_columns` must exist in both tables. Composite keys are supported (multiple column names).
/// NULL PK values cause an error.
pub fn concatenate_with_pk(
    existing: &QvdTable,
    new_rows: &QvdTable,
    pk_columns: &[&str],
    on_conflict: OnConflict,
) -> QvdResult<QvdTable> {
    concatenate_with_pk_schema(existing, new_rows, pk_columns, on_conflict, SchemaMode::Strict)
}

/// Concatenate with PK deduplication and explicit schema mode.
pub fn concatenate_with_pk_schema(
    existing: &QvdTable,
    new_rows: &QvdTable,
    pk_columns: &[&str],
    on_conflict: OnConflict,
    schema: SchemaMode,
) -> QvdResult<QvdTable> {
    // Validate PK columns exist in both tables
    for &pk in pk_columns {
        if existing.column_index(pk).is_none() {
            return Err(QvdError::Format(format!(
                "PK column '{}' not found in existing table", pk
            )));
        }
        if new_rows.column_index(pk).is_none() {
            return Err(QvdError::Format(format!(
                "PK column '{}' not found in new rows", pk
            )));
        }
    }

    // Build PK index from the "winning" side
    match on_conflict {
        OnConflict::Replace => {
            // New wins: build PK set from new_rows, filter out matching existing rows
            let new_pk_set = build_pk_set(new_rows, pk_columns)?;

            // Check for NULL PKs in new_rows (already caught by build_pk_set)
            // Check for NULL PKs in existing (we'll skip them — they pass through)
            let kept_rows = filter_rows_not_in_pk_set(existing, pk_columns, &new_pk_set)?;
            let filtered_existing = existing.subset_rows(&kept_rows);

            concatenate_with_schema(&filtered_existing, new_rows, schema)
        }
        OnConflict::Skip => {
            // Existing wins: build PK set from existing, filter out matching new rows
            let existing_pk_set = build_pk_set(existing, pk_columns)?;
            let kept_rows = filter_rows_not_in_pk_set(new_rows, pk_columns, &existing_pk_set)?;
            let filtered_new = new_rows.subset_rows(&kept_rows);

            concatenate_with_schema(existing, &filtered_new, schema)
        }
        OnConflict::Error => {
            // Build PK set from existing, check if any new rows collide
            let existing_pk_set = build_pk_set(existing, pk_columns)?;
            let _new_pk_set = build_pk_set(new_rows, pk_columns)?;

            for row in 0..new_rows.num_rows() {
                let pk_val = extract_pk_value(new_rows, pk_columns, row)?;
                if existing_pk_set.contains_key(&pk_val) {
                    return Err(QvdError::Format(format!(
                        "PK collision: value '{}' exists in both tables (row {})", pk_val, row
                    )));
                }
            }

            concatenate_with_schema(existing, new_rows, schema)
        }
    }
}

// ============================================================
// Internal helpers
// ============================================================

/// Merge one column from two tables into a single symbol table + index vector.
///
/// If a side is None (column missing in that table), those rows get NULL indices.
fn merge_column(
    a: Option<(&Vec<QvdSymbol>, &Vec<i64>)>,
    num_rows_a: usize,
    b: Option<(&Vec<QvdSymbol>, &Vec<i64>)>,
    num_rows_b: usize,
) -> (Vec<QvdSymbol>, Vec<i64>) {
    let total_rows = num_rows_a + num_rows_b;

    // Start with A's symbols as the base
    let (base_symbols, base_indices) = match a {
        Some((syms, idxs)) => (syms.clone(), idxs.clone()),
        None => (Vec::new(), Vec::new()),
    };

    let mut merged_syms = base_symbols;
    // Build lookup: string_repr → index in merged_syms
    let mut sym_map: HashMap<String, usize> = HashMap::with_capacity(merged_syms.len());
    for (i, sym) in merged_syms.iter().enumerate() {
        sym_map.insert(sym.to_string_repr(), i);
    }

    // Use -1 as temporary NULL marker; remap to num_symbols at the end
    let mut merged_idx: Vec<i64> = Vec::with_capacity(total_rows);

    // Prepare A-side indices (NULL-fill if column missing)
    match a {
        Some((a_syms, _)) => {
            for &idx in &base_indices {
                if idx < 0 || (idx as usize) >= a_syms.len() {
                    merged_idx.push(-1);
                } else {
                    merged_idx.push(idx);
                }
            }
        }
        None => {
            // Column missing in A → all rows are NULL
            merged_idx.resize(num_rows_a, -1);
        }
    }

    // Process B-side: remap symbols into merged table
    match b {
        Some((b_syms, b_idxs)) => {
            // Build B-local → merged index mapping
            let mut b_to_merged: Vec<usize> = Vec::with_capacity(b_syms.len());
            for sym in b_syms {
                let repr = sym.to_string_repr();
                let merged_idx_val = if let Some(&existing) = sym_map.get(&repr) {
                    existing
                } else {
                    let new_idx = merged_syms.len();
                    sym_map.insert(repr, new_idx);
                    merged_syms.push(sym.clone());
                    new_idx
                };
                b_to_merged.push(merged_idx_val);
            }

            for &idx in b_idxs {
                if idx < 0 || (idx as usize) >= b_syms.len() {
                    merged_idx.push(-1);
                } else {
                    merged_idx.push(b_to_merged[idx as usize] as i64);
                }
            }
        }
        None => {
            // Column missing in B → all rows are NULL
            merged_idx.resize(merged_idx.len() + num_rows_b, -1);
        }
    }

    // Remap temporary NULL marker (-1) to num_symbols (Qlik convention)
    let null_sentinel = merged_syms.len() as i64;
    for idx in &mut merged_idx {
        if *idx < 0 {
            *idx = null_sentinel;
        }
    }

    (merged_syms, merged_idx)
}

/// Build a HashMap of PK string → first row index for the given table.
/// Returns error if any PK value is NULL.
fn build_pk_set(
    table: &QvdTable,
    pk_columns: &[&str],
) -> QvdResult<HashMap<String, usize>> {
    let mut pk_set = HashMap::with_capacity(table.num_rows());
    for row in 0..table.num_rows() {
        let pk_val = extract_pk_value(table, pk_columns, row)?;
        pk_set.entry(pk_val).or_insert(row);
    }
    Ok(pk_set)
}

/// Extract composite PK value as a canonical string for a given row.
fn extract_pk_value(
    table: &QvdTable,
    pk_columns: &[&str],
    row: usize,
) -> QvdResult<String> {
    if pk_columns.len() == 1 {
        let col_idx = table.column_index(pk_columns[0]).unwrap();
        let sym_idx = table.row_indices[col_idx][row];
        if sym_idx < 0 || (sym_idx as usize) >= table.symbols[col_idx].len() {
            return Err(QvdError::Format(format!(
                "NULL value in PK column '{}' at row {}", pk_columns[0], row
            )));
        }
        Ok(table.symbols[col_idx][sym_idx as usize].to_string_repr())
    } else {
        // Composite key: join with separator that's unlikely in data
        let mut parts = Vec::with_capacity(pk_columns.len());
        for &pk in pk_columns {
            let col_idx = table.column_index(pk).unwrap();
            let sym_idx = table.row_indices[col_idx][row];
            if sym_idx < 0 || (sym_idx as usize) >= table.symbols[col_idx].len() {
                return Err(QvdError::Format(format!(
                    "NULL value in PK column '{}' at row {}", pk, row
                )));
            }
            parts.push(table.symbols[col_idx][sym_idx as usize].to_string_repr());
        }
        Ok(parts.join("\x1F")) // ASCII Unit Separator
    }
}

/// Return row indices of `table` whose PK is NOT in `pk_set`.
fn filter_rows_not_in_pk_set(
    table: &QvdTable,
    pk_columns: &[&str],
    pk_set: &HashMap<String, usize>,
) -> QvdResult<Vec<usize>> {
    let mut kept = Vec::with_capacity(table.num_rows());
    for row in 0..table.num_rows() {
        let pk_val = extract_pk_value(table, pk_columns, row);
        let should_keep = match pk_val {
            Ok(val) => !pk_set.contains_key(&val),
            Err(_) => true, // NULL PK in existing side: keep the row (can't collide)
        };
        if should_keep {
            kept.push(row);
        }
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{QvdTableHeader, QvdFieldHeader, NumberFormat};

    fn make_table(
        name: &str,
        columns: Vec<(&str, Vec<QvdSymbol>, Vec<i64>)>,
        num_rows: usize,
    ) -> QvdTable {
        let mut fields = Vec::new();
        let mut symbols = Vec::new();
        let mut row_indices = Vec::new();

        for (col_name, syms, idxs) in columns {
            fields.push(QvdFieldHeader {
                field_name: col_name.to_string(),
                bit_offset: 0,
                bit_width: 0,
                bias: 0,
                number_format: NumberFormat::default(),
                no_of_symbols: syms.len(),
                offset: 0,
                length: 0,
                comment: String::new(),
                tags: Vec::new(),
            });
            symbols.push(syms);
            row_indices.push(idxs);
        }

        let header = QvdTableHeader {
            qv_build_no: "50699".to_string(),
            creator_doc: "test".to_string(),
            create_utc_time: String::new(),
            source_create_utc_time: String::new(),
            source_file_utc_time: String::new(),
            source_file_size: "-1".to_string(),
            stale_utc_time: String::new(),
            table_name: name.to_string(),
            fields,
            compression: String::new(),
            record_byte_size: 0,
            no_of_records: num_rows,
            offset: 0,
            length: 0,
            lineage: Vec::new(),
            comment: String::new(),
        };

        let mut t = QvdTable { header, symbols, row_indices, raw_xml: Vec::new(), raw_binary: Vec::new() };
        t.normalize();
        t
    }

    #[test]
    fn test_concatenate_simple() {
        let a = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(1), QvdSymbol::Int(2)], vec![0, 1]),
            ("val", vec![QvdSymbol::Text("a".into()), QvdSymbol::Text("b".into())], vec![0, 1]),
        ], 2);

        let b = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(3)], vec![0]),
            ("val", vec![QvdSymbol::Text("c".into())], vec![0]),
        ], 1);

        let merged = concatenate(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.num_cols(), 2);
        // Check values
        assert_eq!(merged.get(0, 0).as_string().unwrap(), "1");
        assert_eq!(merged.get(1, 0).as_string().unwrap(), "2");
        assert_eq!(merged.get(2, 0).as_string().unwrap(), "3");
        assert_eq!(merged.get(2, 1).as_string().unwrap(), "c");
    }

    #[test]
    fn test_concatenate_shared_symbols() {
        // Both tables have symbol "x" — should be deduplicated
        let a = make_table("t", vec![
            ("col", vec![QvdSymbol::Text("x".into()), QvdSymbol::Text("y".into())], vec![0, 1]),
        ], 2);
        let b = make_table("t", vec![
            ("col", vec![QvdSymbol::Text("x".into()), QvdSymbol::Text("z".into())], vec![0, 1]),
        ], 2);

        let merged = concatenate(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 4);
        // "x" should appear only once in symbols
        assert_eq!(merged.symbols[0].len(), 3); // x, y, z
        assert_eq!(merged.get(0, 0).as_string().unwrap(), "x");
        assert_eq!(merged.get(2, 0).as_string().unwrap(), "x");
    }

    #[test]
    fn test_concatenate_schema_strict_rejects_mismatch() {
        let a = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(1)], vec![0]),
            ("a", vec![QvdSymbol::Text("aa".into())], vec![0]),
        ], 1);
        let b = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(2)], vec![0]),
            ("b", vec![QvdSymbol::Text("bb".into())], vec![0]),
        ], 1);

        // Strict mode (default) should reject
        let result = concatenate(&a, &b);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Schema mismatch"));
    }

    #[test]
    fn test_concatenate_schema_union() {
        // A has columns [id, a], B has columns [id, b]
        let a = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(1)], vec![0]),
            ("a", vec![QvdSymbol::Text("aa".into())], vec![0]),
        ], 1);
        let b = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(2)], vec![0]),
            ("b", vec![QvdSymbol::Text("bb".into())], vec![0]),
        ], 1);

        // Union mode should merge with NULLs
        let merged = concatenate_with_schema(&a, &b, SchemaMode::Union).unwrap();
        assert_eq!(merged.num_rows(), 2);
        assert_eq!(merged.num_cols(), 3); // id, a, b
        assert_eq!(merged.column_names(), vec!["id", "a", "b"]);
        // Row 0: id=1, a="aa", b=NULL
        assert_eq!(merged.get(0, 0).as_string().unwrap(), "1");
        assert_eq!(merged.get(0, 1).as_string().unwrap(), "aa");
        assert!(merged.get(0, 2).is_null());
        // Row 1: id=2, a=NULL, b="bb"
        assert_eq!(merged.get(1, 0).as_string().unwrap(), "2");
        assert!(merged.get(1, 1).is_null());
        assert_eq!(merged.get(1, 2).as_string().unwrap(), "bb");
    }

    #[test]
    fn test_concatenate_with_pk_replace() {
        let existing = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(1), QvdSymbol::Int(2), QvdSymbol::Int(3)], vec![0, 1, 2]),
            ("val", vec![QvdSymbol::Text("old1".into()), QvdSymbol::Text("old2".into()), QvdSymbol::Text("old3".into())], vec![0, 1, 2]),
        ], 3);

        let new_rows = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(2), QvdSymbol::Int(4)], vec![0, 1]),
            ("val", vec![QvdSymbol::Text("new2".into()), QvdSymbol::Text("new4".into())], vec![0, 1]),
        ], 2);

        let result = concatenate_with_pk(&existing, &new_rows, &["pk"], OnConflict::Replace).unwrap();
        assert_eq!(result.num_rows(), 4); // rows: 1(old), 3(old), 2(new), 4(new)

        // Collect all pk→val pairs
        let mut pairs: Vec<(String, String)> = Vec::new();
        for row in 0..result.num_rows() {
            pairs.push((
                result.get(row, 0).as_string().unwrap(),
                result.get(row, 1).as_string().unwrap(),
            ));
        }
        pairs.sort();
        assert_eq!(pairs, vec![
            ("1".into(), "old1".into()),
            ("2".into(), "new2".into()), // replaced!
            ("3".into(), "old3".into()),
            ("4".into(), "new4".into()), // new
        ]);
    }

    #[test]
    fn test_concatenate_with_pk_skip() {
        let existing = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(1), QvdSymbol::Int(2)], vec![0, 1]),
            ("val", vec![QvdSymbol::Text("old1".into()), QvdSymbol::Text("old2".into())], vec![0, 1]),
        ], 2);

        let new_rows = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(2), QvdSymbol::Int(3)], vec![0, 1]),
            ("val", vec![QvdSymbol::Text("new2".into()), QvdSymbol::Text("new3".into())], vec![0, 1]),
        ], 2);

        let result = concatenate_with_pk(&existing, &new_rows, &["pk"], OnConflict::Skip).unwrap();
        assert_eq!(result.num_rows(), 3); // 1(old), 2(old), 3(new)

        let mut pairs: Vec<(String, String)> = Vec::new();
        for row in 0..result.num_rows() {
            pairs.push((
                result.get(row, 0).as_string().unwrap(),
                result.get(row, 1).as_string().unwrap(),
            ));
        }
        pairs.sort();
        assert_eq!(pairs, vec![
            ("1".into(), "old1".into()),
            ("2".into(), "old2".into()), // kept existing!
            ("3".into(), "new3".into()),
        ]);
    }

    #[test]
    fn test_concatenate_with_pk_error_on_conflict() {
        let existing = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(1)], vec![0]),
        ], 1);
        let new_rows = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(1)], vec![0]),
        ], 1);

        let result = concatenate_with_pk(&existing, &new_rows, &["pk"], OnConflict::Error);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PK collision"));
    }

    #[test]
    fn test_concatenate_with_pk_null_error() {
        let existing = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(1)], vec![0, 1]), // row 1 → idx 1 which is >= num_symbols(1) → NULL
        ], 2);
        // build_pk_set on existing should fail because row 1 has NULL PK
        // Actually filter_rows_not_in_pk_set tolerates NULLs in existing (keeps them).
        // NULLs in new_rows are the problem:
        let new_rows = make_table("t", vec![
            ("pk", vec![QvdSymbol::Int(2)], vec![0, 1]), // row 1 → NULL
        ], 2);

        let result = concatenate_with_pk(&existing, &new_rows, &["pk"], OnConflict::Replace);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NULL value in PK"));
    }

    #[test]
    fn test_concatenate_composite_pk() {
        let existing = make_table("t", vec![
            ("a", vec![QvdSymbol::Int(1), QvdSymbol::Int(1)], vec![0, 1]),
            ("b", vec![QvdSymbol::Text("x".into()), QvdSymbol::Text("y".into())], vec![0, 1]),
            ("val", vec![QvdSymbol::Text("old".into())], vec![0, 0]),
        ], 2);

        let new_rows = make_table("t", vec![
            ("a", vec![QvdSymbol::Int(1)], vec![0]),
            ("b", vec![QvdSymbol::Text("x".into())], vec![0]),
            ("val", vec![QvdSymbol::Text("new".into())], vec![0]),
        ], 1);

        let result = concatenate_with_pk(&existing, &new_rows, &["a", "b"], OnConflict::Replace).unwrap();
        assert_eq!(result.num_rows(), 2); // (1,"y","old") kept, (1,"x","new") replaced

        let mut pairs: Vec<(String, String, String)> = Vec::new();
        for row in 0..result.num_rows() {
            pairs.push((
                result.get(row, 0).as_string().unwrap(),
                result.get(row, 1).as_string().unwrap(),
                result.get(row, 2).as_string().unwrap(),
            ));
        }
        pairs.sort();
        assert_eq!(pairs, vec![
            ("1".into(), "x".into(), "new".into()),
            ("1".into(), "y".into(), "old".into()),
        ]);
    }

    #[test]
    fn test_concatenate_empty_tables() {
        let a = make_table("t", vec![
            ("id", vec![QvdSymbol::Int(1)], vec![0]),
        ], 1);
        let b = make_table("t", vec![
            ("id", Vec::<QvdSymbol>::new(), Vec::<i64>::new()),
        ], 0);

        let merged = concatenate(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 1);

        let merged2 = concatenate(&b, &a).unwrap();
        assert_eq!(merged2.num_rows(), 1);
    }
}
