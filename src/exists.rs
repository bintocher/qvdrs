use std::collections::HashSet;

use crate::reader::QvdTable;

/// A fast lookup structure analogous to Qlik's EXISTS() function.
///
/// Internally uses a HashSet for O(1) lookups against the symbol table
/// of a specific field. This mirrors how Qlik Sense internally uses
/// hash-based symbol tables for the EXISTS() function.
pub struct ExistsIndex {
    values: HashSet<String>,
}

impl ExistsIndex {
    /// Build an EXISTS index from a QVD table for the given column name.
    /// This indexes all unique values (symbols) of the field.
    pub fn from_column(table: &QvdTable, col_name: &str) -> Option<Self> {
        let col_idx = table.header.fields.iter().position(|f| f.field_name == col_name)?;
        let mut values = HashSet::with_capacity(table.symbols[col_idx].len());
        for symbol in &table.symbols[col_idx] {
            values.insert(symbol.to_string_repr());
        }
        Some(ExistsIndex { values })
    }

    /// Build an EXISTS index from a column index.
    pub fn from_column_index(table: &QvdTable, col_idx: usize) -> Self {
        let mut values = HashSet::with_capacity(table.symbols[col_idx].len());
        for symbol in &table.symbols[col_idx] {
            values.insert(symbol.to_string_repr());
        }
        ExistsIndex { values }
    }

    /// Check if a value exists in the field's symbol table. O(1) lookup.
    pub fn exists(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    /// Number of unique values in the index.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Filter rows from a QVD table where a field's value exists in the given ExistsIndex.
/// Returns the row indices that match.
pub fn filter_rows_by_exists(
    table: &QvdTable,
    col_name: &str,
    index: &ExistsIndex,
) -> Vec<usize> {
    let col_idx = match table.header.fields.iter().position(|f| f.field_name == col_name) {
        Some(idx) => idx,
        None => return Vec::new(),
    };

    let mut matching_rows = Vec::new();
    for row in 0..table.num_rows() {
        let sym_idx = table.row_indices[col_idx][row];
        if sym_idx < 0 {
            continue; // NULL never matches
        }
        let sym_idx = sym_idx as usize;
        if sym_idx < table.symbols[col_idx].len() {
            let val = table.symbols[col_idx][sym_idx].to_string_repr();
            if index.exists(&val) {
                matching_rows.push(row);
            }
        }
    }
    matching_rows
}

/// A more efficient EXISTS check that works at the symbol level.
/// Pre-computes which symbol indices match, then scans row indices.
pub fn filter_rows_by_exists_fast(
    table: &QvdTable,
    col_idx: usize,
    index: &ExistsIndex,
) -> Vec<usize> {
    // Pre-compute which symbol indices exist in the lookup
    let symbol_matches: Vec<bool> = table.symbols[col_idx]
        .iter()
        .map(|sym| index.exists(&sym.to_string_repr()))
        .collect();

    let mut matching_rows = Vec::with_capacity(table.num_rows() / 4);
    for row in 0..table.num_rows() {
        let sym_idx = table.row_indices[col_idx][row];
        if sym_idx >= 0 {
            let sym_idx = sym_idx as usize;
            if sym_idx < symbol_matches.len() && symbol_matches[sym_idx] {
                matching_rows.push(row);
            }
        }
    }
    matching_rows
}
