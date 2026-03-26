use std::io::{BufRead, Read, Seek, SeekFrom};

use crate::error::{QvdError, QvdResult};
use crate::header::{parse_xml_header, QvdTableHeader};
use crate::index::read_all_row_indices;
use crate::symbol::read_symbols;
use crate::value::{QvdSymbol, QvdValue};

/// A fully parsed QVD file containing metadata, symbol tables, and row indices.
///
/// Use [`read_qvd_file`] to load from disk, then access data via [`get`](QvdTable::get),
/// [`column_strings`](QvdTable::column_strings), or convert to Arrow with
/// [`qvd_to_record_batch`](crate::parquet::qvd_to_record_batch) (feature `parquet_support`).
#[derive(Debug)]
pub struct QvdTable {
    pub header: QvdTableHeader,
    /// Columns of symbol tables: symbols[col_idx][symbol_idx]
    pub symbols: Vec<Vec<QvdSymbol>>,
    /// Row indices: row_indices[col_idx][row_idx] = symbol index (or negative for NULL)
    pub row_indices: Vec<Vec<i64>>,
    /// Raw XML header bytes (for byte-identical roundtrip)
    pub raw_xml: Vec<u8>,
    /// Raw binary section bytes (for byte-identical roundtrip)
    pub raw_binary: Vec<u8>,
}

impl QvdTable {
    pub fn num_rows(&self) -> usize {
        self.header.no_of_records
    }

    pub fn num_cols(&self) -> usize {
        self.header.fields.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.header.fields.iter().map(|f| f.field_name.as_str()).collect()
    }

    pub fn get(&self, row: usize, col: usize) -> QvdValue {
        let idx = self.row_indices[col][row];
        if idx < 0 {
            QvdValue::Null
        } else {
            let sym_idx = idx as usize;
            if sym_idx < self.symbols[col].len() {
                QvdValue::Symbol(self.symbols[col][sym_idx].clone())
            } else {
                QvdValue::Null
            }
        }
    }

    pub fn get_by_name(&self, row: usize, col_name: &str) -> Option<QvdValue> {
        let col = self.header.fields.iter().position(|f| f.field_name == col_name)?;
        Some(self.get(row, col))
    }

    pub fn column_strings(&self, col: usize) -> Vec<Option<String>> {
        (0..self.num_rows())
            .map(|row| self.get(row, col).as_string())
            .collect()
    }

    /// Create a new QvdTable containing only the specified rows.
    /// Symbol tables are rebuilt to contain only referenced symbols.
    pub fn subset_rows(&self, row_indices: &[usize]) -> QvdTable {
        let num_cols = self.num_cols();
        let new_num_rows = row_indices.len();

        let mut new_symbols: Vec<Vec<QvdSymbol>> = Vec::with_capacity(num_cols);
        let mut new_row_indices: Vec<Vec<i64>> = Vec::with_capacity(num_cols);

        for col in 0..num_cols {
            let old_symbols = &self.symbols[col];
            let old_indices = &self.row_indices[col];

            // Find which old symbol indices are actually used
            let mut used: Vec<bool> = vec![false; old_symbols.len()];
            let mut has_null = false;
            for &row in row_indices {
                let idx = old_indices[row];
                if idx < 0 {
                    has_null = true;
                } else if (idx as usize) < old_symbols.len() {
                    used[idx as usize] = true;
                }
            }

            // Build compacted symbol table and old→new index mapping
            let mut old_to_new: Vec<i64> = vec![-1; old_symbols.len()];
            let mut col_symbols: Vec<QvdSymbol> = Vec::new();
            for (old_idx, &is_used) in used.iter().enumerate() {
                if is_used {
                    old_to_new[old_idx] = col_symbols.len() as i64;
                    col_symbols.push(old_symbols[old_idx].clone());
                }
            }

            // Remap row indices
            let mut col_indices: Vec<i64> = Vec::with_capacity(new_num_rows);
            for &row in row_indices {
                let idx = old_indices[row];
                if idx < 0 {
                    col_indices.push(idx);
                } else {
                    col_indices.push(old_to_new[idx as usize]);
                }
            }

            let _ = has_null; // used implicitly via negative indices
            new_symbols.push(col_symbols);
            new_row_indices.push(col_indices);
        }

        // Rebuild header with updated counts
        let mut header = self.header.clone();
        header.no_of_records = new_num_rows;
        for (col, field) in header.fields.iter_mut().enumerate() {
            field.no_of_symbols = new_symbols[col].len();
        }
        // Recalculate bit widths
        let mut bit_offset = 0;
        for (col, field) in header.fields.iter_mut().enumerate() {
            let has_null = new_row_indices[col].iter().any(|&i| i < 0);
            field.bias = if has_null { -2 } else { 0 };
            let total_needed = if has_null {
                new_symbols[col].len() + 2
            } else {
                new_symbols[col].len()
            };
            field.bit_width = crate::index::bits_needed(total_needed);
            field.bit_offset = bit_offset;
            bit_offset += field.bit_width;
        }
        let total_bits = bit_offset;
        header.record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };
        header.offset = 0;
        header.length = 0;

        QvdTable {
            header,
            symbols: new_symbols,
            row_indices: new_row_indices,
            raw_xml: Vec::new(),
            raw_binary: Vec::new(),
        }
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.header.fields.iter().position(|f| f.field_name == name)
    }

    /// Filter rows where the specified column's value matches any of the given string values.
    /// Returns row indices of matching rows.
    pub fn filter_by_values(&self, col_name: &str, values: &[&str]) -> Vec<usize> {
        let col_idx = match self.column_index(col_name) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        // Pre-compute which symbol indices match any of the filter values
        let value_set: std::collections::HashSet<&str> = values.iter().copied().collect();
        let symbol_matches: Vec<bool> = self.symbols[col_idx]
            .iter()
            .map(|sym| value_set.contains(sym.to_string_repr().as_str()))
            .collect();

        let mut matching_rows = Vec::new();
        for row in 0..self.num_rows() {
            let sym_idx = self.row_indices[col_idx][row];
            if sym_idx >= 0 {
                let sym_idx = sym_idx as usize;
                if sym_idx < symbol_matches.len() && symbol_matches[sym_idx] {
                    matching_rows.push(row);
                }
            }
        }
        matching_rows
    }
}

/// Read and parse a QVD file from any `Read + Seek + BufRead` source.
pub fn read_qvd<R: Read + Seek + BufRead>(mut reader: R) -> QvdResult<QvdTable> {
    // 1. Read XML header (everything up to the null byte)
    let mut xml_bytes = Vec::new();
    reader.read_until(0, &mut xml_bytes)?;

    // The raw_xml includes everything up to and including the null byte
    let raw_xml = xml_bytes.clone();

    // Remove trailing null byte for parsing
    if xml_bytes.last() == Some(&0) {
        xml_bytes.pop();
    }

    let xml_string = String::from_utf8(xml_bytes)
        .map_err(|e| QvdError::Format(format!("XML header is not valid UTF-8: {}", e)))?;

    // 2. Parse XML header
    let header = parse_xml_header(&xml_string)?;

    // 3. Read binary section
    reader.seek(SeekFrom::Start(raw_xml.len() as u64))?;
    let mut raw_binary = Vec::new();
    reader.read_to_end(&mut raw_binary)?;

    // 4. Parse symbol tables
    let mut symbols = Vec::with_capacity(header.fields.len());
    for field in &header.fields {
        let field_symbols = read_symbols(&raw_binary, field)?;
        symbols.push(field_symbols);
    }

    // 5. Parse index table
    let index_start = header.offset;
    if index_start > raw_binary.len() {
        return Err(QvdError::Format(format!(
            "Index table offset {} exceeds binary section size {}",
            index_start, raw_binary.len()
        )));
    }
    let index_buf = &raw_binary[index_start..];
    let row_indices = read_all_row_indices(
        index_buf,
        &header.fields,
        header.record_byte_size,
        header.no_of_records,
    );

    Ok(QvdTable {
        header,
        symbols,
        row_indices,
        raw_xml,
        raw_binary,
    })
}

/// Read a QVD file from a file path. This is the main entry point for reading QVD files.
pub fn read_qvd_file(path: &str) -> QvdResult<QvdTable> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    read_qvd(reader)
}
