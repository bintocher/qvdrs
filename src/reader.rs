use std::io::{BufRead, Read, Seek, SeekFrom};

use crate::error::{QvdError, QvdResult};
use crate::header::{parse_xml_header, QvdTableHeader};
use crate::index::read_all_row_indices;
use crate::symbol::read_symbols;
use crate::value::{QvdSymbol, QvdValue};

/// A fully parsed QVD file.
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
}

/// Read and parse a QVD file from a reader.
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

/// Read a QVD file from a file path.
pub fn read_qvd_file(path: &str) -> QvdResult<QvdTable> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    read_qvd(reader)
}
