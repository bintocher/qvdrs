use std::io::{BufRead, Read, Seek, SeekFrom};

use crate::error::{QvdError, QvdResult};
use crate::header::{parse_xml_header, QvdTableHeader};
use crate::index::read_field_index;
use crate::symbol::read_symbols;
use crate::value::{QvdSymbol, QvdValue};

/// A streaming QVD reader that reads rows in chunks without loading
/// the entire index table into memory at once.
///
/// Symbol tables are always loaded fully (they are needed for decoding),
/// but the index table is read chunk by chunk.
pub struct QvdStreamReader<R: Read + Seek + BufRead> {
    reader: R,
    pub header: QvdTableHeader,
    pub symbols: Vec<Vec<QvdSymbol>>,
    /// Offset in the file where the binary section starts
    binary_start: u64,
    /// Current row position (0-based)
    current_row: usize,
}

/// A chunk of decoded rows from the QVD file.
pub struct QvdChunk {
    /// Column-major data: columns[col_idx][row_idx_within_chunk]
    pub columns: Vec<Vec<QvdValue>>,
    /// Number of rows in this chunk
    pub num_rows: usize,
    /// Starting row index in the original file
    pub start_row: usize,
}

impl<R: Read + Seek + BufRead> QvdStreamReader<R> {
    /// Open a QVD file for streaming reading.
    pub fn open(mut reader: R) -> QvdResult<Self> {
        // 1. Read XML header
        let mut xml_bytes = Vec::new();
        reader.read_until(0, &mut xml_bytes)?;
        let binary_start = xml_bytes.len() as u64;

        if xml_bytes.last() == Some(&0) {
            xml_bytes.pop();
        }

        let xml_string = String::from_utf8(xml_bytes)
            .map_err(|e| QvdError::Format(format!("XML header is not valid UTF-8: {}", e)))?;

        let header = parse_xml_header(&xml_string)?;

        // 2. Read symbol tables (must be loaded fully)
        // We need to read the binary section up to the index table start
        let symbol_section_size = header.offset;
        let mut symbol_buf = vec![0u8; symbol_section_size];
        reader.seek(SeekFrom::Start(binary_start))?;
        reader.read_exact(&mut symbol_buf)?;

        let mut symbols = Vec::with_capacity(header.fields.len());
        for field in &header.fields {
            let field_symbols = read_symbols(&symbol_buf, field)?;
            symbols.push(field_symbols);
        }

        Ok(QvdStreamReader {
            reader,
            header,
            symbols,
            binary_start,
            current_row: 0,
        })
    }

    /// Total number of rows in the file.
    pub fn total_rows(&self) -> usize {
        self.header.no_of_records
    }

    /// Number of rows remaining to read.
    pub fn remaining_rows(&self) -> usize {
        self.header.no_of_records.saturating_sub(self.current_row)
    }

    /// Read the next chunk of rows. Returns None when all rows have been read.
    pub fn next_chunk(&mut self, chunk_size: usize) -> QvdResult<Option<QvdChunk>> {
        if self.current_row >= self.header.no_of_records {
            return Ok(None);
        }

        let rows_to_read = chunk_size.min(self.remaining_rows());
        let record_byte_size = self.header.record_byte_size;
        let start_row = self.current_row;

        // Seek to the right position in the index table
        let index_file_offset = self.binary_start
            + self.header.offset as u64
            + (self.current_row as u64 * record_byte_size as u64);
        self.reader.seek(SeekFrom::Start(index_file_offset))?;

        // Read the chunk of index records
        let buf_size = rows_to_read * record_byte_size;
        let mut buf = vec![0u8; buf_size];
        self.reader.read_exact(&mut buf)?;

        // Decode rows
        let num_cols = self.header.fields.len();
        let mut columns: Vec<Vec<QvdValue>> = (0..num_cols)
            .map(|_| Vec::with_capacity(rows_to_read))
            .collect();

        for row in 0..rows_to_read {
            let row_start = row * record_byte_size;
            let record = &buf[row_start..row_start + record_byte_size];

            for (col_idx, field) in self.header.fields.iter().enumerate() {
                let idx = read_field_index(record, field);
                let value = if idx < 0 {
                    QvdValue::Null
                } else {
                    let sym_idx = idx as usize;
                    if sym_idx < self.symbols[col_idx].len() {
                        QvdValue::Symbol(self.symbols[col_idx][sym_idx].clone())
                    } else {
                        QvdValue::Null
                    }
                };
                columns[col_idx].push(value);
            }
        }

        self.current_row += rows_to_read;

        Ok(Some(QvdChunk {
            columns,
            num_rows: rows_to_read,
            start_row,
        }))
    }

    /// Read the next chunk as column indices (without resolving symbols).
    /// More efficient if you plan to do your own symbol resolution.
    #[allow(clippy::type_complexity)]
    pub fn next_chunk_indices(&mut self, chunk_size: usize) -> QvdResult<Option<(Vec<Vec<i64>>, usize, usize)>> {
        if self.current_row >= self.header.no_of_records {
            return Ok(None);
        }

        let rows_to_read = chunk_size.min(self.remaining_rows());
        let record_byte_size = self.header.record_byte_size;
        let start_row = self.current_row;

        let index_file_offset = self.binary_start
            + self.header.offset as u64
            + (self.current_row as u64 * record_byte_size as u64);
        self.reader.seek(SeekFrom::Start(index_file_offset))?;

        let buf_size = rows_to_read * record_byte_size;
        let mut buf = vec![0u8; buf_size];
        self.reader.read_exact(&mut buf)?;

        let num_cols = self.header.fields.len();
        let mut columns: Vec<Vec<i64>> = (0..num_cols)
            .map(|_| Vec::with_capacity(rows_to_read))
            .collect();

        for row in 0..rows_to_read {
            let row_start = row * record_byte_size;
            let record = &buf[row_start..row_start + record_byte_size];

            for (col_idx, field) in self.header.fields.iter().enumerate() {
                let idx = read_field_index(record, field);
                columns[col_idx].push(idx);
            }
        }

        self.current_row += rows_to_read;

        Ok(Some((columns, rows_to_read, start_row)))
    }

    /// Reset reader to the beginning.
    pub fn reset(&mut self) -> QvdResult<()> {
        self.current_row = 0;
        Ok(())
    }

    /// Column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.header.fields.iter().map(|f| f.field_name.as_str()).collect()
    }
}

/// Open a QVD file for streaming reading from a file path.
pub fn open_qvd_stream(path: &str) -> QvdResult<QvdStreamReader<std::io::BufReader<std::fs::File>>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    QvdStreamReader::open(reader)
}
