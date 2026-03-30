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

    /// Read filtered: stream the entire file, decode only the filter column per row,
    /// and collect indices only for matching rows and selected columns.
    ///
    /// - `filter_col`: column name to filter on
    /// - `exists_index`: ExistsIndex with values to match
    /// - `select_cols`: if Some, only include these columns in the output; if None, include all
    /// - `chunk_size`: number of rows to read per I/O chunk
    ///
    /// Memory: symbol tables (small) + one chunk buffer + only matching row indices.
    /// For 87M rows with 23% match rate, this holds ~20M rows instead of 87M.
    pub fn read_filtered(
        &mut self,
        filter_col: &str,
        exists_index: &crate::exists::ExistsIndex,
        select_cols: Option<&[&str]>,
        chunk_size: usize,
    ) -> QvdResult<crate::reader::QvdTable> {
        let filter_col_idx = self.header.fields.iter()
            .position(|f| f.field_name == filter_col)
            .ok_or_else(|| QvdError::Format(format!("Column '{}' not found", filter_col)))?;

        // Determine which columns to include in output
        let output_col_indices: Vec<usize> = match select_cols {
            Some(names) => {
                let mut indices = Vec::with_capacity(names.len());
                for name in names {
                    let idx = self.header.fields.iter()
                        .position(|f| f.field_name == *name)
                        .ok_or_else(|| QvdError::Format(format!("Column '{}' not found", name)))?;
                    indices.push(idx);
                }
                // Ensure filter column is included (needed for decoding)
                indices
            }
            None => (0..self.header.fields.len()).collect(),
        };

        // Pre-compute which symbol indices in the filter column match
        let symbol_matches: Vec<bool> = self.symbols[filter_col_idx]
            .iter()
            .map(|sym| exists_index.exists(&sym.to_string_repr()))
            .collect();

        let num_output_cols = output_col_indices.len();
        let filter_field = &self.header.fields[filter_col_idx];
        let record_byte_size = self.header.record_byte_size;

        // Pre-clone the field headers we need (to avoid borrow conflict in the loop)
        let output_fields: Vec<crate::header::QvdFieldHeader> = output_col_indices.iter()
            .map(|&ci| self.header.fields[ci].clone())
            .collect();

        // Find filter column position within output columns (if present)
        let filter_in_output = output_col_indices.iter().position(|&ci| ci == filter_col_idx);

        // Accumulators for matched rows only
        let mut result_indices: Vec<Vec<i64>> = (0..num_output_cols)
            .map(|_| Vec::new())
            .collect();
        let mut total_matched: usize = 0;

        // Stream chunks
        while self.current_row < self.header.no_of_records {
            let rows_to_read = chunk_size.min(self.remaining_rows());

            let index_file_offset = self.binary_start
                + self.header.offset as u64
                + (self.current_row as u64 * record_byte_size as u64);
            self.reader.seek(SeekFrom::Start(index_file_offset))?;

            let buf_size = rows_to_read * record_byte_size;
            let mut buf = vec![0u8; buf_size];
            self.reader.read_exact(&mut buf)?;

            for row in 0..rows_to_read {
                let row_start = row * record_byte_size;
                let record = &buf[row_start..row_start + record_byte_size];

                // Decode ONLY the filter column first
                let filter_idx = read_field_index(record, filter_field);

                // Check if this row matches
                let matches = if filter_idx >= 0 {
                    let si = filter_idx as usize;
                    si < symbol_matches.len() && symbol_matches[si]
                } else {
                    false
                };

                if matches {
                    // Decode only the selected output columns
                    for (out_idx, field) in output_fields.iter().enumerate() {
                        if Some(out_idx) == filter_in_output {
                            // Already decoded
                            result_indices[out_idx].push(filter_idx);
                        } else {
                            let idx = read_field_index(record, field);
                            result_indices[out_idx].push(idx);
                        }
                    }
                    total_matched += 1;
                }
            }

            self.current_row += rows_to_read;
        }

        // Build output header with only selected columns
        let mut header = self.header.clone();
        header.fields = output_fields;
        header.no_of_records = total_matched;
        header.offset = 0;
        header.length = 0;

        // Compact symbols: remove unused ones, remap indices
        let mut new_symbols: Vec<Vec<QvdSymbol>> = Vec::with_capacity(num_output_cols);
        for (out_idx, field) in header.fields.iter_mut().enumerate() {
            let orig_col_idx = output_col_indices[out_idx];
            let old_syms = &self.symbols[orig_col_idx];
            let col_indices = &mut result_indices[out_idx];

            // Find used symbol indices
            let mut used = vec![false; old_syms.len()];
            for &idx in col_indices.iter() {
                if idx >= 0 && (idx as usize) < old_syms.len() {
                    used[idx as usize] = true;
                }
            }

            // Build compacted symbol table + remap
            let mut old_to_new: Vec<i64> = vec![-1; old_syms.len()];
            let mut compacted: Vec<QvdSymbol> = Vec::new();
            for (old_idx, &is_used) in used.iter().enumerate() {
                if is_used {
                    old_to_new[old_idx] = compacted.len() as i64;
                    compacted.push(old_syms[old_idx].clone());
                }
            }

            // Remap indices in-place; NULL → num_new_symbols (Qlik convention: bias=0)
            let num_new_symbols = compacted.len();
            for idx in col_indices.iter_mut() {
                if *idx < 0 || (*idx as usize) >= old_syms.len() {
                    *idx = num_new_symbols as i64; // NULL sentinel
                } else {
                    *idx = old_to_new[*idx as usize];
                }
            }

            field.no_of_symbols = num_new_symbols;
            field.bias = 0;
            field.bit_width = if num_new_symbols <= 1 { 0 } else { crate::index::bits_needed(num_new_symbols + 1) };

            new_symbols.push(compacted);
        }

        // Assign bit_offsets sorted by descending bit_width (Qlik convention)
        let mut sortable: Vec<(usize, usize)> = header.fields.iter().enumerate()
            .filter(|(_, f)| f.bit_width > 0)
            .map(|(i, f)| (i, f.bit_width))
            .collect();
        sortable.sort_by(|a, b| b.1.cmp(&a.1));
        let mut current_bit_offset = 0usize;
        for (idx, _) in &sortable {
            header.fields[*idx].bit_offset = current_bit_offset;
            current_bit_offset += header.fields[*idx].bit_width;
        }
        for f in &mut header.fields {
            if f.bit_width == 0 { f.bit_offset = 0; }
        }
        let total_bits = current_bit_offset;
        header.record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };

        Ok(crate::reader::QvdTable {
            header,
            symbols: new_symbols,
            row_indices: result_indices,
            raw_xml: Vec::new(),
            raw_binary: Vec::new(),
        })
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
