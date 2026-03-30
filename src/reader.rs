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
            for &row in row_indices {
                let idx = old_indices[row];
                if idx >= 0 && (idx as usize) < old_symbols.len() {
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

            // Remap row indices; NULL → num_new_symbols (Qlik convention: bias=0)
            let num_new_symbols = col_symbols.len();
            let mut col_indices: Vec<i64> = Vec::with_capacity(new_num_rows);
            for &row in row_indices {
                let idx = old_indices[row];
                if idx < 0 || (idx as usize) >= old_symbols.len() {
                    col_indices.push(num_new_symbols as i64); // NULL sentinel
                } else {
                    col_indices.push(old_to_new[idx as usize]);
                }
            }

            new_symbols.push(col_symbols);
            new_row_indices.push(col_indices);
        }

        // Rebuild header with updated counts
        let mut header = self.header.clone();
        header.no_of_records = new_num_rows;
        for (col, field) in header.fields.iter_mut().enumerate() {
            let num_symbols = new_symbols[col].len();
            field.no_of_symbols = num_symbols;
            field.bias = 0;
            field.bit_width = if num_symbols <= 1 { 0 } else { crate::index::bits_needed(num_symbols + 1) };
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

    /// Normalize the table for maximum Qlik Sense compatibility.
    ///
    /// Converts symbol types (DualInt→Int, DualDouble→Double for pure numbers,
    /// keeps DualDouble for dates/timestamps), sets proper NumberFormat,
    /// Tags, BitWidth, BitOffset ordering, and NULL representation.
    ///
    /// Call this before writing a QVD file to ensure Qlik can read it correctly.
    pub fn normalize(&mut self) {
        use crate::header::NumberFormat;

        let num_cols = self.num_cols();

        for col in 0..num_cols {
            let symbols = &mut self.symbols[col];
            let indices = &mut self.row_indices[col];
            let field = &mut self.header.fields[col];
            let num_symbols = symbols.len();

            // 1. Convert symbol types: DualInt→Int, DualDouble→Double (except dates/timestamps)
            let has_date_tag = field.tags.iter().any(|t| t == "$timestamp" || t == "$date")
                || matches!(field.number_format.format_type.as_str(), "DATE" | "TIMESTAMP" | "1" | "3");
            if !has_date_tag {
                for sym in symbols.iter_mut() {
                    *sym = match std::mem::replace(sym, QvdSymbol::Int(0)) {
                        QvdSymbol::DualInt(v, _) => QvdSymbol::Int(v),
                        QvdSymbol::DualDouble(v, _) => {
                            if v.fract() == 0.0 && !v.is_nan() && !v.is_infinite()
                                && v >= i32::MIN as f64 && v <= i32::MAX as f64
                            {
                                QvdSymbol::Int(v as i32)
                            } else {
                                QvdSymbol::Double(v)
                            }
                        }
                        other => other,
                    };
                }
            }

            // 2. Remap NULL indices: any negative or >= num_symbols → num_symbols
            for idx in indices.iter_mut() {
                if *idx < 0 || (*idx as usize) >= num_symbols {
                    *idx = num_symbols as i64;
                }
            }
            field.bias = 0;

            // 3. BitWidth: reserve NULL sentinel
            field.bit_width = if num_symbols <= 1 { 0 } else {
                crate::index::bits_needed(num_symbols + 1)
            };

            // 4. Determine NumberFormat from actual symbol types
            let all_int = !symbols.is_empty() && symbols.iter().all(|s| matches!(s, QvdSymbol::Int(_)));
            let all_numeric = !symbols.is_empty() && symbols.iter().all(|s|
                matches!(s, QvdSymbol::Int(_) | QvdSymbol::Double(_) | QvdSymbol::DualInt(_, _) | QvdSymbol::DualDouble(_, _)));

            if has_date_tag {
                // Keep existing date/timestamp format
            } else if all_int {
                field.number_format = NumberFormat {
                    format_type: "INTEGER".to_string(),
                    n_dec: 0,
                    use_thou: 1,
                    fmt: "###0".to_string(),
                    dec: ",".to_string(),
                    thou: String::new(),
                };
            } else if all_numeric {
                let has_any_double = symbols.iter().any(|s|
                    matches!(s, QvdSymbol::Double(_) | QvdSymbol::DualDouble(_, _)));
                if has_any_double {
                    field.number_format = NumberFormat {
                        format_type: "REAL".to_string(),
                        n_dec: 14,
                        use_thou: 1,
                        fmt: "##############".to_string(),
                        dec: ",".to_string(),
                        thou: String::new(),
                    };
                } else {
                    field.number_format = NumberFormat {
                        format_type: "INTEGER".to_string(),
                        n_dec: 0,
                        use_thou: 1,
                        fmt: "###0".to_string(),
                        dec: ",".to_string(),
                        thou: String::new(),
                    };
                }
            } else {
                field.number_format = NumberFormat {
                    format_type: "ASCII".to_string(),
                    ..NumberFormat::default()
                };
            }

            // 5. Tags
            if has_date_tag {
                // Keep existing date/timestamp tags
            } else if all_int {
                field.tags = vec!["$numeric".to_string(), "$integer".to_string()];
            } else if all_numeric {
                field.tags = vec!["$numeric".to_string()];
            } else {
                let all_ascii = symbols.iter().all(|s|
                    s.to_string_repr().bytes().all(|b| b.is_ascii()));
                if all_ascii {
                    field.tags = vec!["$ascii".to_string(), "$text".to_string()];
                } else {
                    field.tags = vec!["$text".to_string()];
                }
            }
        }

        // 6. BitOffset: sort by descending bit_width (Qlik convention)
        let mut sortable: Vec<(usize, usize)> = self.header.fields.iter().enumerate()
            .filter(|(_, f)| f.bit_width > 0)
            .map(|(i, f)| (i, f.bit_width))
            .collect();
        sortable.sort_by(|a, b| b.1.cmp(&a.1));

        let mut current_bit_offset = 0usize;
        for (idx, _) in &sortable {
            self.header.fields[*idx].bit_offset = current_bit_offset;
            current_bit_offset += self.header.fields[*idx].bit_width;
        }
        for f in &mut self.header.fields {
            if f.bit_width == 0 { f.bit_offset = 0; }
        }
        let total_bits = current_bit_offset;
        self.header.record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };

        // 7. Clear raw bytes to force regeneration on write
        self.raw_xml.clear();
        self.raw_binary.clear();
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
