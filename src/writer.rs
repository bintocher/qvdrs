use std::io::Write;

use crate::error::QvdResult;
use crate::header::write_xml_header;
use crate::index::write_row_record;
use crate::reader::QvdTable;
use crate::symbol::write_symbols;

/// Write a QvdTable to a writer, producing a byte-identical QVD file.
///
/// If the table was read from a file (has raw_xml and raw_binary),
/// writes back the original raw bytes for perfect roundtrip fidelity.
pub fn write_qvd<W: Write>(table: &QvdTable, mut writer: W) -> QvdResult<()> {
    if !table.raw_xml.is_empty() {
        // Byte-identical roundtrip: write original raw bytes
        writer.write_all(&table.raw_xml)?;
        writer.write_all(&table.raw_binary)?;
        writer.flush()?;
        return Ok(());
    }

    // Generate from parsed data
    write_qvd_generated(table, writer)
}

/// Write a QvdTable by regenerating all bytes from parsed data.
pub fn write_qvd_generated<W: Write>(table: &QvdTable, mut writer: W) -> QvdResult<()> {
    // 1. Rebuild symbol table binaries
    let mut symbol_bufs: Vec<Vec<u8>> = Vec::with_capacity(table.symbols.len());
    for col_symbols in &table.symbols {
        symbol_bufs.push(write_symbols(col_symbols));
    }

    // 2. Compute field offsets and lengths
    let mut header = table.header.clone();
    let mut current_offset = 0usize;
    for (i, field) in header.fields.iter_mut().enumerate() {
        field.offset = current_offset;
        field.length = symbol_bufs[i].len();
        field.no_of_symbols = table.symbols[i].len();
        current_offset += field.length;
    }

    // 3. Build index table
    let record_byte_size = header.record_byte_size;
    let mut index_buf = Vec::with_capacity(header.no_of_records * record_byte_size);

    for row_idx in 0..header.no_of_records {
        let raw_indices: Vec<u64> = header
            .fields
            .iter()
            .enumerate()
            .map(|(col_idx, field)| {
                let signed_idx = table.row_indices[col_idx][row_idx];
                (signed_idx - field.bias as i64) as u64
            })
            .collect();

        let record = write_row_record(&header.fields, &raw_indices, record_byte_size);
        index_buf.extend_from_slice(&record);
    }

    // 4. Update header offsets
    header.offset = current_offset;
    header.length = index_buf.len();

    // 5. Write XML header
    let xml = write_xml_header(&header);
    writer.write_all(xml.as_bytes())?;
    writer.write_all(&[0])?;

    // 6. Write symbol tables
    for sym_buf in &symbol_bufs {
        writer.write_all(sym_buf)?;
    }

    // 7. Write index table
    writer.write_all(&index_buf)?;

    writer.flush()?;
    Ok(())
}

/// Write a QvdTable to a file path.
pub fn write_qvd_file(table: &QvdTable, path: &str) -> QvdResult<()> {
    let file = std::fs::File::create(path)?;
    let writer = std::io::BufWriter::new(file);
    write_qvd(table, writer)
}

/// Build a new QvdTable from scratch.
pub struct QvdTableBuilder {
    pub table_name: String,
    pub columns: Vec<ColumnData>,
}

pub struct ColumnData {
    pub name: String,
    pub values: Vec<Option<String>>,
}

impl QvdTableBuilder {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(mut self, name: &str, values: Vec<Option<String>>) -> Self {
        self.columns.push(ColumnData {
            name: name.to_string(),
            values,
        });
        self
    }

    pub fn build(self) -> QvdTable {
        use crate::header::*;
        use crate::index::bits_needed;
        use crate::value::QvdSymbol;
        use std::collections::HashMap;

        let num_rows = self.columns.first().map(|c| c.values.len()).unwrap_or(0);
        let mut fields = Vec::new();
        let mut all_symbols = Vec::new();
        let mut all_indices = Vec::new();

        for col in &self.columns {
            let mut symbol_map: HashMap<String, usize> = HashMap::new();
            let mut symbols: Vec<QvdSymbol> = Vec::new();
            let mut indices: Vec<i64> = Vec::with_capacity(num_rows);
            let has_null = col.values.iter().any(|v| v.is_none());

            for val in &col.values {
                match val {
                    None => {
                        indices.push(-2); // temporary placeholder
                    }
                    Some(s) => {
                        let sym_idx = if let Some(&idx) = symbol_map.get(s) {
                            idx
                        } else {
                            let idx = symbols.len();
                            symbol_map.insert(s.clone(), idx);
                            if let Ok(v) = s.parse::<i32>() {
                                symbols.push(QvdSymbol::Int(v));
                            } else if let Ok(v) = s.parse::<f64>() {
                                if v.fract() == 0.0 && !v.is_nan() && !v.is_infinite()
                                    && v >= i32::MIN as f64 && v <= i32::MAX as f64
                                {
                                    symbols.push(QvdSymbol::Int(v as i32));
                                } else {
                                    symbols.push(QvdSymbol::Double(v));
                                }
                            } else {
                                symbols.push(QvdSymbol::Text(s.clone()));
                            }
                            idx
                        };
                        indices.push(sym_idx as i64);
                    }
                }
            }

            let num_symbols = symbols.len();

            // Remap NULL indices from -2 to num_symbols (Qlik convention)
            if has_null {
                for idx in &mut indices {
                    if *idx == -2 {
                        *idx = num_symbols as i64;
                    }
                }
            }

            let bias = 0i32;
            let bit_width = if num_symbols <= 1 { 0 } else { bits_needed(num_symbols + 1) };

            // Determine tags based on symbol types
            let all_numeric = !symbols.is_empty() && symbols.iter().all(|s| matches!(s, QvdSymbol::Int(_) | QvdSymbol::Double(_)));
            let all_int = !symbols.is_empty() && symbols.iter().all(|s| matches!(s, QvdSymbol::Int(_)));
            let tags = if all_int {
                vec!["$numeric".to_string(), "$integer".to_string()]
            } else if all_numeric {
                vec!["$numeric".to_string()]
            } else {
                let all_ascii = symbols.iter().all(|s| s.to_string_repr().bytes().all(|b| b.is_ascii()));
                if all_ascii {
                    vec!["$ascii".to_string(), "$text".to_string()]
                } else {
                    vec!["$text".to_string()]
                }
            };

            // Determine NumberFormat based on symbol types
            let number_format = if all_int {
                NumberFormat {
                    format_type: "INTEGER".to_string(),
                    n_dec: 0,
                    use_thou: 1,
                    fmt: "###0".to_string(),
                    dec: ",".to_string(),
                    thou: String::new(),
                }
            } else if all_numeric {
                NumberFormat {
                    format_type: "REAL".to_string(),
                    n_dec: 14,
                    use_thou: 1,
                    fmt: "##############".to_string(),
                    dec: ",".to_string(),
                    thou: String::new(),
                }
            } else {
                NumberFormat {
                    format_type: "ASCII".to_string(),
                    ..NumberFormat::default()
                }
            };

            fields.push(QvdFieldHeader {
                field_name: col.name.clone(),
                bit_offset: 0, // assigned below after sorting
                bit_width,
                bias,
                number_format,
                no_of_symbols: num_symbols,
                offset: 0,
                length: 0,
                comment: String::new(),
                tags,
            });

            all_symbols.push(symbols);
            all_indices.push(indices);
        }

        // Assign bit_offsets sorted by descending bit_width (Qlik convention)
        let mut sortable: Vec<(usize, usize)> = fields.iter().enumerate()
            .filter(|(_, f)| f.bit_width > 0)
            .map(|(i, f)| (i, f.bit_width))
            .collect();
        sortable.sort_by(|a, b| b.1.cmp(&a.1));

        let mut current_bit_offset = 0usize;
        for (idx, _) in &sortable {
            fields[*idx].bit_offset = current_bit_offset;
            current_bit_offset += fields[*idx].bit_width;
        }
        let total_bits = current_bit_offset;
        let record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };

        let header = QvdTableHeader {
            qv_build_no: "50699".to_string(),
            creator_doc: format!("qvdrs v{}", env!("CARGO_PKG_VERSION")),
            create_utc_time: String::new(),
            source_create_utc_time: String::new(),
            source_file_utc_time: String::new(),
            source_file_size: "-1".to_string(),
            stale_utc_time: String::new(),
            table_name: self.table_name,
            fields,
            compression: String::new(),
            record_byte_size,
            no_of_records: num_rows,
            offset: 0,
            length: 0,
            lineage: Vec::new(),
            comment: String::new(),
        };

        QvdTable {
            header,
            symbols: all_symbols,
            row_indices: all_indices,
            raw_xml: Vec::new(),
            raw_binary: Vec::new(),
        }
    }
}
