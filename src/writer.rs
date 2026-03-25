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
        let mut bit_offset = 0usize;

        for col in &self.columns {
            let mut symbol_map: HashMap<String, usize> = HashMap::new();
            let mut symbols: Vec<QvdSymbol> = Vec::new();
            let mut indices: Vec<i64> = Vec::with_capacity(num_rows);
            let has_null = col.values.iter().any(|v| v.is_none());
            let bias: i32 = if has_null { -2 } else { 0 };

            for val in &col.values {
                match val {
                    None => {
                        indices.push(-2);
                    }
                    Some(s) => {
                        let sym_idx = if let Some(&idx) = symbol_map.get(s) {
                            idx
                        } else {
                            let idx = symbols.len();
                            symbol_map.insert(s.clone(), idx);
                            if let Ok(v) = s.parse::<i32>() {
                                symbols.push(QvdSymbol::DualInt(v, s.clone()));
                            } else if let Ok(v) = s.parse::<f64>() {
                                symbols.push(QvdSymbol::DualDouble(v, s.clone()));
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
            let total_needed = if has_null { num_symbols + 2 } else { num_symbols };
            let bit_width = bits_needed(total_needed);

            fields.push(QvdFieldHeader {
                field_name: col.name.clone(),
                bit_offset,
                bit_width,
                bias,
                number_format: NumberFormat::default(),
                no_of_symbols: num_symbols,
                offset: 0,
                length: 0,
                comment: String::new(),
                tags: Vec::new(),
            });

            all_symbols.push(symbols);
            all_indices.push(indices);
            bit_offset += bit_width;
        }

        let total_bits = bit_offset;
        let record_byte_size = total_bits.div_ceil(8);

        let header = QvdTableHeader {
            qv_build_no: "0".to_string(),
            creator_doc: String::new(),
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
