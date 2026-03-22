use crate::error::{QvdError, QvdResult};
use crate::header::QvdFieldHeader;
use crate::value::QvdSymbol;

/// Parse symbol table for a single field from the binary buffer.
///
/// The buffer starts at the beginning of the binary section (after XML header).
/// Field's `offset` and `length` are relative to this buffer start.
pub fn read_symbols(buf: &[u8], field: &QvdFieldHeader) -> QvdResult<Vec<QvdSymbol>> {
    let start = field.offset;
    let end = start + field.length;
    if end > buf.len() {
        return Err(QvdError::Format(format!(
            "Symbol table for '{}' exceeds buffer: offset={}, length={}, buf_len={}",
            field.field_name, field.offset, field.length, buf.len()
        )));
    }

    let mut symbols = Vec::with_capacity(field.no_of_symbols);
    let mut i = start;

    while i < end && symbols.len() < field.no_of_symbols {
        let type_byte = buf[i];
        i += 1;

        match type_byte {
            0x01 => {
                // Integer: 4 bytes little-endian
                if i + 4 > end {
                    return Err(QvdError::Format(format!(
                        "Unexpected end of symbol table for '{}' at integer",
                        field.field_name
                    )));
                }
                let val = i32::from_le_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
                i += 4;
                symbols.push(QvdSymbol::Int(val));
            }
            0x02 => {
                // Double: 8 bytes little-endian
                if i + 8 > end {
                    return Err(QvdError::Format(format!(
                        "Unexpected end of symbol table for '{}' at double",
                        field.field_name
                    )));
                }
                let bytes: [u8; 8] = buf[i..i + 8].try_into().unwrap();
                let val = f64::from_le_bytes(bytes);
                i += 8;
                symbols.push(QvdSymbol::Double(val));
            }
            0x04 => {
                // String: null-terminated
                let str_start = i;
                while i < end && buf[i] != 0 {
                    i += 1;
                }
                let s = String::from_utf8(buf[str_start..i].to_vec())?;
                if i < end {
                    i += 1; // skip null terminator
                }
                symbols.push(QvdSymbol::Text(s));
            }
            0x05 => {
                // Dual Int: 4 bytes integer + null-terminated string
                if i + 4 > end {
                    return Err(QvdError::Format(format!(
                        "Unexpected end of symbol table for '{}' at dual int",
                        field.field_name
                    )));
                }
                let val = i32::from_le_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
                i += 4;
                let str_start = i;
                while i < end && buf[i] != 0 {
                    i += 1;
                }
                let s = String::from_utf8(buf[str_start..i].to_vec())?;
                if i < end {
                    i += 1; // skip null terminator
                }
                symbols.push(QvdSymbol::DualInt(val, s));
            }
            0x06 => {
                // Dual Double: 8 bytes double + null-terminated string
                if i + 8 > end {
                    return Err(QvdError::Format(format!(
                        "Unexpected end of symbol table for '{}' at dual double",
                        field.field_name
                    )));
                }
                let bytes: [u8; 8] = buf[i..i + 8].try_into().unwrap();
                let val = f64::from_le_bytes(bytes);
                i += 8;
                let str_start = i;
                while i < end && buf[i] != 0 {
                    i += 1;
                }
                let s = String::from_utf8(buf[str_start..i].to_vec())?;
                if i < end {
                    i += 1; // skip null terminator
                }
                symbols.push(QvdSymbol::DualDouble(val, s));
            }
            _ => {
                return Err(QvdError::Format(format!(
                    "Unknown symbol type 0x{:02x} in field '{}' at offset {}",
                    type_byte, field.field_name, i - 1
                )));
            }
        }
    }

    Ok(symbols)
}

/// Serialize symbols back to binary format.
pub fn write_symbols(symbols: &[QvdSymbol]) -> Vec<u8> {
    let total_size: usize = symbols.iter().map(|s| s.binary_size()).sum();
    let mut buf = Vec::with_capacity(total_size);

    for symbol in symbols {
        buf.push(symbol.type_byte());
        match symbol {
            QvdSymbol::Int(v) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            QvdSymbol::Double(v) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            QvdSymbol::Text(s) => {
                buf.extend_from_slice(s.as_bytes());
                buf.push(0);
            }
            QvdSymbol::DualInt(v, s) => {
                buf.extend_from_slice(&v.to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
                buf.push(0);
            }
            QvdSymbol::DualDouble(v, s) => {
                buf.extend_from_slice(&v.to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
                buf.push(0);
            }
        }
    }

    buf
}
