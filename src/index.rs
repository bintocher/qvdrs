use crate::header::QvdFieldHeader;

/// Extract the symbol index for a given field from a single row record.
///
/// The record bytes are in little-endian order (first byte = least significant).
/// We need to treat the record as a bit array and extract bits at
/// `bit_offset..bit_offset+bit_width`, then add `bias`.
pub fn read_field_index(record: &[u8], field: &QvdFieldHeader) -> i64 {
    if field.bit_width == 0 {
        return field.bias as i64;
    }

    let bit_offset = field.bit_offset;
    let bit_width = field.bit_width;

    // Extract bits from little-endian byte array.
    // Byte 0 contains bits 0-7, byte 1 contains bits 8-15, etc.
    let mut value: u64 = 0;
    for bit_pos in 0..bit_width {
        let abs_bit = bit_offset + bit_pos;
        let byte_idx = abs_bit / 8;
        let bit_idx = abs_bit % 8;
        if byte_idx < record.len() && (record[byte_idx] >> bit_idx) & 1 == 1 {
            value |= 1u64 << bit_pos;
        }
    }

    value as i64 + field.bias as i64
}

/// Decode all rows for all fields from the index table.
/// Returns a Vec of columns, where each column is a Vec of symbol indices (or negative for NULL).
pub fn read_all_row_indices(
    buf: &[u8],
    fields: &[QvdFieldHeader],
    record_byte_size: usize,
    no_of_records: usize,
) -> Vec<Vec<i64>> {
    let mut columns: Vec<Vec<i64>> = fields.iter().map(|_| Vec::with_capacity(no_of_records)).collect();

    for row_idx in 0..no_of_records {
        let row_start = row_idx * record_byte_size;
        let row_end = row_start + record_byte_size;
        if row_end > buf.len() {
            break;
        }
        let record = &buf[row_start..row_end];

        for (col_idx, field) in fields.iter().enumerate() {
            let index = read_field_index(record, field);
            columns[col_idx].push(index);
        }
    }

    columns
}

/// Encode a single row record from column indices.
/// Returns a byte vector of `record_byte_size` length.
pub fn write_row_record(
    fields: &[QvdFieldHeader],
    indices: &[u64],
    record_byte_size: usize,
) -> Vec<u8> {
    let mut record = vec![0u8; record_byte_size];

    for (field, &raw_index) in fields.iter().zip(indices.iter()) {
        if field.bit_width == 0 {
            continue;
        }

        let bit_offset = field.bit_offset;
        let bit_width = field.bit_width;

        for bit_pos in 0..bit_width {
            if (raw_index >> bit_pos) & 1 == 1 {
                let abs_bit = bit_offset + bit_pos;
                let byte_idx = abs_bit / 8;
                let bit_idx = abs_bit % 8;
                if byte_idx < record.len() {
                    record[byte_idx] |= 1u8 << bit_idx;
                }
            }
        }
    }

    record
}

/// Calculate the minimum number of bits needed to represent `n` distinct values.
pub fn bits_needed(n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    let max_val = n - 1;
    (usize::BITS - max_val.leading_zeros()) as usize
}
