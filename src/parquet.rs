use std::collections::HashMap;
use std::path::Path;

use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit, Int8Type, Int16Type, Int32Type, Int64Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use chrono;

use crate::error::{QvdError, QvdResult};
use crate::header::*;
use crate::index::bits_needed;
use crate::reader::QvdTable;
use crate::value::QvdSymbol;

/// Qlik epoch: 1899-12-30 (day 0)
const QLIK_EPOCH_OFFSET: i64 = 693594; // days from 0001-01-01 to 1899-12-30
const UNIX_EPOCH_DAYS: i64 = 719163;   // days from 0001-01-01 to 1970-01-01
/// Days between Qlik epoch and Unix epoch
const UNIX_TO_QLIK_DAYS: i64 = UNIX_EPOCH_DAYS - QLIK_EPOCH_OFFSET; // 25569

/// Intermediate result of processing one column across all batches.
struct ColumnResult {
    symbols: Vec<QvdSymbol>,
    indices: Vec<i64>,
    has_null: bool,
}

/// Read a Parquet file and convert it to a QvdTable.
pub fn read_parquet_to_qvd(path: &str) -> QvdResult<QvdTable> {
    let file = std::fs::File::open(path)
        .map_err(QvdError::Io)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| QvdError::Format(format!("Failed to open parquet: {}", e)))?;

    let reader = builder.build()
        .map_err(|e| QvdError::Format(format!("Failed to build parquet reader: {}", e)))?;

    let schema = reader.schema().clone();

    // Collect all batches
    let mut all_batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| QvdError::Format(format!("Failed to read parquet batch: {}", e)))?;
        all_batches.push(batch);
    }

    let num_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    let num_cols = schema.fields().len();

    // Derive table name from file stem
    let table_name = Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("table")
        .to_string();

    // Build symbol tables and indices for each column
    let mut all_symbols: Vec<Vec<QvdSymbol>> = Vec::with_capacity(num_cols);
    let mut all_indices: Vec<Vec<i64>> = Vec::with_capacity(num_cols);
    let mut fields: Vec<QvdFieldHeader> = Vec::with_capacity(num_cols);
    let mut bit_offset = 0usize;

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        let col_name = field.name().clone();
        let data_type = field.data_type().clone();

        // Collect all arrays for this column
        let arrays: Vec<&dyn Array> = all_batches.iter()
            .map(|b| b.column(col_idx).as_ref())
            .collect();

        let col_result = process_column_fast(&arrays, &data_type, num_rows);

        let bias: i32 = if col_result.has_null { -2 } else { 0 };
        let num_symbols = col_result.symbols.len();
        let total_needed = if col_result.has_null { num_symbols + 2 } else { num_symbols };
        let bit_width = bits_needed(total_needed);
        let tags = compute_tags(&data_type, &col_result.symbols);
        let number_format = compute_number_format(&data_type);

        fields.push(QvdFieldHeader {
            field_name: col_name,
            bit_offset,
            bit_width,
            bias,
            number_format,
            no_of_symbols: num_symbols,
            offset: 0,
            length: 0,
            comment: String::new(),
            tags,
        });

        all_symbols.push(col_result.symbols);
        all_indices.push(col_result.indices);
        bit_offset += bit_width;
    }

    let total_bits = bit_offset;
    let record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };

    let header = QvdTableHeader {
        qv_build_no: "0".to_string(),
        creator_doc: String::new(),
        create_utc_time: chrono_now_utc(),
        source_create_utc_time: String::new(),
        source_file_utc_time: String::new(),
        source_file_size: "-1".to_string(),
        stale_utc_time: String::new(),
        table_name,
        fields,
        compression: String::new(),
        record_byte_size,
        no_of_records: num_rows,
        offset: 0,
        length: 0,
        lineage: Vec::new(),
        comment: String::new(),
    };

    Ok(QvdTable {
        header,
        symbols: all_symbols,
        row_indices: all_indices,
        raw_xml: Vec::new(),
        raw_binary: Vec::new(),
    })
}

/// Convert a Parquet file directly to a QVD file.
pub fn convert_parquet_to_qvd(parquet_path: &str, qvd_path: &str) -> QvdResult<()> {
    let table = read_parquet_to_qvd(parquet_path)?;
    crate::writer::write_qvd_file(&table, qvd_path)
}

// ============================================================
// Fast column processing — type-specialized, no per-row dispatch
// ============================================================

/// Process an entire column across all batches using type-specialized fast paths.
fn process_column_fast(arrays: &[&dyn Array], data_type: &DataType, total_rows: usize) -> ColumnResult {
    match data_type {
        // Fast path: dictionary-encoded string columns (most common in Parquet)
        DataType::Dictionary(_, value_type) if matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) => {
            process_dict_string_column(arrays, total_rows)
        }
        DataType::Utf8 => process_string_column::<i32>(arrays, total_rows),
        DataType::LargeUtf8 => process_string_column::<i64>(arrays, total_rows),
        DataType::Int8 => process_int_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<Int8Array>().unwrap().value(i) as i32),
        DataType::Int16 => process_int_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<Int16Array>().unwrap().value(i) as i32),
        DataType::Int32 => process_int_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<Int32Array>().unwrap().value(i)),
        DataType::Int64 => process_int64_column(arrays, total_rows),
        DataType::UInt8 => process_int_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<UInt8Array>().unwrap().value(i) as i32),
        DataType::UInt16 => process_int_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<UInt16Array>().unwrap().value(i) as i32),
        DataType::UInt32 => process_uint32_column(arrays, total_rows),
        DataType::UInt64 => process_uint64_column(arrays, total_rows),
        DataType::Float32 => process_float_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<Float32Array>().unwrap().value(i) as f64),
        DataType::Float64 => process_float_column(arrays, total_rows, |a, i| a.as_any().downcast_ref::<Float64Array>().unwrap().value(i)),
        DataType::Boolean => process_boolean_column(arrays, total_rows),
        _ => process_generic_column(arrays, data_type, total_rows),
    }
}

/// Ultra-fast path for dictionary-encoded string columns.
/// Parquet dictionary = unique values + indices. This maps directly to QVD symbol table + row indices.
/// We map each batch's dict values to a global symbol table, then remap indices — no per-row hashing.
fn process_dict_string_column(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    let mut global_map: HashMap<String, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        // Try each dictionary key type
        if try_process_dict::<Int32Type>(array, &mut global_map, &mut symbols, &mut indices, &mut has_null).is_some() {
            continue;
        }
        if try_process_dict::<Int16Type>(array, &mut global_map, &mut symbols, &mut indices, &mut has_null).is_some() {
            continue;
        }
        if try_process_dict::<Int8Type>(array, &mut global_map, &mut symbols, &mut indices, &mut has_null).is_some() {
            continue;
        }
        if try_process_dict::<Int64Type>(array, &mut global_map, &mut symbols, &mut indices, &mut has_null).is_some() {
            continue;
        }
        // Fallback: process row-by-row
        for row in 0..array.len() {
            if array.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let s = array_value_to_string(array, row);
                let sym_idx = *global_map.entry(s.clone()).or_insert_with(|| {
                    let idx = symbols.len();
                    symbols.push(QvdSymbol::Text(s));
                    idx
                });
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

/// Try to process a DictionaryArray with a specific key type.
/// Returns Some(true) if successfully processed, None if wrong type.
fn try_process_dict<K: arrow::datatypes::ArrowDictionaryKeyType>(
    array: &dyn Array,
    global_map: &mut HashMap<String, usize>,
    symbols: &mut Vec<QvdSymbol>,
    indices: &mut Vec<i64>,
    has_null: &mut bool,
) -> Option<bool>
where
    K::Native: TryInto<usize>,
{
    let dict = array.as_any().downcast_ref::<DictionaryArray<K>>()?;
    let values = dict.values();
    let string_values = values.as_any().downcast_ref::<StringArray>();

    if let Some(sv) = string_values {
        // Build local-to-global index mapping for this batch's dictionary
        let mut local_to_global: Vec<usize> = Vec::with_capacity(sv.len());
        for i in 0..sv.len() {
            if sv.is_null(i) {
                local_to_global.push(usize::MAX); // sentinel
            } else {
                let s = sv.value(i);
                let global_idx = if let Some(&idx) = global_map.get(s) {
                    idx
                } else {
                    let idx = symbols.len();
                    global_map.insert(s.to_string(), idx);
                    symbols.push(QvdSymbol::Text(s.to_string()));
                    idx
                };
                local_to_global.push(global_idx);
            }
        }

        // Now map each row's local dict index to the global symbol index
        let keys = dict.keys();
        for row in 0..dict.len() {
            if dict.is_null(row) {
                *has_null = true;
                indices.push(-2);
            } else {
                let local_idx: usize = keys.value(row).try_into().unwrap_or(0);
                let global_idx = local_to_global[local_idx];
                if global_idx == usize::MAX {
                    *has_null = true;
                    indices.push(-2);
                } else {
                    indices.push(global_idx as i64);
                }
            }
        }
    } else {
        // Non-string dictionary values — fall back to generic
        for row in 0..dict.len() {
            if dict.is_null(row) {
                *has_null = true;
                indices.push(-2);
            } else {
                let s = array_value_to_string(array, row);
                let sym_idx = *global_map.entry(s.clone()).or_insert_with(|| {
                    let idx = symbols.len();
                    symbols.push(QvdSymbol::Text(s));
                    idx
                });
                indices.push(sym_idx as i64);
            }
        }
    }

    Some(true)
}

/// Fast path for plain string columns — downcast once, iterate values.
fn process_string_column<O: OffsetSizeTrait>(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    let mut map: HashMap<String, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        let arr = array.as_any().downcast_ref::<GenericStringArray<O>>().unwrap();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let s = arr.value(row);
                let sym_idx = if let Some(&idx) = map.get(s) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(s.to_string(), idx);
                    symbols.push(QvdSymbol::Text(s.to_string()));
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

/// Fast path for i32-fitting integer columns — use i32 as hash key instead of String.
fn process_int_column<F>(arrays: &[&dyn Array], total_rows: usize, get_value: F) -> ColumnResult
where
    F: Fn(&dyn Array, usize) -> i32,
{
    let mut map: HashMap<i32, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        for row in 0..array.len() {
            if array.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let v = get_value(array, row);
                let sym_idx = if let Some(&idx) = map.get(&v) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(v, idx);
                    symbols.push(QvdSymbol::DualInt(v, v.to_string()));
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

/// Fast path for i64 columns.
fn process_int64_column(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    let mut map: HashMap<i64, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let v = arr.value(row);
                let sym_idx = if let Some(&idx) = map.get(&v) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(v, idx);
                    if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                        symbols.push(QvdSymbol::DualInt(v as i32, v.to_string()));
                    } else {
                        symbols.push(QvdSymbol::DualDouble(v as f64, v.to_string()));
                    }
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

fn process_uint32_column(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    let mut map: HashMap<u32, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let v = arr.value(row);
                let sym_idx = if let Some(&idx) = map.get(&v) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(v, idx);
                    if v <= i32::MAX as u32 {
                        symbols.push(QvdSymbol::DualInt(v as i32, v.to_string()));
                    } else {
                        symbols.push(QvdSymbol::DualDouble(v as f64, v.to_string()));
                    }
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

fn process_uint64_column(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    let mut map: HashMap<u64, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let v = arr.value(row);
                let sym_idx = if let Some(&idx) = map.get(&v) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(v, idx);
                    if v <= i32::MAX as u64 {
                        symbols.push(QvdSymbol::DualInt(v as i32, v.to_string()));
                    } else {
                        symbols.push(QvdSymbol::DualDouble(v as f64, v.to_string()));
                    }
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

/// Fast path for float columns — use i64 bits as hash key (avoids String alloc).
fn process_float_column<F>(arrays: &[&dyn Array], total_rows: usize, get_value: F) -> ColumnResult
where
    F: Fn(&dyn Array, usize) -> f64,
{
    let mut map: HashMap<u64, usize> = HashMap::new(); // f64 bits as key
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        for row in 0..array.len() {
            if array.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else {
                let v = get_value(array, row);
                let bits = v.to_bits();
                let sym_idx = if let Some(&idx) = map.get(&bits) {
                    idx
                } else {
                    let idx = symbols.len();
                    map.insert(bits, idx);
                    symbols.push(QvdSymbol::DualDouble(v, v.to_string()));
                    idx
                };
                indices.push(sym_idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

fn process_boolean_column(arrays: &[&dyn Array], total_rows: usize) -> ColumnResult {
    // At most 2 unique values (0, 1), plus possibly NULL
    let mut sym_false: Option<usize> = None;
    let mut sym_true: Option<usize> = None;
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                has_null = true;
                indices.push(-2);
            } else if arr.value(row) {
                let idx = *sym_true.get_or_insert_with(|| {
                    let i = symbols.len();
                    symbols.push(QvdSymbol::DualInt(1, "1".to_string()));
                    i
                });
                indices.push(idx as i64);
            } else {
                let idx = *sym_false.get_or_insert_with(|| {
                    let i = symbols.len();
                    symbols.push(QvdSymbol::DualInt(0, "0".to_string()));
                    i
                });
                indices.push(idx as i64);
            }
        }
    }

    ColumnResult { symbols, indices, has_null }
}

/// Generic fallback — still uses String-based dedup but with per-row extract_symbol.
fn process_generic_column(arrays: &[&dyn Array], data_type: &DataType, total_rows: usize) -> ColumnResult {
    let mut map: HashMap<String, usize> = HashMap::new();
    let mut symbols: Vec<QvdSymbol> = Vec::new();
    let mut indices: Vec<i64> = Vec::with_capacity(total_rows);
    let mut has_null = false;

    for &array in arrays {
        for row in 0..array.len() {
            if array.is_null(row) {
                has_null = true;
                indices.push(-2);
                continue;
            }

            let (sym, key) = extract_symbol(array, data_type, row);
            let sym_idx = if let Some(&idx) = map.get(&key) {
                idx
            } else {
                let idx = symbols.len();
                map.insert(key, idx);
                symbols.push(sym);
                idx
            };
            indices.push(sym_idx as i64);
        }
    }

    ColumnResult { symbols, indices, has_null }
}

fn extract_symbol(array: &dyn Array, data_type: &DataType, row: usize) -> (QvdSymbol, String) {
    match data_type {
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let unix_days = arr.value(row) as i64;
            let qlik_days = unix_days + UNIX_TO_QLIK_DAYS;
            let date_str = format_date_from_unix_days(unix_days);
            let s = date_str.clone();
            (QvdSymbol::DualDouble(qlik_days as f64, date_str), s)
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let unix_days = ms / 86_400_000;
            let qlik_days = unix_days + UNIX_TO_QLIK_DAYS;
            let date_str = format_date_from_unix_days(unix_days);
            let s = date_str.clone();
            (QvdSymbol::DualDouble(qlik_days as f64, date_str), s)
        }
        DataType::Timestamp(unit, _tz) => {
            let ts_value = match unit {
                TimeUnit::Second => {
                    array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(row) as f64
                }
                TimeUnit::Millisecond => {
                    array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row) as f64 / 1_000.0
                }
                TimeUnit::Microsecond => {
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row) as f64 / 1_000_000.0
                }
                TimeUnit::Nanosecond => {
                    array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row) as f64 / 1_000_000_000.0
                }
            };
            let qlik_serial = ts_value / 86_400.0 + UNIX_TO_QLIK_DAYS as f64;
            let ts_str = format_timestamp_from_unix_secs(ts_value);
            let s = ts_str.clone();
            (QvdSymbol::DualDouble(qlik_serial, ts_str), s)
        }
        DataType::Binary | DataType::LargeBinary => {
            let s = "<binary>".to_string();
            (QvdSymbol::Text(s.clone()), s)
        }
        _ => {
            let s = array_value_to_string(array, row);
            (QvdSymbol::Text(s.clone()), s)
        }
    }
}

fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    use arrow::util::display::ArrayFormatter;
    let fmt = ArrayFormatter::try_new(array, &Default::default());
    match fmt {
        Ok(f) => f.value(row).to_string(),
        Err(_) => "<error>".to_string(),
    }
}

fn format_date_from_unix_days(unix_days: i64) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    match epoch.checked_add_signed(chrono::Duration::days(unix_days)) {
        Some(date) => date.format("%Y-%m-%d").to_string(),
        None => unix_days.to_string(),
    }
}

fn format_timestamp_from_unix_secs(secs: f64) -> String {
    let total_secs = secs as i64;
    let nanos = ((secs - total_secs as f64) * 1_000_000_000.0) as u32;
    let dt = chrono::DateTime::from_timestamp(total_secs, nanos);
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => secs.to_string(),
    }
}

fn chrono_now_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn compute_tags(data_type: &DataType, _symbols: &[QvdSymbol]) -> Vec<String> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            vec!["$numeric".to_string(), "$integer".to_string()]
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            vec!["$numeric".to_string()]
        }
        DataType::Boolean => {
            vec!["$numeric".to_string(), "$integer".to_string()]
        }
        DataType::Date32 | DataType::Date64 => {
            vec!["$numeric".to_string(), "$timestamp".to_string()]
        }
        DataType::Timestamp(_, _) => {
            vec!["$numeric".to_string(), "$timestamp".to_string()]
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => {
            vec!["$text".to_string()]
        }
        _ => Vec::new(),
    }
}

fn compute_number_format(data_type: &DataType) -> NumberFormat {
    match data_type {
        DataType::Date32 | DataType::Date64 => NumberFormat {
            format_type: "1".to_string(),
            ..Default::default()
        },
        DataType::Timestamp(_, _) => NumberFormat {
            format_type: "3".to_string(),
            ..Default::default()
        },
        _ => NumberFormat::default(),
    }
}

// ============================================================
// QVD → Arrow RecordBatch
// ============================================================

use arrow::datatypes::{Schema, Field};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn infer_arrow_type(field: &QvdFieldHeader, symbols: &[QvdSymbol]) -> DataType {
    let fmt_type = field.number_format.format_type.as_str();
    let has_tag = |t: &str| field.tags.iter().any(|tag| tag == t);

    match fmt_type {
        "1" => return DataType::Date32,
        "2" => return DataType::Utf8,
        "3" => return DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => {}
    }

    if has_tag("$timestamp") {
        let has_fractional = symbols.iter().any(|s| match s {
            QvdSymbol::Double(v) | QvdSymbol::DualDouble(v, _) => v.fract() != 0.0,
            _ => false,
        });
        return if has_fractional {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        } else {
            DataType::Date32
        };
    }

    if has_tag("$integer") && has_tag("$numeric") {
        let all_int = symbols.iter().all(|s| matches!(s, QvdSymbol::Int(_) | QvdSymbol::DualInt(_, _)));
        if all_int { return DataType::Int64; }
    }

    if has_tag("$numeric") { return DataType::Float64; }
    if has_tag("$text") { return DataType::Utf8; }
    if symbols.is_empty() { return DataType::Utf8; }

    let all_int = symbols.iter().all(|s| matches!(s, QvdSymbol::Int(_) | QvdSymbol::DualInt(_, _)));
    if all_int { return DataType::Int64; }

    let all_numeric = symbols.iter().all(|s| matches!(s,
        QvdSymbol::Int(_) | QvdSymbol::Double(_) | QvdSymbol::DualInt(_, _) | QvdSymbol::DualDouble(_, _)
    ));
    if all_numeric { return DataType::Float64; }

    DataType::Utf8
}

/// Convert a QvdTable to an Arrow RecordBatch.
#[allow(clippy::needless_range_loop)]
pub fn qvd_to_record_batch(table: &QvdTable) -> QvdResult<RecordBatch> {
    let num_rows = table.num_rows();
    let num_cols = table.num_cols();

    let mut fields = Vec::with_capacity(num_cols);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let qvd_field = &table.header.fields[col_idx];
        let symbols = &table.symbols[col_idx];
        let indices = &table.row_indices[col_idx];
        let has_null = indices.iter().any(|&i| i < 0);
        let arrow_type = infer_arrow_type(qvd_field, symbols);

        fields.push(Field::new(&qvd_field.field_name, arrow_type.clone(), has_null));

        let array: Arc<dyn Array> = match &arrow_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in 0..num_rows {
                    let idx = indices[row];
                    if idx < 0 {
                        builder.append_null();
                    } else {
                        let sym = &symbols[idx as usize];
                        match sym {
                            QvdSymbol::Int(v) | QvdSymbol::DualInt(v, _) => builder.append_value(*v as i64),
                            QvdSymbol::Double(v) | QvdSymbol::DualDouble(v, _) => builder.append_value(*v as i64),
                            QvdSymbol::Text(s) => {
                                if let Ok(v) = s.parse::<i64>() {
                                    builder.append_value(v);
                                } else {
                                    builder.append_null();
                                }
                            }
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in 0..num_rows {
                    let idx = indices[row];
                    if idx < 0 {
                        builder.append_null();
                    } else {
                        let sym = &symbols[idx as usize];
                        match sym.as_f64() {
                            Some(v) => builder.append_value(v),
                            None => {
                                if let QvdSymbol::Text(s) = sym {
                                    if let Ok(v) = s.parse::<f64>() {
                                        builder.append_value(v);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(num_rows);
                for row in 0..num_rows {
                    let idx = indices[row];
                    if idx < 0 {
                        builder.append_null();
                    } else {
                        let sym = &symbols[idx as usize];
                        let qlik_days = match sym {
                            QvdSymbol::Int(v) | QvdSymbol::DualInt(v, _) => *v as f64,
                            QvdSymbol::Double(v) | QvdSymbol::DualDouble(v, _) => *v,
                            QvdSymbol::Text(s) => {
                                if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                    let epoch = chrono::NaiveDate::from_ymd_opt(1899, 12, 30).unwrap();
                                    (d - epoch).num_days() as f64
                                } else {
                                    builder.append_null();
                                    continue;
                                }
                            }
                        };
                        let unix_days = qlik_days as i32 - UNIX_TO_QLIK_DAYS as i32;
                        builder.append_value(unix_days);
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                for row in 0..num_rows {
                    let idx = indices[row];
                    if idx < 0 {
                        builder.append_null();
                    } else {
                        let sym = &symbols[idx as usize];
                        let qlik_serial = match sym {
                            QvdSymbol::Int(v) | QvdSymbol::DualInt(v, _) => *v as f64,
                            QvdSymbol::Double(v) | QvdSymbol::DualDouble(v, _) => *v,
                            QvdSymbol::Text(_) => {
                                builder.append_null();
                                continue;
                            }
                        };
                        let unix_secs = (qlik_serial - UNIX_TO_QLIK_DAYS as f64) * 86_400.0;
                        let micros = (unix_secs * 1_000_000.0) as i64;
                        builder.append_value(micros);
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                for row in 0..num_rows {
                    let idx = indices[row];
                    if idx < 0 {
                        builder.append_null();
                    } else {
                        let sym = &symbols[idx as usize];
                        builder.append_value(sym.to_string_repr());
                    }
                }
                Arc::new(builder.finish())
            }
        };

        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| QvdError::Format(format!("Failed to create RecordBatch: {}", e)))
}

// ============================================================
// QVD → Parquet file
// ============================================================

use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

impl ParquetCompression {
    pub fn parse(s: &str) -> QvdResult<Self> {
        match s.to_lowercase().as_str() {
            "none" | "uncompressed" => Ok(Self::None),
            "snappy" => Ok(Self::Snappy),
            "gzip" | "gz" => Ok(Self::Gzip),
            "lz4" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            _ => Err(QvdError::Format(format!("Unknown compression: '{}'. Use: none, snappy, gzip, lz4, zstd", s))),
        }
    }

    fn to_parquet_compression(self) -> Compression {
        match self {
            Self::None => Compression::UNCOMPRESSED,
            Self::Snappy => Compression::SNAPPY,
            Self::Gzip => Compression::GZIP(Default::default()),
            Self::Lz4 => Compression::LZ4,
            Self::Zstd => Compression::ZSTD(Default::default()),
        }
    }
}

pub fn convert_qvd_to_parquet(
    qvd_path: &str,
    parquet_path: &str,
    compression: ParquetCompression,
) -> QvdResult<()> {
    let table = crate::reader::read_qvd_file(qvd_path)?;
    write_qvd_table_to_parquet(&table, parquet_path, compression)
}

pub fn write_qvd_table_to_parquet(
    table: &QvdTable,
    parquet_path: &str,
    compression: ParquetCompression,
) -> QvdResult<()> {
    let batch = qvd_to_record_batch(table)?;

    let props = WriterProperties::builder()
        .set_compression(compression.to_parquet_compression())
        .build();

    let file = std::fs::File::create(parquet_path)
        .map_err(QvdError::Io)?;

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| QvdError::Format(format!("Failed to create parquet writer: {}", e)))?;

    writer.write(&batch)
        .map_err(|e| QvdError::Format(format!("Failed to write parquet batch: {}", e)))?;

    writer.close()
        .map_err(|e| QvdError::Format(format!("Failed to close parquet writer: {}", e)))?;

    Ok(())
}

// ============================================================
// Arrow RecordBatch → QVD
// ============================================================

pub fn record_batch_to_qvd(batch: &RecordBatch, table_name: &str) -> QvdResult<QvdTable> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_cols = schema.fields().len();

    let mut all_symbols: Vec<Vec<QvdSymbol>> = Vec::with_capacity(num_cols);
    let mut all_indices: Vec<Vec<i64>> = Vec::with_capacity(num_cols);
    let mut fields: Vec<QvdFieldHeader> = Vec::with_capacity(num_cols);
    let mut bit_offset = 0usize;

    for col_idx in 0..num_cols {
        let arrow_field = &schema.fields()[col_idx];
        let col_name = arrow_field.name().clone();
        let data_type = arrow_field.data_type().clone();
        let array = batch.column(col_idx);

        let col_result = process_column_fast(&[array.as_ref()], &data_type, num_rows);

        let bias: i32 = if col_result.has_null { -2 } else { 0 };
        let num_symbols = col_result.symbols.len();
        let total_needed = if col_result.has_null { num_symbols + 2 } else { num_symbols };
        let bit_width = bits_needed(total_needed);
        let tags = compute_tags(&data_type, &col_result.symbols);
        let number_format = compute_number_format(&data_type);

        fields.push(QvdFieldHeader {
            field_name: col_name,
            bit_offset,
            bit_width,
            bias,
            number_format,
            no_of_symbols: num_symbols,
            offset: 0,
            length: 0,
            comment: String::new(),
            tags,
        });

        all_symbols.push(col_result.symbols);
        all_indices.push(col_result.indices);
        bit_offset += bit_width;
    }

    let total_bits = bit_offset;
    let record_byte_size = if total_bits == 0 { 0 } else { total_bits.div_ceil(8) };

    let header = QvdTableHeader {
        qv_build_no: "0".to_string(),
        creator_doc: String::new(),
        create_utc_time: chrono_now_utc(),
        source_create_utc_time: String::new(),
        source_file_utc_time: String::new(),
        source_file_size: "-1".to_string(),
        stale_utc_time: String::new(),
        table_name: table_name.to_string(),
        fields,
        compression: String::new(),
        record_byte_size,
        no_of_records: num_rows,
        offset: 0,
        length: 0,
        lineage: Vec::new(),
        comment: String::new(),
    };

    Ok(QvdTable {
        header,
        symbols: all_symbols,
        row_indices: all_indices,
        raw_xml: Vec::new(),
        raw_binary: Vec::new(),
    })
}

pub fn write_record_batch_to_qvd(batch: &RecordBatch, table_name: &str, qvd_path: &str) -> QvdResult<()> {
    let table = record_batch_to_qvd(batch, table_name)?;
    crate::writer::write_qvd_file(&table, qvd_path)
}

pub fn parquet_to_qvd(parquet_path: &str, qvd_path: &str) -> QvdResult<()> {
    convert_parquet_to_qvd(parquet_path, qvd_path)
}

pub fn qvd_to_parquet(qvd_path: &str, parquet_path: &str) -> QvdResult<()> {
    convert_qvd_to_parquet(qvd_path, parquet_path, ParquetCompression::Snappy)
}
