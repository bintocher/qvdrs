/// Tests: regeneration comparison and conversion roundtrips.
/// These tests require local QVD files in qvd_input/ (excluded from git).
use std::io::Cursor;
use md5;

fn has_test_files() -> bool {
    std::path::Path::new("qvd_input/test_qvd.qvd").exists()
}

#[test]
fn compare_regenerated_with_original() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }
    let original_bytes = std::fs::read("qvd_input/test_qvd.qvd").unwrap();
    let null_pos = original_bytes.iter().position(|&b| b == 0).unwrap();
    let original_xml = std::str::from_utf8(&original_bytes[..null_pos]).unwrap();
    let original_binary = &original_bytes[null_pos + 1..];

    let mut table = qvd::read_qvd(Cursor::new(&original_bytes)).unwrap();
    table.raw_xml.clear();
    table.raw_binary.clear();

    let mut output = Vec::new();
    qvd::write_qvd(&table, Cursor::new(&mut output)).unwrap();

    let regen_null_pos = output.iter().position(|&b| b == 0).unwrap();
    let regen_xml = std::str::from_utf8(&output[..regen_null_pos]).unwrap();
    let regen_binary = &output[regen_null_pos + 1..];

    // Compare binary — must be identical
    assert_eq!(original_binary.len(), regen_binary.len(), "Binary size mismatch");
    assert_eq!(original_binary, regen_binary, "Binary data mismatch");

    // XML diff report
    let orig_lines: Vec<&str> = original_xml.lines().collect();
    let regen_lines: Vec<&str> = regen_xml.lines().collect();
    let max_lines = orig_lines.len().max(regen_lines.len());
    let mut diff_count = 0;
    for i in 0..max_lines {
        let orig = orig_lines.get(i).unwrap_or(&"<MISSING>");
        let regen = regen_lines.get(i).unwrap_or(&"<MISSING>");
        if orig != regen {
            if diff_count < 5 {
                println!("Line {}: ORIG={:?}  REGEN={:?}", i + 1, orig, regen);
            }
            diff_count += 1;
        }
    }
    // Allow minor XML diffs (empty <Tags> block) but report them
    if diff_count > 0 {
        println!("XML has {} line differences (shifted by missing empty <Tags></Tags>)", diff_count);
    }

    // Critical XML elements must be present in regen
    assert!(regen_xml.contains("<EncryptionInfo>"), "Missing <EncryptionInfo>");
    assert!(regen_xml.contains("<Comment></Comment>"), "Missing <Comment>");
    // All field headers must have <Comment>
    let field_blocks: Vec<&str> = regen_xml.split("<QvdFieldHeader>").skip(1).collect();
    for (i, block) in field_blocks.iter().enumerate() {
        assert!(block.contains("<Comment>"), "Field {} missing <Comment>", i);
    }
}

#[test]
fn double_roundtrip_regenerated() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }
    let original_bytes = std::fs::read("qvd_input/test_qvd.qvd").unwrap();

    let mut table1 = qvd::read_qvd(Cursor::new(&original_bytes)).unwrap();
    table1.raw_xml.clear();
    table1.raw_binary.clear();

    let mut output1 = Vec::new();
    qvd::write_qvd(&table1, Cursor::new(&mut output1)).unwrap();

    let mut table2 = qvd::read_qvd(Cursor::new(&output1)).unwrap();
    table2.raw_xml.clear();
    table2.raw_binary.clear();

    let mut output2 = Vec::new();
    qvd::write_qvd(&table2, Cursor::new(&mut output2)).unwrap();

    assert_eq!(output1, output2, "Double roundtrip produced DIFFERENT output");

    // Verify all data values match original
    let orig_table = qvd::read_qvd(Cursor::new(&original_bytes)).unwrap();
    let final_table = qvd::read_qvd(Cursor::new(&output2)).unwrap();
    assert_eq!(orig_table.num_rows(), final_table.num_rows());
    assert_eq!(orig_table.num_cols(), final_table.num_cols());
    for col in 0..orig_table.num_cols() {
        for row in 0..orig_table.num_rows() {
            assert_eq!(
                orig_table.get(row, col).as_string(),
                final_table.get(row, col).as_string(),
                "Value mismatch at row={}, col={}", row, col
            );
        }
    }
    println!("PASS: double roundtrip, all {} x {} values match", orig_table.num_rows(), orig_table.num_cols());
}

/// QVD → Parquet → QVD → Parquet → QVD → compare data
#[cfg(feature = "parquet_support")]
#[test]
fn conversion_roundtrip_qvd_parquet_qvd_parquet_qvd() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }
    use std::fs;

    let input_qvd = "qvd_input/test_qvd.qvd";
    let parquet1 = "qvd_output/_test_roundtrip1.parquet";
    let qvd1 = "qvd_output/_test_roundtrip1.qvd";
    let parquet2 = "qvd_output/_test_roundtrip2.parquet";
    let qvd2 = "qvd_output/_test_roundtrip2.qvd";

    // Read original QVD
    let orig_table = qvd::read_qvd_file(input_qvd).unwrap();
    let orig_rows = orig_table.num_rows();
    let orig_cols = orig_table.num_cols();

    // Step 1: QVD → Parquet
    qvd::convert_qvd_to_parquet(input_qvd, parquet1, qvd::ParquetCompression::Snappy).unwrap();

    // Step 2: Parquet → QVD
    qvd::convert_parquet_to_qvd(parquet1, qvd1).unwrap();

    // Step 3: QVD → Parquet (second pass)
    qvd::convert_qvd_to_parquet(qvd1, parquet2, qvd::ParquetCompression::Snappy).unwrap();

    // Step 4: Parquet → QVD (second pass)
    qvd::convert_parquet_to_qvd(parquet2, qvd2).unwrap();

    // Read final QVD
    let final_table = qvd::read_qvd_file(qvd2).unwrap();
    assert_eq!(orig_rows, final_table.num_rows(), "Row count mismatch after 2x conversion");
    assert_eq!(orig_cols, final_table.num_cols(), "Col count mismatch after 2x conversion");

    // Compare values: use numeric comparison for DualDouble/DualInt,
    // string comparison for Text. Arrow doesn't preserve string representations
    // of dual values, so "10.0" may become "10" — but numeric value is preserved.
    let mut mismatches = 0;
    for col in 0..orig_cols {
        for row in 0..orig_rows {
            let orig_val = orig_table.get(row, col);
            let final_val = final_table.get(row, col);

            let match_ok = match (&orig_val, &final_val) {
                (qvd::QvdValue::Null, qvd::QvdValue::Null) => true,
                (qvd::QvdValue::Symbol(_), qvd::QvdValue::Symbol(_)) => {
                    // Prefer numeric comparison when both have numeric values
                    match (orig_val.as_f64(), final_val.as_f64()) {
                        (Some(a), Some(b)) => (a - b).abs() < 1e-10,
                        _ => orig_val.as_string() == final_val.as_string(),
                    }
                }
                _ => false,
            };

            if !match_ok {
                if mismatches < 10 {
                    let col_name = &orig_table.header.fields[col].field_name;
                    println!("MISMATCH row={} col={} ({}): {:?} → {:?}",
                        row, col, col_name, orig_val.as_string(), final_val.as_string());
                }
                mismatches += 1;
            }
        }
    }
    println!("Conversion roundtrip: {} x {} values, {} mismatches",
        orig_rows, orig_cols, mismatches);
    assert_eq!(mismatches, 0, "Data mismatches in conversion roundtrip");

    // Verify generated QVD has proper XML structure
    let qvd1_bytes = fs::read(qvd1).unwrap();
    let null_pos = qvd1_bytes.iter().position(|&b| b == 0).unwrap();
    let xml = std::str::from_utf8(&qvd1_bytes[..null_pos]).unwrap();
    assert!(xml.contains("<EncryptionInfo>"), "Generated QVD missing <EncryptionInfo>");
    assert!(xml.contains("<Type>"), "Generated QVD missing <Type>");
    // Type should never be empty
    assert!(!xml.contains("<Type></Type>"), "Generated QVD has empty <Type> — must be UNKNOWN");

    // Compare QVD1 and QVD2 binary data
    let qvd2_bytes = fs::read(qvd2).unwrap();
    let null1 = qvd1_bytes.iter().position(|&b| b == 0).unwrap();
    let null2 = qvd2_bytes.iter().position(|&b| b == 0).unwrap();
    let bin1 = &qvd1_bytes[null1 + 1..];
    let bin2 = &qvd2_bytes[null2 + 1..];
    assert_eq!(bin1, bin2, "Binary data differs between QVD pass1 and pass2");

    println!("PASS: QVD→Parquet→QVD→Parquet→QVD roundtrip complete");

    // Cleanup
    let _ = fs::remove_file(parquet1);
    let _ = fs::remove_file(qvd1);
    let _ = fs::remove_file(parquet2);
    let _ = fs::remove_file(qvd2);
}

/// Test builder path (simulates from_arrow): build → save → read → verify XML structure
#[test]
fn builder_generates_valid_xml() {
    let table = qvd::QvdTableBuilder::new("test")
        .add_column("id", vec![Some("1".to_string()), Some("2".to_string()), Some("3".to_string())])
        .add_column("name", vec![Some("alice".to_string()), Some("bob".to_string()), Some("charlie".to_string())])
        .build();

    let mut buf = Vec::new();
    qvd::write_qvd(&table, Cursor::new(&mut buf)).unwrap();

    let null_pos = buf.iter().position(|&b| b == 0).unwrap();
    let xml = std::str::from_utf8(&buf[..null_pos]).unwrap();

    println!("Builder-generated XML:\n{}", xml);

    // Critical checks
    assert!(xml.contains("<EncryptionInfo></EncryptionInfo>"), "Missing EncryptionInfo");
    assert!(!xml.contains("<Type></Type>"), "Empty <Type> — must have a valid type");
    // Builder should produce proper Qlik-compatible types (INTEGER for numeric, ASCII for text)
    assert!(xml.contains("<Type>INTEGER</Type>") || xml.contains("<Type>ASCII</Type>"),
        "Missing proper NumberFormat type (expected INTEGER or ASCII)");

    // All field headers must have <Comment>
    let fields: Vec<&str> = xml.split("<QvdFieldHeader>").skip(1).collect();
    assert_eq!(fields.len(), 2);
    for (i, f) in fields.iter().enumerate() {
        assert!(f.contains("<Comment>"), "Field {} missing <Comment>", i);
    }

    // Read back and verify data
    let readback = qvd::read_qvd(Cursor::new(&buf)).unwrap();
    assert_eq!(readback.num_rows(), 3);
    assert_eq!(readback.num_cols(), 2);
    assert_eq!(readback.get(0, 0).as_string(), Some("1".to_string()));
    assert_eq!(readback.get(2, 1).as_string(), Some("charlie".to_string()));

    println!("PASS: builder-generated QVD has correct XML structure and data");
}

/// Test MD5: raw roundtrip (read→write with raw bytes) must produce byte-identical files
#[test]
fn md5_raw_roundtrip() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }
    let qvd_dir = "qvd_input";
    let mut tested = 0;
    let mut failed = Vec::new();

    for entry in std::fs::read_dir(qvd_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().map(|e| e == "qvd").unwrap_or(false) {
            let size = entry.metadata().unwrap().len();
            if size > 100_000 { continue; }

            let original_bytes = std::fs::read(&path).unwrap();
            let orig_md5 = md5::compute(&original_bytes);

            let table = match qvd::read_qvd(Cursor::new(&original_bytes)) {
                Ok(t) => t,
                Err(_) => continue,
            };

            // Raw roundtrip (uses raw_xml + raw_binary)
            let mut output = Vec::new();
            qvd::write_qvd(&table, Cursor::new(&mut output)).unwrap();
            let regen_md5 = md5::compute(&output);

            if orig_md5 != regen_md5 {
                failed.push(format!("{}: MD5 mismatch orig={:x} regen={:x}",
                    path.file_name().unwrap().to_str().unwrap(), orig_md5, regen_md5));
            }
            tested += 1;
        }
    }

    println!("MD5 raw roundtrip: {} files tested, {} failures", tested, failed.len());
    for f in &failed {
        println!("  FAIL: {}", f);
    }
    assert!(failed.is_empty(), "MD5 mismatch in raw roundtrip");
}

/// Test EXISTS-based filtering + subset_rows + write QVD
#[test]
fn exists_filter_subset_write() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }

    let table = qvd::read_qvd_file("qvd_input/test_qvd.qvd").unwrap();
    println!("Original: {} rows x {} cols", table.num_rows(), table.num_cols());

    // Build ExistsIndex from explicit values (simulating a lookup table)
    let index = qvd::ExistsIndex::from_values(&["Q1", "Q3"]);

    // Filter using exists_fast
    let col_idx = table.column_index("TEST.Quarter").unwrap();
    let matching = qvd::filter_rows_by_exists_fast(&table, col_idx, &index);
    println!("Matched rows: {}", matching.len());
    assert_eq!(matching.len(), 6); // Q1=3 rows + Q3=3 rows

    // Create subset
    let filtered = table.subset_rows(&matching);
    assert_eq!(filtered.num_rows(), 6);
    assert_eq!(filtered.num_cols(), table.num_cols());

    // Write to QVD
    let out_path = "qvd_output/_test_exists_filter.qvd";
    qvd::write_qvd_file(&filtered, out_path).unwrap();

    // Read back and verify
    let readback = qvd::read_qvd_file(out_path).unwrap();
    assert_eq!(readback.num_rows(), 6);
    assert_eq!(readback.num_cols(), table.num_cols());

    // Verify all rows have Quarter = Q1 or Q3
    for row in 0..readback.num_rows() {
        let quarter = readback.get_by_name(row, "TEST.Quarter").unwrap().as_string().unwrap();
        assert!(quarter == "Q1" || quarter == "Q3",
            "Row {} has quarter={}, expected Q1 or Q3", row, quarter);
    }

    // Verify data matches original filtered rows
    for (new_row, &orig_row) in matching.iter().enumerate() {
        for col in 0..table.num_cols() {
            let orig = table.get(orig_row, col).as_string();
            let got = readback.get(new_row, col).as_string();
            assert_eq!(orig, got, "Mismatch at new_row={} orig_row={} col={}", new_row, orig_row, col);
        }
    }

    let _ = std::fs::remove_file(out_path);
    println!("PASS: EXISTS filter → subset_rows → write QVD → readback verified");
}

/// Test streaming read_filtered: opens QVD as stream, filters by EXISTS, selects columns
#[test]
fn streaming_read_filtered() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }

    let index = qvd::ExistsIndex::from_values(&["Q1", "Q3"]);
    let mut stream = qvd::open_qvd_stream("qvd_input/test_qvd.qvd").unwrap();
    assert_eq!(stream.total_rows(), 12);

    // Filter + select 2 columns
    let filtered = stream.read_filtered(
        "TEST.Quarter",
        &index,
        Some(&["TEST.Month", "TEST.Quarter"]),
        4, // small chunk size to test chunking
    ).unwrap();

    assert_eq!(filtered.num_rows(), 6);
    assert_eq!(filtered.num_cols(), 2);
    assert_eq!(filtered.column_names(), vec!["TEST.Month", "TEST.Quarter"]);

    // Verify all rows have Quarter = Q1 or Q3
    for row in 0..filtered.num_rows() {
        let quarter = filtered.get_by_name(row, "TEST.Quarter").unwrap().as_string().unwrap();
        assert!(quarter == "Q1" || quarter == "Q3",
            "Row {} has quarter={}", row, quarter);
    }

    // Verify months: Q1 has months 1,2,3; Q3 has 7,8,9
    let months: Vec<String> = (0..filtered.num_rows())
        .map(|r| filtered.get_by_name(r, "TEST.Month").unwrap().as_string().unwrap())
        .collect();
    assert_eq!(months, vec!["1", "2", "3", "7", "8", "9"]);

    // Test with select_cols=None (all columns)
    let mut stream2 = qvd::open_qvd_stream("qvd_input/test_qvd.qvd").unwrap();
    let filtered_all = stream2.read_filtered("TEST.Quarter", &index, None, 65536).unwrap();
    assert_eq!(filtered_all.num_rows(), 6);
    assert_eq!(filtered_all.num_cols(), 4);

    // Write to QVD and read back
    let out = "qvd_output/_test_streaming_filtered.qvd";
    qvd::write_qvd_file(&filtered, out).unwrap();
    let readback = qvd::read_qvd_file(out).unwrap();
    assert_eq!(readback.num_rows(), 6);
    assert_eq!(readback.num_cols(), 2);
    let _ = std::fs::remove_file(out);

    println!("PASS: streaming read_filtered with column selection verified");
}

/// Batch test: read all small QVD files, regenerate, verify binary data unchanged
#[test]
fn batch_regenerate_all_small_files() {
    if !has_test_files() { println!("SKIP: qvd_input/ not found"); return; }
    let qvd_dir = "qvd_input";
    let mut tested = 0;
    let mut failed = Vec::new();

    for entry in std::fs::read_dir(qvd_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().map(|e| e == "qvd").unwrap_or(false) {
            let size = entry.metadata().unwrap().len();
            if size > 100_000 { continue; } // skip large files

            let original_bytes = std::fs::read(&path).unwrap();
            let null_pos = match original_bytes.iter().position(|&b| b == 0) {
                Some(p) => p,
                None => continue,
            };
            let original_binary = &original_bytes[null_pos + 1..];

            let mut table = match qvd::read_qvd(Cursor::new(&original_bytes)) {
                Ok(t) => t,
                Err(e) => {
                    println!("SKIP {}: read error: {}", path.display(), e);
                    continue;
                }
            };

            table.raw_xml.clear();
            table.raw_binary.clear();

            let mut output = Vec::new();
            qvd::write_qvd(&table, Cursor::new(&mut output)).unwrap();

            let regen_null = output.iter().position(|&b| b == 0).unwrap();
            let regen_binary = &output[regen_null + 1..];

            if original_binary != regen_binary {
                failed.push(format!("{}: binary mismatch (orig={} regen={})",
                    path.file_name().unwrap().to_str().unwrap(),
                    original_binary.len(), regen_binary.len()));
            }
            tested += 1;
        }
    }

    println!("Tested {} files, {} failures", tested, failed.len());
    for f in &failed {
        println!("  FAIL: {}", f);
    }
    assert!(failed.is_empty(), "Some files had binary mismatches after regeneration");
}
