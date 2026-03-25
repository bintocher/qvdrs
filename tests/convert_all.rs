/// Convert all QVD files from qvd_input through multiple transformation paths
/// and write final results to qvd_output.
///
/// Transformations:
/// 1. QVD → regenerate → QVD
/// 2. QVD → Parquet → QVD
/// 3. QVD → Arrow RecordBatch → QVD
/// 4. QVD → Parquet → QVD → Parquet → QVD (double conversion)

#[cfg(feature = "parquet_support")]
#[test]
fn convert_all_input_to_output() {
    let input_dir = "qvd_input";
    let output_dir = "C:/work/qlik/80_coding/qvdrs/qvd_output";
    let temp_dir = "C:/work/qlik/80_coding/qvdrs/qvd_output/_temp";

    std::fs::create_dir_all(output_dir).unwrap();
    std::fs::create_dir_all(temp_dir).unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(input_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "qvd").unwrap_or(false))
        .collect();
    entries.sort_by_key(|e| e.path());

    let total = entries.len();
    let mut ok = 0;
    let mut skipped = 0;
    let mut errors: Vec<String> = Vec::new();

    for (i, entry) in entries.iter().enumerate() {
        let path = entry.path();
        let fname = path.file_name().unwrap().to_str().unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let size = entry.metadata().unwrap().len();
        let size_mb = size as f64 / 1_048_576.0;

        if size > 100_000_000 {
            println!("[{}/{}] {} ({:.1} MB) SKIP", i + 1, total, fname, size_mb);
            skipped += 1;
            continue;
        }

        let orig = match qvd::read_qvd_file(path.to_str().unwrap()) {
            Ok(t) => t,
            Err(e) => {
                errors.push(format!("{}: read error: {}", fname, e));
                continue;
            }
        };
        let rows = orig.num_rows();
        let cols = orig.num_cols();
        let input = path.to_str().unwrap();

        let mut file_ok = true;

        // [1] QVD → regenerate → QVD
        {
            let mut t = qvd::read_qvd_file(input).unwrap();
            t.raw_xml.clear();
            t.raw_binary.clear();
            let out = format!("{}/{}", output_dir, fname);
            if let Err(e) = qvd::write_qvd_file(&t, &out) {
                errors.push(format!("{} [1]: write err: {}", fname, e));
                file_ok = false;
            } else if let Ok(rb) = qvd::read_qvd_file(&out) {
                if rb.num_rows() != rows || rb.num_cols() != cols {
                    errors.push(format!("{} [1]: {}x{} vs {}x{}", fname, rows, cols, rb.num_rows(), rb.num_cols()));
                    file_ok = false;
                }
            }
        }

        // [2] QVD → Parquet → QVD
        {
            let pq = format!("{}/{}.parquet", temp_dir, stem);
            let out = format!("{}/{}_from_parquet.qvd", output_dir, stem);
            let r = qvd::convert_qvd_to_parquet(input, &pq, qvd::ParquetCompression::Snappy)
                .and_then(|_| qvd::convert_parquet_to_qvd(&pq, &out));
            let _ = std::fs::remove_file(&pq);
            match r {
                Err(e) => { errors.push(format!("{} [2]: {}", fname, e)); file_ok = false; }
                Ok(()) => {
                    if let Ok(rb) = qvd::read_qvd_file(&out) {
                        if rb.num_rows() != rows || rb.num_cols() != cols {
                            errors.push(format!("{} [2]: {}x{} vs {}x{}", fname, rows, cols, rb.num_rows(), rb.num_cols()));
                            file_ok = false;
                        }
                    }
                }
            }
        }

        // [3] QVD → Arrow RecordBatch → QVD
        {
            let out = format!("{}/{}_from_arrow.qvd", output_dir, stem);
            let r = qvd::qvd_to_record_batch(&orig)
                .and_then(|batch| qvd::write_record_batch_to_qvd(&batch, &orig.header.table_name, &out));
            match r {
                Err(e) => { errors.push(format!("{} [3]: {}", fname, e)); file_ok = false; }
                Ok(()) => {
                    if let Ok(rb) = qvd::read_qvd_file(&out) {
                        if rb.num_rows() != rows || rb.num_cols() != cols {
                            errors.push(format!("{} [3]: {}x{} vs {}x{}", fname, rows, cols, rb.num_rows(), rb.num_cols()));
                            file_ok = false;
                        }
                    }
                }
            }
        }

        // [4] QVD → PQ → QVD → PQ → QVD (double)
        {
            let pq1 = format!("{}/{}_d1.parquet", temp_dir, stem);
            let qvd1 = format!("{}/{}_d1.qvd", temp_dir, stem);
            let pq2 = format!("{}/{}_d2.parquet", temp_dir, stem);
            let out = format!("{}/{}_double_conv.qvd", output_dir, stem);
            let r = qvd::convert_qvd_to_parquet(input, &pq1, qvd::ParquetCompression::Snappy)
                .and_then(|_| qvd::convert_parquet_to_qvd(&pq1, &qvd1))
                .and_then(|_| qvd::convert_qvd_to_parquet(&qvd1, &pq2, qvd::ParquetCompression::Zstd))
                .and_then(|_| qvd::convert_parquet_to_qvd(&pq2, &out));
            let _ = std::fs::remove_file(&pq1);
            let _ = std::fs::remove_file(&qvd1);
            let _ = std::fs::remove_file(&pq2);
            match r {
                Err(e) => { errors.push(format!("{} [4]: {}", fname, e)); file_ok = false; }
                Ok(()) => {
                    if let Ok(rb) = qvd::read_qvd_file(&out) {
                        if rb.num_rows() != rows || rb.num_cols() != cols {
                            errors.push(format!("{} [4]: {}x{} vs {}x{}", fname, rows, cols, rb.num_rows(), rb.num_cols()));
                            file_ok = false;
                        }
                    }
                }
            }
        }

        if file_ok {
            println!("[{}/{}] {} ({:.1} MB) — ALL 4 OK ({} rows, {} cols)",
                i + 1, total, fname, size_mb, rows, cols);
            ok += 1;
        } else {
            println!("[{}/{}] {} — ERRORS (see below)", i + 1, total, fname);
        }
    }

    let _ = std::fs::remove_dir_all(temp_dir);

    println!("\n=== SUMMARY ===");
    println!("Total: {}, OK: {}, Skipped: {}, Errors: {}", total, ok, skipped, errors.len());
    for e in &errors {
        println!("  {}", e);
    }
    assert!(errors.is_empty(), "{} errors", errors.len());
}
