use std::io::Read;

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: roundtrip <file.qvd>");
        std::process::exit(1);
    });

    let output_dir = "qvd_output";
    std::fs::create_dir_all(output_dir).unwrap();

    let filename = std::path::Path::new(&path)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let output_path = format!("{}/{}", output_dir, filename);

    println!("Reading: {}", path);
    let table = qvd::read_qvd_file(&path).unwrap();
    println!(
        "  Table: {}, Rows: {}, Cols: {}",
        table.header.table_name,
        table.num_rows(),
        table.num_cols()
    );

    println!("Writing: {}", output_path);
    qvd::write_qvd_file(&table, &output_path).unwrap();

    // Compare file checksums
    let orig_hash = file_md5(&path);
    let new_hash = file_md5(&output_path);

    let orig_size = std::fs::metadata(&path).unwrap().len();
    let new_size = std::fs::metadata(&output_path).unwrap().len();

    println!("Original:  size={}, md5={}", orig_size, orig_hash);
    println!("Written:   size={}, md5={}", new_size, new_hash);

    if orig_hash == new_hash {
        println!("PASS: checksums match!");
    } else {
        println!("FAIL: checksums differ!");
        // Show where files diverge
        compare_files(&path, &output_path);
        std::process::exit(1);
    }
}

fn file_md5(path: &str) -> String {
    let mut file = std::fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    // Simple MD5-like hash for comparison (not cryptographic, just for testing)
    // We'll use a simple checksum since we don't want external deps in the example
    let mut hash: u64 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64).wrapping_add(i as u64);
    }
    format!("{:016x}", hash)
}

fn compare_files(path1: &str, path2: &str) {
    let buf1 = std::fs::read(path1).unwrap();
    let buf2 = std::fs::read(path2).unwrap();

    let min_len = buf1.len().min(buf2.len());
    for i in 0..min_len {
        if buf1[i] != buf2[i] {
            let start = if i > 16 { i - 16 } else { 0 };
            let end = (i + 16).min(min_len);
            println!("First difference at byte {}:", i);
            println!("  Original: {:?}", &buf1[start..end]);
            println!("  Written:  {:?}", &buf2[start..end]);
            return;
        }
    }
    if buf1.len() != buf2.len() {
        println!("Files differ in length: {} vs {}", buf1.len(), buf2.len());
    }
}
