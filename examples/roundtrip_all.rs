use std::io::Read;
use std::time::Instant;

fn main() {
    let input_dir = std::env::args().nth(1).unwrap_or_else(|| "qvd_input".to_string());
    let output_dir = "qvd_output";
    std::fs::create_dir_all(output_dir).unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(&input_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "qvd"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    let mut pass = 0;
    let mut fail = 0;

    for entry in &entries {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        let input_path = path.to_str().unwrap();
        let output_path = format!("{}/{}", output_dir, filename);
        let file_size = std::fs::metadata(input_path).unwrap().len();

        print!("{:<45} {:>12} bytes  ", filename, file_size);

        let start = Instant::now();
        match qvd::read_qvd_file(input_path) {
            Ok(table) => {
                let read_time = start.elapsed();
                print!("read={:>6.1}s  ", read_time.as_secs_f64());

                let write_start = Instant::now();
                match qvd::write_qvd_file(&table, &output_path) {
                    Ok(()) => {
                        let write_time = write_start.elapsed();
                        print!("write={:>6.1}s  ", write_time.as_secs_f64());

                        let orig_hash = file_hash(input_path);
                        let new_hash = file_hash(&output_path);

                        if orig_hash == new_hash {
                            println!("PASS (rows={}, cols={})", table.num_rows(), table.num_cols());
                            pass += 1;
                        } else {
                            let orig_size = std::fs::metadata(input_path).unwrap().len();
                            let new_size = std::fs::metadata(&output_path).unwrap().len();
                            println!("FAIL (size: {} vs {})", orig_size, new_size);
                            fail += 1;
                        }
                    }
                    Err(e) => {
                        println!("WRITE ERROR: {}", e);
                        fail += 1;
                    }
                }
            }
            Err(e) => {
                println!("READ ERROR: {}", e);
                fail += 1;
            }
        }
    }

    println!();
    println!("Results: {} passed, {} failed out of {} files", pass, fail, pass + fail);
    if fail > 0 {
        std::process::exit(1);
    }
}

fn file_hash(path: &str) -> u128 {
    let mut file = std::fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    // FNV-1a 128-bit hash
    let mut hash: u128 = 0x6c62272e07bb0142_62b821756295c58d;
    for &byte in &buf {
        hash ^= byte as u128;
        hash = hash.wrapping_mul(0x0000000001000000_000000000000013b);
    }
    hash
}
