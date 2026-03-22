use qvd::read_qvd_file;

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: read_qvd <file.qvd>");
        std::process::exit(1);
    });

    println!("Reading QVD file: {}", path);

    match read_qvd_file(&path) {
        Ok(table) => {
            println!("Table: {}", table.header.table_name);
            println!("Rows: {}", table.num_rows());
            println!("Columns: {}", table.num_cols());
            println!();

            for (i, field) in table.header.fields.iter().enumerate() {
                println!(
                    "  [{:2}] {:<30} symbols={:<6} bit_offset={:<4} bit_width={:<3} bias={}",
                    i, field.field_name, field.no_of_symbols,
                    field.bit_offset, field.bit_width, field.bias
                );
            }

            println!();
            let preview_rows = table.num_rows().min(10);
            println!("First {} rows:", preview_rows);

            // Print header
            for field in &table.header.fields {
                print!("{:<20}", field.field_name);
            }
            println!();
            for _ in &table.header.fields {
                print!("{:-<20}", "");
            }
            println!();

            // Print data
            for row in 0..preview_rows {
                for col in 0..table.num_cols() {
                    let val = table.get(row, col);
                    let s = val.as_string().unwrap_or_else(|| "NULL".to_string());
                    let display = if s.len() > 18 { format!("{}...", &s[..15]) } else { s };
                    print!("{:<20}", display);
                }
                println!();
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
