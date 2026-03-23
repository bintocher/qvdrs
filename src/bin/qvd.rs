use std::time::Instant;
use clap::{Parser, Subcommand};
use qvd::parquet::ParquetCompression;

#[derive(Parser)]
#[command(name = "qvd", version, about = "QVD file utility — convert, inspect, and query QVD files")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert between QVD and Parquet formats
    Convert {
        /// Input file path (QVD or Parquet)
        input: String,
        /// Output file path (QVD or Parquet)
        output: String,
        /// Compression for Parquet output: none, snappy, gzip, lz4, zstd
        #[arg(short, long, default_value = "snappy")]
        compression: String,
    },
    /// Show file metadata and schema
    Inspect {
        /// QVD file path
        path: String,
    },
    /// Show first N rows of a QVD file
    Head {
        /// QVD file path
        path: String,
        /// Number of rows to display
        #[arg(short, long, default_value = "10")]
        rows: usize,
    },
    /// Show QVD file schema (column names and types)
    Schema {
        /// QVD file path
        path: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, output, compression } => {
            cmd_convert(&input, &output, &compression);
        }
        Commands::Inspect { path } => {
            cmd_inspect(&path);
        }
        Commands::Head { path, rows } => {
            cmd_head(&path, rows);
        }
        Commands::Schema { path } => {
            cmd_schema(&path);
        }
    }
}

fn cmd_convert(input: &str, output: &str, compression: &str) {
    let start = Instant::now();

    let input_lower = input.to_lowercase();
    let output_lower = output.to_lowercase();

    let result = if input_lower.ends_with(".parquet") && output_lower.ends_with(".qvd") {
        // Parquet → QVD
        println!("Converting Parquet → QVD");
        println!("  Input:  {}", input);
        println!("  Output: {}", output);
        qvd::convert_parquet_to_qvd(input, output)
    } else if input_lower.ends_with(".qvd") && output_lower.ends_with(".parquet") {
        // QVD → Parquet
        let comp = match ParquetCompression::parse(compression) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        };
        println!("Converting QVD → Parquet (compression: {})", compression);
        println!("  Input:  {}", input);
        println!("  Output: {}", output);
        qvd::convert_qvd_to_parquet(input, output, comp)
    } else if input_lower.ends_with(".qvd") && output_lower.ends_with(".qvd") {
        // QVD → QVD (rewrite/regenerate)
        println!("Rewriting QVD");
        println!("  Input:  {}", input);
        println!("  Output: {}", output);
        let table = match qvd::read_qvd_file(input) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Error reading QVD: {}", e);
                std::process::exit(1);
            }
        };
        qvd::write_qvd_file(&table, output)
    } else if input_lower.ends_with(".parquet") && output_lower.ends_with(".parquet") {
        // Parquet → Parquet (recompress)
        let comp = match ParquetCompression::parse(compression) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        };
        println!("Recompressing Parquet (compression: {})", compression);
        println!("  Input:  {}", input);
        println!("  Output: {}", output);
        let table = match qvd::read_parquet_to_qvd(input) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Error reading Parquet: {}", e);
                std::process::exit(1);
            }
        };
        qvd::write_qvd_table_to_parquet(&table, output, comp)
    } else {
        eprintln!("Error: Cannot determine conversion direction.");
        eprintln!("Supported: .qvd <-> .parquet");
        std::process::exit(1);
    };

    match result {
        Ok(()) => {
            let elapsed = start.elapsed();
            let out_size = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
            println!("  Done in {:.1}s, output size: {}", elapsed.as_secs_f64(), format_size(out_size));
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_inspect(path: &str) {
    let start = Instant::now();
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let table = match qvd::read_qvd_file(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error reading QVD: {}", e);
            std::process::exit(1);
        }
    };
    let elapsed = start.elapsed();

    println!("File:       {}", path);
    println!("Size:       {}", format_size(file_size));
    println!("Table:      {}", table.header.table_name);
    println!("Rows:       {}", format_number(table.header.no_of_records));
    println!("Columns:    {}", table.header.fields.len());
    println!("Created:    {}", table.header.create_utc_time);
    println!("Build:      {}", table.header.qv_build_no);
    println!("RecordSize: {} bytes", table.header.record_byte_size);
    println!("Read time:  {:.2}s", elapsed.as_secs_f64());
    println!();
    println!("{:<30} {:>10} {:>8} {:>6} {:>6}  {}", "Column", "Symbols", "BitWidth", "Bias", "FmtType", "Tags");
    println!("{}", "-".repeat(80));
    for field in &table.header.fields {
        let tags = field.tags.join(", ");
        println!("{:<30} {:>10} {:>8} {:>6} {:>6}  {}",
            field.field_name,
            format_number(field.no_of_symbols),
            field.bit_width,
            field.bias,
            &field.number_format.format_type,
            tags,
        );
    }
}

fn cmd_head(path: &str, rows: usize) {
    let table = match qvd::read_qvd_file(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error reading QVD: {}", e);
            std::process::exit(1);
        }
    };

    let n = rows.min(table.num_rows());
    let col_names: Vec<&str> = table.column_names();

    // Compute column widths
    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for row in 0..n {
        for col in 0..col_names.len() {
            let val = table.get(row, col);
            let len = val.as_string().map(|s| s.len()).unwrap_or(4); // "NULL"
            widths[col] = widths[col].max(len).min(40);
        }
    }

    // Print header
    let header: Vec<String> = col_names.iter().enumerate()
        .map(|(i, name)| format!("{:<width$}", name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    // Print rows
    for row in 0..n {
        let vals: Vec<String> = (0..col_names.len()).map(|col| {
            let val = table.get(row, col);
            let s = val.as_string().unwrap_or_else(|| "NULL".to_string());
            let truncated = if s.len() > 40 { format!("{}...", &s[..37]) } else { s };
            format!("{:<width$}", truncated, width = widths[col])
        }).collect();
        println!("{}", vals.join(" | "));
    }

    println!("\n[{} rows x {} columns, {} total rows]", n, col_names.len(), format_number(table.num_rows()));
}

fn cmd_schema(path: &str) {
    let table = match qvd::read_qvd_file(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error reading QVD: {}", e);
            std::process::exit(1);
        }
    };

    let batch = match qvd::qvd_to_record_batch(&table) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error converting to Arrow: {}", e);
            std::process::exit(1);
        }
    };

    println!("Arrow Schema for '{}':", path);
    println!();
    for field in batch.schema().fields() {
        println!("  {:<30} {:?}{}", field.name(), field.data_type(),
            if field.is_nullable() { " (nullable)" } else { "" });
    }
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
