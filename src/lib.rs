pub mod error;
pub mod header;
pub mod symbol;
pub mod value;
pub mod index;
pub mod reader;
pub mod writer;
pub mod exists;
pub mod streaming;

#[cfg(any(feature = "parquet_support", feature = "python"))]
pub mod parquet;

#[cfg(feature = "python")]
pub mod python;

pub use error::{QvdError, QvdResult};
pub use header::QvdTableHeader;
pub use reader::{read_qvd, read_qvd_file, QvdTable};
pub use writer::{write_qvd, write_qvd_file, QvdTableBuilder};
pub use exists::{ExistsIndex, filter_rows_by_exists, filter_rows_by_exists_fast};
pub use value::{QvdSymbol, QvdValue};
pub use streaming::{QvdStreamReader, QvdChunk, open_qvd_stream};

#[cfg(any(feature = "parquet_support", feature = "python"))]
pub use parquet::{
    read_parquet_to_qvd, convert_parquet_to_qvd,
    qvd_to_record_batch, record_batch_to_qvd, write_record_batch_to_qvd,
    convert_qvd_to_parquet, write_qvd_table_to_parquet,
    parquet_to_qvd, qvd_to_parquet,
    ParquetCompression,
};
