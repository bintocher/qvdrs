use std::fmt;

#[derive(Debug)]
pub enum QvdError {
    Io(std::io::Error),
    Xml(String),
    Format(String),
    Utf8(std::string::FromUtf8Error),
    SymbolIndex { field: String, index: i64, num_symbols: usize },
}

impl fmt::Display for QvdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QvdError::Io(e) => write!(f, "IO error: {}", e),
            QvdError::Xml(msg) => write!(f, "XML parsing error: {}", msg),
            QvdError::Format(msg) => write!(f, "Invalid QVD format: {}", msg),
            QvdError::Utf8(e) => write!(f, "UTF-8 error: {}", e),
            QvdError::SymbolIndex { field, index, num_symbols } => {
                write!(f, "Symbol index out of bounds: field={}, index={}, symbols={}", field, index, num_symbols)
            }
        }
    }
}

impl std::error::Error for QvdError {}

impl From<std::io::Error> for QvdError {
    fn from(e: std::io::Error) -> Self { QvdError::Io(e) }
}

impl From<std::string::FromUtf8Error> for QvdError {
    fn from(e: std::string::FromUtf8Error) -> Self { QvdError::Utf8(e) }
}

pub type QvdResult<T> = Result<T, QvdError>;
