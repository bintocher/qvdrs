use std::hash::{Hash, Hasher};

/// Represents a single value in a QVD symbol table.
#[derive(Debug, Clone)]
pub enum QvdSymbol {
    /// Type 0x01: 4-byte signed integer (little-endian)
    Int(i32),
    /// Type 0x02: 8-byte IEEE 754 double (little-endian)
    Double(f64),
    /// Type 0x04: null-terminated UTF-8 string
    Text(String),
    /// Type 0x05: 4-byte integer + null-terminated string representation
    DualInt(i32, String),
    /// Type 0x06: 8-byte double + null-terminated string representation
    DualDouble(f64, String),
}

impl QvdSymbol {
    /// Returns the string representation of this symbol.
    pub fn to_string_repr(&self) -> String {
        match self {
            QvdSymbol::Int(v) => v.to_string(),
            QvdSymbol::Double(v) => v.to_string(),
            QvdSymbol::Text(s) => s.clone(),
            QvdSymbol::DualInt(_, s) => s.clone(),
            QvdSymbol::DualDouble(_, s) => s.clone(),
        }
    }

    /// Returns the numeric value if available.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            QvdSymbol::Int(v) => Some(*v as f64),
            QvdSymbol::Double(v) => Some(*v),
            QvdSymbol::DualInt(v, _) => Some(*v as f64),
            QvdSymbol::DualDouble(v, _) => Some(*v),
            QvdSymbol::Text(_) => None,
        }
    }

    /// Returns the type byte for binary serialization.
    pub fn type_byte(&self) -> u8 {
        match self {
            QvdSymbol::Int(_) => 0x01,
            QvdSymbol::Double(_) => 0x02,
            QvdSymbol::Text(_) => 0x04,
            QvdSymbol::DualInt(_, _) => 0x05,
            QvdSymbol::DualDouble(_, _) => 0x06,
        }
    }

    /// Returns the binary size of this symbol in the symbol table.
    pub fn binary_size(&self) -> usize {
        match self {
            QvdSymbol::Int(_) => 1 + 4,
            QvdSymbol::Double(_) => 1 + 8,
            QvdSymbol::Text(s) => 1 + s.len() + 1,
            QvdSymbol::DualInt(_, s) => 1 + 4 + s.len() + 1,
            QvdSymbol::DualDouble(_, s) => 1 + 8 + s.len() + 1,
        }
    }
}

impl PartialEq for QvdSymbol {
    fn eq(&self, other: &Self) -> bool {
        self.to_string_repr() == other.to_string_repr()
    }
}

impl Eq for QvdSymbol {}

impl Hash for QvdSymbol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_string_repr().hash(state);
    }
}

/// Represents a cell value in the decoded QVD table.
#[derive(Debug, Clone)]
pub enum QvdValue {
    /// Non-null value referencing a symbol
    Symbol(QvdSymbol),
    /// NULL value
    Null,
}

impl QvdValue {
    pub fn is_null(&self) -> bool {
        matches!(self, QvdValue::Null)
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            QvdValue::Symbol(s) => Some(s.to_string_repr()),
            QvdValue::Null => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            QvdValue::Symbol(s) => s.as_f64(),
            QvdValue::Null => None,
        }
    }
}
