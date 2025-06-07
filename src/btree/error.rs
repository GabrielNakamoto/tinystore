use std::fmt;

pub enum NodeError {
    Io(std::io::Error),
    Decoding(bincode::error::DecodeError),
    Encoding(bincode::error::EncodeError),
}

impl From<bincode::error::DecodeError> for NodeError {
    fn from(error: bincode::error::DecodeError) -> Self {
        NodeError::Decoding(error)
    }
}

impl From<bincode::error::EncodeError> for NodeError {
    fn from(error: bincode::error::EncodeError) -> Self {
        NodeError::Encoding(error)
    }
}

impl From<std::io::Error> for NodeError {
    fn from(error: std::io::Error) -> Self {
        NodeError::Io(error)
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Io(e) => write!(f, "IO error: {}", e),
            NodeError::Decoding(e) => write!(f, "Bincode decoding error: {}", e),
            NodeError::Encoding(e) => write!(f, "Bincode encoding error: {}", e),
        }
    }
}
