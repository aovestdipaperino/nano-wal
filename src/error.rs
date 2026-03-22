use std::fmt;
use std::io;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    InvalidConfig(String),
    CorruptedData(String),
    HeaderTooLarge { size: usize, max: usize },
    Shutdown,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "I/O error: {}", e),
            WalError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            WalError::CorruptedData(msg) => write!(f, "Data corruption: {}", msg),
            WalError::HeaderTooLarge { size, max } => {
                write!(f, "Header size {} exceeds maximum {}", size, max)
            }
            WalError::Shutdown => write!(f, "WAL has been shut down"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, WalError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_error_display() {
        let err = WalError::InvalidConfig("bad".into());
        assert_eq!(format!("{}", err), "Invalid configuration: bad");

        let err = WalError::Shutdown;
        assert_eq!(format!("{}", err), "WAL has been shut down");

        let err = WalError::HeaderTooLarge { size: 70000, max: 65535 };
        assert_eq!(format!("{}", err), "Header size 70000 exceeds maximum 65535");

        let err = WalError::CorruptedData("bad magic".into());
        assert_eq!(format!("{}", err), "Data corruption: bad magic");
    }

    #[test]
    fn test_wal_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let wal_err: WalError = io_err.into();
        assert!(matches!(wal_err, WalError::Io(_)));
    }
}
