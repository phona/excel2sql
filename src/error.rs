pub use mysql::{Error as MySQLError};
pub use calamine::{Error as CalaError};

pub enum Error {
    MySQLError(MySQLError),
	CalaError(CalaError),
}

impl From<MySQLError> for Error {
    fn from(e: MySQLError) -> Self {
        Error::MySQLError(e)
    }
}

impl From<CalaError> for Error {
    fn from(e: CalaError) -> Self {
        Error::CalaError(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::CalaError(e) => write!(f, "{}", e),
            Error::MySQLError(e) => write!(f, "{}", e),
        }
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::CalaError(e) => write!(f, "{:?}", e),
            Error::MySQLError(e) => write!(f, "{:?}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::CalaError(e) => Some(e),
            Error::MySQLError(e) => Some(e),
        }
    }
}

