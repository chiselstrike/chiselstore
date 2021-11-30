//! ChiselStore errors.

use thiserror::Error;

/// Errors encountered in the store layer.
#[derive(Error, Debug)]
pub enum StoreError {
    /// SQLite error.
    #[error("SQLite error: {0}")]
    SQLiteError(sqlite::Error),
}

impl From<sqlite::Error> for StoreError {
    fn from(e: sqlite::Error) -> Self {
        StoreError::SQLiteError(e)
    }
}
