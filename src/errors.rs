//! ChiselStore errors.

use thiserror::Error;

/// Errors encountered in the store layer.
#[derive(Error, Debug)]
pub enum StoreError {
    /// SQLite error.
    #[error("SQLite error: {0}")]
    SQLiteError(sqlite::Error),
    /// This node is not a leader and cannot therefore execute the command.
    #[error("Node is not a leader")]
    NotLeader,
}

impl From<sqlite::Error> for StoreError {
    fn from(e: sqlite::Error) -> Self {
        StoreError::SQLiteError(e)
    }
}
