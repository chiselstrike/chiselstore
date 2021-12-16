//! ChiselStore errors.

use thiserror::Error;

/// Errors encountered in the store layer.
#[derive(Error, Debug)]
pub enum StoreError {
    /// SQLite error.
    #[error("SQLite error: {0}")]
    SQLiteError(#[from] sqlite::Error),
    /// This node is not a leader and cannot therefore execute the command.
    #[error("Node is not a leader")]
    NotLeader,
}