use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    SQLiteError(sqlite::Error),
}

impl From<sqlite::Error> for StoreError {
    fn from(e: sqlite::Error) -> Self {
        StoreError::SQLiteError(e)
    }
}
