use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("io error")]
    IO(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    TryIntoError(#[from] std::array::TryFromSliceError),

    #[error("Corrupted page: {0}")]
    Corrupted(String),

    #[error("key not found")]
    NotFound,
}
