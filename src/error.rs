//! Centralized error handling using `thiserror` for deterministic,
//! type-safe error propagation across the crate.

use thiserror::Error;

/// The canonical error type for all S3 and network operations.
#[derive(Debug, Error)]
pub enum Error {
    /// The upload or download operation was explicitly cancelled by the user token.
    #[error("Operation cancelled by user")]
    Cancelled,

    /// A byte range was incorrectly resolved with a start greater than the end.
    #[error("Invalid range resolved: start cannot be greater than end")]
    InvalidRange,

    /// The HTTP response returned a non-success status code.
    #[error("HTTP request failed with status {status_code}: {error_text}")]
    HttpFailed {
        /// The HTTP status code returned.
        status_code: u16,
        /// The raw error body returned by the server.
        error_text: String,
    },

    /// A generalized context wrapper that attaches a descriptive message to an underlying error.
    #[error("{context}")]
    Context {
        /// The human-readable context of the failed operation.
        context: &'static str,
        /// The underlying source error, if any exists.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

impl Error {
    /// Evaluates if the error is a transient fault safe for automated retry sequences.
    /// Default protocol retries cover standard HTTP 429 and 50x series drops,
    /// alongside raw underlying network severance (wrapped via Context).
    #[must_use]
    pub const fn is_default_retryable(&self) -> bool {
        match self {
            Self::HttpFailed { status_code, .. } => {
                matches!(status_code, 408 | 429 | 500 | 502 | 503 | 504)
            }
            Self::Cancelled | Self::InvalidRange => false,
            Self::Context { .. } => true,
        }
    }
}

/// A specialized `Result` alias prioritizing the crate's strongly-typed `Error`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Extension trait to ergonomically attach static context strings to `Result` and `Option`.
pub trait ErrorContextExt<T> {
    /// Wraps the error or empty option in a `crate::error::Error::Context` with the provided string.
    fn ctx(self, msg: &'static str) -> Result<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> ErrorContextExt<T>
    for std::result::Result<T, E>
{
    fn ctx(self, msg: &'static str) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: msg,
            source: Some(Box::new(e)),
        })
    }
}

impl<T> ErrorContextExt<T> for Option<T> {
    fn ctx(self, msg: &'static str) -> Result<T> {
        self.ok_or(Error::Context {
            context: msg,
            source: None,
        })
    }
}
