//! Core network retry mechanics utilizing exponential backoff and jitter.

use std::{future::Future, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tokio_with_wasm::alias as tokio;

use crate::Error;

/// Type alias for a user-provided closure to determine custom retryability logic.
pub type RetryHook = Arc<dyn Fn(&Error) -> bool + Send + Sync>;

/// Configuration for network backoff and retry behavior.
#[derive(Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before yielding the error.
    pub max_attempts: u32,
    /// The foundational delay duration (scaled exponentially).
    pub base_delay: Duration,
    /// The absolute maximum delay limit between retries.
    pub max_delay: Duration,
    /// Optional user-injected hook to override standard S3 protocol retry logic.
    pub retry_hook: Option<RetryHook>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            retry_hook: None,
        }
    }
}

impl std::fmt::Debug for RetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryConfig")
            .field("max_attempts", &self.max_attempts)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("retry_hook_present", &self.retry_hook.is_some())
            .finish()
    }
}

impl RetryConfig {
    /// Evaluates an error against the configured hook or standard defaults.
    #[must_use]
    pub fn should_retry(&self, error: &Error) -> bool {
        self.retry_hook
            .as_ref()
            .map_or_else(|| error.is_default_retryable(), |hook| (hook)(error))
    }
}

/// Evaluates pseudo-random jitter scaled exclusively to 64-bit seconds limits.
///
/// Bounds evaluation is inclusive `[0, max_secs]` to prevent total zeroing
/// when small secondary bounds (like 1 second) are rolled.
fn evaluate_jitter(max_secs: u64) -> u64 {
    if max_secs == 0 {
        return 0;
    }

    #[cfg(target_family = "wasm")]
    {
        let random = web_sys::js_sys::Math::random();
        // Multiply by (max + 1) to ensure the floor truncation maps uniformly up to max_secs
        (random * ((max_secs + 1) as f64)) as u64
    }

    #[cfg(not(target_family = "wasm"))]
    {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();

        // Modulo (max + 1) covers the inclusive upper bound securely
        (nanos as u64) % (max_secs + 1)
    }
}

/// Higher-order async runner applying safe exponential backoff with full jitter.
///
/// # Errors
/// Returns the underlying network error if the max attempts are exceeded,
/// the error is not retryable, or the operation is actively cancelled.
pub async fn with_retry<T, F, Fut>(
    mut action: F,
    config: &RetryConfig,
    cancel_token: &CancellationToken,
) -> crate::error::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = crate::error::Result<T>>,
{
    let mut attempt = 0;

    loop {
        if cancel_token.is_cancelled() {
            return Err(crate::error::Error::Cancelled);
        }

        match action().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                attempt += 1;
                if attempt > config.max_attempts || !config.should_retry(&err) {
                    return Err(err);
                }

                #[allow(clippy::cast_precision_loss)]
                let base_secs = config.base_delay.as_secs() as f64;
                #[allow(clippy::cast_precision_loss)]
                let max_secs = config.max_delay.as_secs() as f64;

                let factor = 2.0_f64.powi((attempt - 1) as i32);

                let capped_secs = (base_secs * factor).min(max_secs) as u64;

                // Full jitter implementation: random[0, min(cap, base * 2^attempt)]
                let sleep_secs = evaluate_jitter(capped_secs);
                let sleep_dur = Duration::from_secs(sleep_secs);

                tracing::warn!(
                    attempt,
                    max_attempts = config.max_attempts,
                    delay_secs = sleep_secs,
                    error = %err,
                    "Transient S3 failure encountered, initiating retry"
                );

                ::tokio::select! {
                    () = tokio::time::sleep(sleep_dur) => {},
                    () = cancel_token.cancelled() => return Err(crate::error::Error::Cancelled),
                }
            }
        }
    }
}
