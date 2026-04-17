//! Shared abstractions for S3 operations independent of the underlying HTTP client.

pub mod backends;
pub mod error;
pub mod util;

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncWrite;

pub use error::{Error, ErrorContextExt, Result};
pub use util::{
    progress::ProgressGuard,
    range::ByteRange,
    retry::{RetryConfig, with_retry},
    send::MaybeSend,
};

#[cfg(feature = "sdk")]
pub use backends::sdk::SdkClient;

#[cfg(feature = "reqwest")]
pub use backends::reqwest::ReqwestClient;

/// Default chunk size for multipart uploads (8 MiB).
pub const DEFAULT_MULTIPART_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// Default concurrency limit for multipart parallel tasks.
pub const DEFAULT_MULTIPART_CONCURRENCY: usize = 4;

/// The metadata returned from a successful S3 download sequence.
#[derive(Debug, Clone, Copy, Default)]
pub struct S3Return {
    /// The actual total number of bytes written to the target writer.
    pub read_length: u64,
    /// The authoritative size of the object evaluated from S3's headers (if available).
    pub object_size: Option<u64>,
}

/// A specialized writer for S3 downloads that allows pre-allocation hints.
///
/// Implementors can use the `size_hint` to prevent vector reallocations
/// or disk fragmentation before the streaming download commences.
#[async_trait]
pub trait S3Writer: AsyncWrite + Unpin + MaybeSend {
    /// Provides a hint about the expected total size of the incoming data.
    ///
    /// # Errors
    /// Returns an error if pre-allocation fails at the destination (e.g., disk full).
    async fn size_hint(&mut self, size: u64) -> Result<()>;
}

/// The core client trait abstraction for interacting with S3 objects.
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ObjectOperation {
    /// Uploads an object streamingly, automatically scaling to parallel multipart chunks
    /// if the payload exceeds configured chunk boundaries.
    ///
    /// # Errors
    /// Returns an error on network failure, server rejection, or user cancellation.
    /// Yields an ordered list of `ETags` on success (1 `ETag` for single uploads, N for multipart).
    async fn put<I>(&mut self, body: I) -> Result<Vec<String>>
    where
        I: Into<S3Payload> + MaybeSend + 'static;

    /// Downloads an object streamingly into the provided writer, utilizing concurrent
    /// multipart range requests if the object size and configuration warrant it.
    ///
    /// # Errors
    /// Returns an error on stream interruption, network failure, or user cancellation.
    async fn get<W: S3Writer>(
        &mut self,
        writer: &mut W,
        total_size: Option<u64>,
    ) -> Result<S3Return>;
}

// 1. Conditionally define the inner stream to drop the `Send` requirement on WASM
#[cfg(not(target_family = "wasm"))]
pub struct S3Payload {
    pub inner: futures::stream::BoxStream<'static, Result<Bytes>>,
}

#[cfg(target_family = "wasm")]
pub struct S3Payload {
    pub inner: futures::stream::LocalBoxStream<'static, Result<Bytes>>,
}

impl Stream for S3Payload {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

// 2. From an in-memory Vec<u8>
impl From<Vec<u8>> for S3Payload {
    fn from(data: Vec<u8>) -> Self {
        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });
        Self {
            inner: Box::pin(stream),
        }
    }
}

// 3. From standard Bytes
impl From<Bytes> for S3Payload {
    fn from(data: Bytes) -> Self {
        let stream = futures::stream::once(async move { Ok(data) });
        Self {
            inner: Box::pin(stream),
        }
    }
}

// 4. From a Browser File (WASM only)
#[cfg(target_family = "wasm")]
impl From<web_sys::File> for S3Payload {
    fn from(file: web_sys::File) -> Self {
        use futures::StreamExt as _;
        use wasm_streams::ReadableStream;
        use web_sys::js_sys::Uint8Array;
        use web_sys::wasm_bindgen::JsCast;

        let js_stream = file.stream();
        let wasm_stream = ReadableStream::from_raw(js_stream.unchecked_into());

        let mapped_stream = wasm_stream.into_stream().map(|js_result| match js_result {
            Ok(js_val) => {
                let array = js_val.unchecked_into::<Uint8Array>();
                Ok(Bytes::from(array.to_vec()))
            }
            Err(_) => Err(Error::Context {
                context: "Failed to read chunk from browser File stream",
                source: None,
            }),
        });

        Self {
            inner: Box::pin(mapped_stream),
        }
    }
}
