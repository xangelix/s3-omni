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

#[async_trait]
impl S3Writer for std::io::Cursor<Vec<u8>> {
    async fn size_hint(&mut self, size: u64) -> Result<()> {
        self.get_mut().reserve(size as usize);
        Ok(())
    }
}

#[async_trait]
impl S3Writer for std::io::Cursor<&mut Vec<u8>> {
    async fn size_hint(&mut self, size: u64) -> Result<()> {
        self.get_mut().reserve(size as usize);
        Ok(())
    }
}

#[async_trait]
impl S3Writer for tokio::io::DuplexStream {
    async fn size_hint(&mut self, _size: u64) -> Result<()> {
        // Duplex streams are fixed-capacity pipes, so pre-allocation isn't applicable.
        Ok(())
    }
}

#[async_trait]
impl S3Writer for tokio::io::Sink {
    async fn size_hint(&mut self, _size: u64) -> Result<()> {
        // The void absorbs all, no pre-allocation required.
        Ok(())
    }
}

#[async_trait]
impl<W: S3Writer + Send> S3Writer for tokio::io::BufWriter<W> {
    async fn size_hint(&mut self, size: u64) -> Result<()> {
        // Transparently pass the size hint down to the inner writer (e.g., the File)
        self.get_mut().size_hint(size).await
    }
}

#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl S3Writer for tokio::fs::File {
    async fn size_hint(&mut self, size: u64) -> Result<()> {
        // Pre-allocates the file length on the filesystem.
        // This prevents disk fragmentation during parallel multipart chunk writes
        // and safely errors out immediately if the disk lacks capacity.
        self.set_len(size)
            .await
            .ctx("Failed to pre-allocate file length on disk")?;
        Ok(())
    }
}

/// The core client trait abstraction for interacting with S3 objects.
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ObjectOperation: Clone + MaybeSend + 'static {
    /// Mutates the operation to target a specific byte range.
    #[must_use]
    fn with_range(self, range: ByteRange) -> Self;

    /// Retrieves the absolute total size of the object from the remote server.
    async fn get_size(&mut self) -> Result<u64>;

    /// Uploads an object streamingly, automatically scaling to parallel multipart chunks
    /// if the payload exceeds configured chunk boundaries.
    ///
    /// # Errors
    /// Returns an error on network failure, server rejection, or user cancellation.
    /// Yields an ordered list of `ETags` on success (1 `ETag` for single uploads, N for multipart).
    async fn put<I>(&mut self, body: I) -> Result<Vec<(i32, String)>>
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

// 5. From a tokio::fs::File (Native only)
#[cfg(not(target_family = "wasm"))]
impl From<tokio::fs::File> for S3Payload {
    fn from(file: tokio::fs::File) -> Self {
        use futures::StreamExt as _;

        // ReaderStream automatically handles buffering and yielding Bytes chunks
        let stream = tokio_util::io::ReaderStream::new(file).map(|res| {
            res.map_err(|e| Error::Context {
                context: "Failed to read chunk from file stream",
                source: Some(Box::new(e)),
            })
        });

        Self {
            inner: Box::pin(stream),
        }
    }
}

// 6. From standard Strings
impl From<String> for S3Payload {
    fn from(s: String) -> Self {
        // Recycle the Vec<u8> implementation
        Self::from(s.into_bytes())
    }
}

impl From<&'static str> for S3Payload {
    fn from(s: &'static str) -> Self {
        // Recycle the Bytes implementation
        Self::from(Bytes::from_static(s.as_bytes()))
    }
}
