//! Provides an S3 interaction client powered by `reqwest`, tailored specifically
//! for working safely with pre-signed URLs.

use std::fmt::Debug;

use ::tokio::{io::AsyncWriteExt as _, select};
use async_trait::async_trait;
use atomic_progress::{Progress, ProgressStack};
use futures::StreamExt as _;
use reqwest::Client;
use tokio_util::sync::CancellationToken;
use tokio_with_wasm::alias as tokio;
use tracing::{debug, info, instrument, trace};

use crate::{
    ByteRange, DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_CONCURRENCY, Error,
    ErrorContextExt as _, MaybeSend, ObjectOperation, ProgressGuard, Result, RetryConfig,
    S3Payload, S3Return, S3Writer, with_retry,
};

/// Builder for constructing a configured `ReqwestClient`.
#[derive(Clone, Debug)]
pub struct ReqwestClient {
    client: Client,
    progress_stack: Option<ProgressStack>,

    range: Option<ByteRange>,
    cancel_token: CancellationToken,
    multipart_chunk_size: u64,
    multipart_concurrency: usize,
    retry_config: RetryConfig,
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            progress_stack: None,

            range: None,
            cancel_token: CancellationToken::new(),
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
            multipart_concurrency: DEFAULT_MULTIPART_CONCURRENCY,
            retry_config: RetryConfig::default(),
        }
    }
}

impl ReqwestClient {
    /// Creates a new unconfigured builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn with_range(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
    }

    #[must_use]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    #[must_use]
    pub const fn with_multipart_chunk_size(mut self, size: u64) -> Self {
        self.multipart_chunk_size = size;
        self
    }

    #[must_use]
    pub const fn with_multipart_concurrency(mut self, concurrency: usize) -> Self {
        self.multipart_concurrency = concurrency;
        self
    }

    #[must_use]
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Consumes the builder and returns a functional `ReqwestClient`.
    ///
    /// # Errors
    /// Returns an error if no presigned URLs have been provided.
    #[instrument(skip(self), err)]
    pub fn op(&self, presigned_urls: Vec<String>) -> Result<ReqwestOperation> {
        Ok(ReqwestOperation {
            client: self.client.clone(),
            progress: self.progress_stack.as_ref().map(|s| s.add_pb("", 0u64)),

            presigned_urls,

            range: self.range,
            cancel_token: self.cancel_token.clone(),
            multipart_chunk_size: self.multipart_chunk_size,
            multipart_concurrency: self.multipart_concurrency,
            retry_config: self.retry_config.clone(),
        })
    }
}

/// A lightweight, stateful client executing HTTP operations against S3.
pub struct ReqwestOperation {
    client: Client,
    progress: Option<Progress>,

    presigned_urls: Vec<String>,

    range: Option<ByteRange>,
    cancel_token: CancellationToken,
    multipart_chunk_size: u64,
    multipart_concurrency: usize,
    retry_config: RetryConfig,
}

impl ReqwestOperation {
    #[must_use]
    pub fn presigned_urls(&self) -> &[String] {
        &self.presigned_urls
    }

    pub const fn presigned_urls_mut(&mut self) -> &mut Vec<String> {
        &mut self.presigned_urls
    }

    #[must_use]
    pub fn with_progress(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }
    #[must_use]
    pub const fn progress(&self) -> Option<&Progress> {
        self.progress.as_ref()
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ObjectOperation for ReqwestOperation {
    #[instrument(skip(self, body), fields(urls = self.presigned_urls.len()), err)]
    async fn put<I>(&mut self, body: I) -> Result<Vec<String>>
    where
        I: Into<S3Payload> + MaybeSend + 'static,
    {
        let mut body = body.into();

        let guard = ProgressGuard(self.progress.clone());
        let is_multipart = self.presigned_urls.len() > 1;

        if !is_multipart {
            let url = self.presigned_urls[0].clone();

            #[cfg(not(target_family = "wasm"))]
            {
                // Single-part streams cannot be safely retried because the stream is consumed.
                // For enterprise-grade operation, chunk sizes should be configured so big files pivot to multipart.
                let progress = self.progress.clone();
                let mapped_stream = body.map(move |res| {
                    if let Ok(ref chunk) = res
                        && let Some(pb) = &progress
                    {
                        pb.inc(chunk.len() as u64);
                    }
                    res
                });
                let req_body = reqwest::Body::wrap_stream(mapped_stream);
                let request = self.client.put(url).body(req_body);

                info!("Transmitting standard PUT request over reqwest native context");
                let response = select! {
                    res = request.send() => res.ctx("Failed to send PUT request to S3")?,
                    () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
                };

                if !response.status().is_success() {
                    let status_code = response.status().as_u16();
                    let error_text = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<failed to read error body>".to_string());
                    tracing::error!(status_code, %error_text, "Failed presigned PUT upload");
                    return Err(Error::HttpFailed {
                        status_code,
                        error_text,
                    });
                }

                let etag = response
                    .headers()
                    .get("ETag")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                debug!("Reqwest PUT upload finalized natively");
                return Ok(vec![etag]);
            }

            #[cfg(target_family = "wasm")]
            {
                use wasm_bindgen_futures::JsFuture;
                use web_sys::js_sys::wasm_bindgen::JsCast;

                let mut buffered_bytes = bytes::BytesMut::new();

                info!(
                    "Buffering stream for WASM single-part upload to bypass 501 chunking rejection"
                );
                loop {
                    let chunk_opt = select! {
                        res = body.next() => res,
                        () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
                    };

                    let Some(chunk_res) = chunk_opt else { break };
                    let chunk = chunk_res.map_err(|e| Error::Context {
                        context: "Failed to read body stream chunk",
                        source: Some(Box::new(e)),
                    })?;

                    guard.inc(chunk.len() as u64);
                    buffered_bytes.extend_from_slice(&chunk);
                }

                let payload = buffered_bytes.freeze();

                // Because WASM single-part buffers fully into memory, we CAN retry it.
                let etag = with_retry(
                    || {
                        let url_ref = &url;
                        let payload_clone = payload.clone();

                        async move {
                            let js_array = web_sys::js_sys::Uint8Array::from(payload_clone.as_ref());
                            let abort_controller =
                                web_sys::AbortController::new().map_err(|_| Error::Context {
                                    context: "Failed to create AbortController",
                                    source: None,
                                })?;
                            let signal = abort_controller.signal();

                            let opts = web_sys::RequestInit::new();
                            opts.set_method("PUT");
                            opts.set_body(&js_array);
                            opts.set_signal(Some(&signal));

                            let request =
                                web_sys::Request::new_with_str_and_init(url_ref, &opts).map_err(|_| {
                                    Error::Context {
                                        context: "Failed to construct JS Request object",
                                        source: None,
                                    }
                                })?;

                            let global = web_sys::js_sys::global();
                            let fetch_promise = if let Ok(window) = global.clone().dyn_into::<web_sys::Window>()
                            {
                                window.fetch_with_request(&request)
                            } else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
                                worker.fetch_with_request(&request)
                            } else {
                                return Err(Error::Context {
                                    context: "Execution environment is neither a Window nor a Worker",
                                    source: None,
                                });
                            };

                            let response_val = JsFuture::from(fetch_promise).await.map_err(|_| Error::Context {
                                context: "JS fetch promise rejected by the browser network layer",
                                source: None
                            })?;

                            let response: web_sys::Response = response_val
                                .dyn_into()
                                .expect("Fetch promise did not resolve to a Response object");

                            if !response.ok() {
                                let status_code = response.status();
                                let error_text = match response.text() {
                                    Ok(text_promise) => match JsFuture::from(text_promise).await {
                                        Ok(text_val) => text_val
                                            .as_string()
                                            .unwrap_or_else(|| "<failed to decode JS string>".to_string()),
                                        Err(_) => "<failed to resolve error text promise>".to_string(),
                                    },
                                    Err(_) => "<failed to extract text from response>".to_string(),
                                };
                                return Err(Error::HttpFailed {
                                    status_code,
                                    error_text,
                                });
                            }

                            let etag = response
                                .headers()
                                .get("ETag")
                                .ok()
                                .flatten()
                                .unwrap_or_default();

                            Ok(etag)
                        }
                    },
                    &self.retry_config,
                    &self.cancel_token
                ).await?;

                debug!("Reqwest PUT upload finalized via WASM");
                return Ok(vec![etag]);
            }
        }

        // --- Multipart Upload Loop ---
        let mut etags = Vec::new();
        let mut url_iter = self.presigned_urls.iter();
        let mut upload_tasks = futures::stream::FuturesUnordered::new();
        let mut current_part_number = 1;
        let mut current_chunk = bytes::BytesMut::with_capacity(self.multipart_chunk_size as usize);

        info!("Starting presigned multipart upload pooling");
        loop {
            let chunk_opt = select! {
                res = body.next() => res,
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            };

            let Some(chunk_res) = chunk_opt else { break };

            let chunk = chunk_res.map_err(|e| Error::Context {
                context: "Failed to read body stream chunk",
                source: Some(Box::new(e)),
            })?;

            guard.inc(chunk.len() as u64);
            current_chunk.extend_from_slice(&chunk);

            let exact_chunk_size = self.multipart_chunk_size as usize;

            while current_chunk.len() >= exact_chunk_size {
                let payload = current_chunk.split_to(exact_chunk_size).freeze();

                let url = url_iter
                    .next()
                    .ctx("Not enough presigned URLs for payload size")?
                    .clone();

                upload_tasks.push(tokio::spawn(upload_part_reqwest(
                    self.client.clone(),
                    url,
                    current_part_number,
                    payload,
                    self.retry_config.clone(),
                    self.cancel_token.clone(),
                )));

                current_part_number += 1;

                while upload_tasks.len() >= self.multipart_concurrency {
                    select! {
                        res = upload_tasks.next() => {
                            if let Some(res) = res {
                                let (part_num, etag) = res.ctx("Task panic")??;
                                etags.push((part_num, etag));
                            }
                        },
                        () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
                    }
                }
            }
        }

        if !current_chunk.is_empty() {
            let url = url_iter
                .next()
                .ctx("Not enough presigned URLs for final chunk")?
                .clone();

            upload_tasks.push(tokio::spawn(upload_part_reqwest(
                self.client.clone(),
                url,
                current_part_number,
                current_chunk.freeze(),
                self.retry_config.clone(),
                self.cancel_token.clone(),
            )));
        }

        while !upload_tasks.is_empty() {
            select! {
                res = upload_tasks.next() => {
                    if let Some(res) = res {
                        let (part_num, etag) = res.ctx("Task panic")??;
                        etags.push((part_num, etag));
                    }
                },
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            }
        }

        etags.sort_by_key(|k| k.0);
        let ordered_etags = etags.into_iter().map(|(_, etag)| etag).collect();

        debug!(
            parts = current_part_number - 1,
            "Completed multipart presigned pool"
        );
        Ok(ordered_etags)
    }

    #[instrument(skip(self, writer), fields(urls = self.presigned_urls.len()), err)]
    async fn get<W>(&mut self, writer: &mut W, total_size: Option<u64>) -> Result<S3Return>
    where
        W: S3Writer,
    {
        let guard = ProgressGuard(self.progress.clone());
        let is_multipart = self.presigned_urls.len() > 1;

        if !is_multipart {
            info!("Transmitting standard GET request");

            // Isolate network connection phase in a retry loop.
            let response = with_retry(
                || {
                    let mut req = self.client.get(self.presigned_urls()[0].clone());
                    if let Some(range) = &self.range {
                        req = req.header(reqwest::header::RANGE, range.to_string());
                    }
                    async move {
                        let res = req.send().await.ctx("Failed to send GET request")?;
                        if !res.status().is_success() {
                            let status_code = res.status().as_u16();
                            let error_text = res
                                .text()
                                .await
                                .unwrap_or_else(|_| "<failed to read error body>".to_string());
                            return Err(Error::HttpFailed {
                                status_code,
                                error_text,
                            });
                        }
                        Ok(res)
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            let content_length = response.content_length();
            if let Some(len) = content_length
                && len > 0
            {
                writer.size_hint(len).await?;
            } else if let Some(len) = total_size {
                writer.size_hint(len).await?;
            }

            let object_size = crate::util::range::parse_object_size_from_content_range(
                response
                    .headers()
                    .get(reqwest::header::CONTENT_RANGE)
                    .and_then(|h| h.to_str().ok()),
            );

            let mut stream = response.bytes_stream();
            let mut total_written = 0;

            loop {
                let chunk_res = select! {
                    res = stream.next() => res,
                    () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
                };

                let Some(chunk_result) = chunk_res else { break };
                let chunk = chunk_result.ctx("Failed to read chunk from network")?;

                guard.inc(chunk.len() as u64);

                writer
                    .write_all(&chunk)
                    .await
                    .ctx("Failed to write chunk")?;
                total_written += chunk.len() as u64;
            }

            debug!(total_written, "Reqwest GET stream fully consumed");
            return Ok(S3Return {
                read_length: total_written,
                object_size: object_size.or(total_size),
            });
        }

        // --- Multipart GET Sequence ---
        if let Some(total_size) = total_size
            && total_size > 0
        {
            writer.size_hint(total_size).await?;
        }

        // 1. Reconstruct the absolute bounds matching SdkOperation
        let safe_total = total_size.unwrap_or(u64::MAX);
        let (absolute_start, absolute_end) = self.range.as_ref().map_or_else(
            || (0, safe_total.saturating_sub(1)),
            |r| r.resolve_bounds(safe_total),
        );

        let chunk_size = self.multipart_chunk_size;
        let urls = self.presigned_urls.clone();
        let client = self.client.clone();
        let progress = self.progress.clone();
        let retry_cfg = self.retry_config.clone();
        let cancel_tok = self.cancel_token.clone();

        // 2. Enumerate to calculate the exact range for each chunk
        let download_tasks = urls.into_iter().enumerate().map(|(index, url)| {
            let client = client.clone();
            let progress = progress.clone();
            let retry_cfg = retry_cfg.clone();
            let cancel_tok = cancel_tok.clone();

            let start = absolute_start + (index as u64 * chunk_size);
            let end = std::cmp::min(start + chunk_size - 1, absolute_end);
            let range_header = format!("bytes={start}-{end}");

            async move {
                with_retry(
                    || {
                        let client_ref = &client;
                        let url_ref = &url;
                        let prog_clone = progress.clone();
                        let range_str = range_header.clone();

                        async move {
                            trace!("Dispatching chunked GET request");
                            let response = client_ref
                                .get(url_ref)
                                .header(reqwest::header::RANGE, range_str) // <- Inject the signed header
                                .send()
                                .await
                                .ctx("Failed GET request")?;

                            if !response.status().is_success() {
                                return Err(Error::HttpFailed {
                                    status_code: response.status().as_u16(),
                                    error_text: response.text().await.unwrap_or_default(),
                                });
                            }

                            let mut stream = response.bytes_stream();
                            let mut chunk_bytes = bytes::BytesMut::new();

                            while let Some(chunk_result) = stream.next().await {
                                let chunk = chunk_result.ctx("Failed to read stream chunk")?;
                                if let Some(pb) = &prog_clone {
                                    pb.inc(chunk.len() as u64);
                                }
                                chunk_bytes.extend_from_slice(&chunk);
                            }

                            Ok(chunk_bytes.freeze())
                        }
                    },
                    &retry_cfg,
                    &cancel_tok,
                )
                .await
            }
        });

        let mut buffered_stream =
            futures::stream::iter(download_tasks).buffered(self.multipart_concurrency);
        let mut total_written = 0;

        info!("Consuming ordered chunks from pre-signed bounds");
        loop {
            let chunk_opt = select! {
                res = buffered_stream.next() => res,
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            };

            let Some(chunk_result) = chunk_opt else { break };
            let chunk = chunk_result?;

            writer
                .write_all(&chunk)
                .await
                .ctx("Failed to write multipart chunk")?;
            total_written += chunk.len() as u64;
        }

        Ok(S3Return {
            read_length: total_written,
            object_size: total_size,
        })
    }
}

/// Internal helper encapsulating the isolation logic for a singular reqwest multipart segment.
#[instrument(skip(client, payload, retry_config, cancel_token), fields(part = part_number, size = payload.len()), err)]
async fn upload_part_reqwest(
    client: reqwest::Client,
    url: String,
    part_number: usize,
    payload: bytes::Bytes,
    retry_config: RetryConfig,
    cancel_token: CancellationToken,
) -> Result<(usize, String)> {
    with_retry(
        || {
            let client_ref = &client;
            let url_ref = &url;
            let payload_clone = payload.clone();

            async move {
                let resp = client_ref
                    .put(url_ref)
                    .body(reqwest::Body::from(payload_clone))
                    .send()
                    .await
                    .ctx("Failed to send PUT request for multipart segment")?;

                if !resp.status().is_success() {
                    let status_code = resp.status().as_u16();
                    let error_text = resp
                        .text()
                        .await
                        .unwrap_or_else(|_| "<failed to read error>".to_string());
                    return Err(Error::HttpFailed {
                        status_code,
                        error_text,
                    });
                }

                let etag = resp
                    .headers()
                    .get("ETag")
                    .ctx("Missing ETag from response headers")?
                    .to_str()
                    .ctx("ETag header is not valid UTF-8")?
                    .to_string();

                Ok(etag)
            }
        },
        &retry_config,
        &cancel_token,
    )
    .await
    .map(|etag| {
        trace!(%etag, "Received segment confirmation ETag");
        (part_number, etag)
    })
}
