//! Defines interception primitives for `aws-sdk-s3` to monitor upload stream progress.

use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use atomic_progress::{Progress, ProgressStack};
use aws_sdk_s3::{
    Client,
    config::{
        ConfigBag, Intercept, RuntimeComponents, interceptors::BeforeTransmitInterceptorContextMut,
    },
    error::BoxError,
    presigning::PresigningConfig,
};
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use http_body::{Body, Frame, SizeHint};
use tokio::io::AsyncWriteExt as _;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};

use crate::{
    ByteRange, DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_CONCURRENCY,
    DEFAULT_PRESIGNED_EXPIRES_IN, Error, ErrorContextExt as _, MaybeSend, ObjectOperation,
    ProgressGuard, Result, RetryConfig, S3Payload, S3Return, S3Writer,
};

/// Interceptor that safely injects a progress-tracking wrapper around the S3 request body.
#[derive(Clone)]
pub struct ProgressInterceptor {
    /// The progress bar to update as bytes are transmitted.
    pub progress: Option<atomic_progress::Progress>,
}

impl std::fmt::Debug for ProgressInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressInterceptor")
            .finish_non_exhaustive()
    }
}

impl Intercept for ProgressInterceptor {
    fn name(&self) -> &'static str {
        "ProgressInterceptor"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut original_body = SdkBody::taken();
        std::mem::swap(context.request_mut().body_mut(), &mut original_body);

        let wrapped_body = ProgressBody {
            inner: original_body,
            progress: self.progress.clone(),
        };

        *context.request_mut().body_mut() = SdkBody::from_body_1_x(wrapped_body);

        Ok(())
    }
}

/// A wrapper `Body` that transparently delegates chunk polling to the inner `SdkBody`
/// while side-effecting size increments onto a progress tracker.
pub struct ProgressBody {
    inner: SdkBody,
    progress: Option<atomic_progress::Progress>,
}

impl Body for ProgressBody {
    type Data = Bytes;
    type Error = aws_smithy_types::body::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let inner = Pin::new(&mut self.inner);
        let poll_result = inner.poll_frame(cx);

        if let Poll::Ready(Some(Ok(ref frame))) = poll_result
            && let Some(data) = frame.data_ref()
            && let Some(pb) = &self.progress
        {
            pb.inc(data.len() as u64);
        }

        poll_result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Debug, Clone)]
pub struct StripContentLengthInterceptor;

impl Intercept for StripContentLengthInterceptor {
    fn name(&self) -> &'static str {
        "StripContentLengthInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // Strip the header injected by the SDK so it doesn't get signed
        context.request_mut().headers_mut().remove("content-length");
        context.request_mut().headers_mut().remove("Content-Length");
        Ok(())
    }
}

/// Configurable builder to construct the heavy AWS SDK client cleanly.
#[derive(Clone)]
pub struct SdkClient {
    client: Client,
    progress_stack: Option<ProgressStack>,

    range: Option<ByteRange>,
    cancel_token: CancellationToken,
    multipart_chunk_size: u64,
    multipart_concurrency: usize,
    presigned_expires_in: Duration,

    retry_config: RetryConfig,
}

impl SdkClient {
    pub async fn new(
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self> {
        let cancel_token = CancellationToken::new();

        let credentials = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_access_key,
            None,
            None,
            "Environment",
        );

        let config =
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .stalled_stream_protection(
                    aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                )
                .credentials_provider(credentials)
                .region("auto")
                .load()
                .await;

        let config = aws_sdk_s3::config::Builder::from(&config)
            .endpoint_url(endpoint)
            .build();

        let aws_client = Client::from_conf(config);
        info!("SdkClient configured successfully");

        Ok(Self {
            client: aws_client,
            progress_stack: None,

            range: None,
            cancel_token,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
            multipart_concurrency: DEFAULT_MULTIPART_CONCURRENCY,
            presigned_expires_in: DEFAULT_PRESIGNED_EXPIRES_IN,

            retry_config: RetryConfig::default(),
        })
    }
    /// Constructs an `SdkClient` from an existing, externally configured AWS Client.
    #[must_use]
    pub fn from_aws_client(client: aws_sdk_s3::Client) -> Self {
        Self {
            client,
            progress_stack: None,
            range: None,
            cancel_token: CancellationToken::new(),
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
            multipart_concurrency: DEFAULT_MULTIPART_CONCURRENCY,
            presigned_expires_in: DEFAULT_PRESIGNED_EXPIRES_IN,
            retry_config: RetryConfig::default(),
        }
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
    pub const fn with_presigned_expires_in(mut self, duration: Duration) -> Self {
        self.presigned_expires_in = duration;
        self
    }

    #[must_use]
    pub fn with_progress_stack(mut self, progress: ProgressStack) -> Self {
        self.progress_stack = Some(progress);
        self
    }

    #[must_use]
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Evaluates configurations and establishes the AWS backend configuration.
    ///
    /// # Errors
    /// Returns an error if environment variables or core properties are absent/invalid.
    pub fn op(
        &self,
        bucket: impl Into<String> + Debug,
        key: impl Into<String> + Debug,
    ) -> SdkOperation {
        debug!("Building SdkClient context");

        let bucket = bucket.into();
        let key = key.into();

        SdkOperation {
            client: self.client.clone(),
            progress: self.progress_stack.as_ref().map(|s| s.add_pb("", 0u64)),

            bucket,
            key,
            range: self.range,
            cancel_token: self.cancel_token.clone(),
            multipart_chunk_size: self.multipart_chunk_size,
            multipart_concurrency: self.multipart_concurrency,
            presigned_expires_in: self.presigned_expires_in,
            retry_config: self.retry_config.clone(),
        }
    }
}

/// A heavy SDK wrapper holding AWS primitives to drive robust transfers.
#[derive(Clone)]
pub struct SdkOperation {
    client: Client,

    bucket: String,
    key: String,

    range: Option<ByteRange>,
    cancel_token: CancellationToken,
    multipart_chunk_size: u64,
    multipart_concurrency: usize,
    presigned_expires_in: Duration,
    progress: Option<Progress>,
    retry_config: RetryConfig,
}

impl SdkOperation {
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub const fn bucket_mut(&mut self) -> &mut String {
        &mut self.bucket
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    pub const fn key_mut(&mut self) -> &mut String {
        &mut self.key
    }

    #[must_use]
    pub const fn range(&self) -> Option<&ByteRange> {
        self.range.as_ref()
    }
    pub const fn range_mut(&mut self) -> &mut Option<ByteRange> {
        &mut self.range
    }
    /// Mutates the byte range for this specific operation context.
    #[must_use]
    pub const fn with_range(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
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

    #[must_use]
    pub const fn aws_client(&self) -> &Client {
        &self.client
    }

    pub const fn aws_client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Evaluates standard presigning logic tied to local configuration duration.
    pub fn presigning_config(&self) -> Result<PresigningConfig> {
        PresigningConfig::builder()
            .start_time(std::time::SystemTime::now())
            .expires_in(self.presigned_expires_in)
            .build()
            .ctx("Failed to build presigning configuration")
    }

    /// Generates a singular pre-signed URL capable of ingesting streaming bodies.
    #[instrument(skip(self), fields(bucket = %self.bucket, key = %self.key), err)]
    pub async fn create_presigned_upload(
        &self,
        _content_length: u64, // Keep for trait/API compatibility, but ignore it
    ) -> Result<String> {
        let config = self.presigning_config()?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let config = config.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                async move {
                    let presigned_req = client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .customize()
                        .interceptor(StripContentLengthInterceptor)
                        .presigned(config)
                        .await
                        .ctx("Failed to create presigned upload URL")?;
                    Ok(presigned_req.uri().to_string())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await
    }

    /// Initiates a multipart pseudo-upload process and maps it to chunked presigned GET ranges.
    #[instrument(skip(self), fields(bucket = %self.bucket, key = %self.key), err)]
    pub async fn create_presigned_multipart_download(
        &self,
        total_size: u64,
    ) -> Result<Vec<(u64, u64, String)>> {
        let (absolute_start, absolute_end) = self.range.as_ref().map_or_else(
            || (0, total_size.saturating_sub(1)),
            |r| r.resolve_bounds(total_size),
        );

        if absolute_start > absolute_end {
            error!("Calculated download bounds are invalid");
            return Err(Error::InvalidRange);
        }

        let mut urls = Vec::new();
        let mut current_start = absolute_start;
        let chunk_size = self.multipart_chunk_size;

        let presigning_config = self.presigning_config()?;

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        while current_start <= absolute_end {
            let part_end = std::cmp::min(current_start + chunk_size - 1, absolute_end);
            let range_header = format!("bytes={current_start}-{part_end}");

            let url = crate::util::retry::with_retry(
                || {
                    let client = client.clone();
                    let config = presigning_config.clone();
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let range_str = range_header.clone();
                    async move {
                        let presigned_req = client
                            .get_object()
                            .bucket(&bucket)
                            .key(&key)
                            .range(&range_str)
                            .presigned(config)
                            .await
                            .ctx("Failed to generate presigned GET URL for part")?;
                        Ok(presigned_req.uri().to_string())
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            urls.push((current_start, part_end, url));
            current_start = part_end + 1;
        }

        debug!(parts = urls.len(), "Presigned multipart download generated");
        Ok(urls)
    }

    /// Initiates a multipart upload and generates pre-signed PUT URLs for each chunk
    /// dynamically calculated from the total size and internal chunk configuration.
    ///
    /// The `Content-Length` of each part is explicitly calculated and signed to enforce
    /// strict payload boundaries.
    ///
    /// Returns a tuple containing the `UploadId` and an ordered list of pre-signed URLs.
    #[instrument(skip(self), fields(bucket = %self.bucket, key = %self.key, size = total_size), err)]
    pub async fn create_presigned_multipart_upload(
        &self,
        total_size: u64,
    ) -> Result<(String, Vec<String>)> {
        let presigning_config = self.presigning_config()?;

        let chunk_size = self.multipart_chunk_size;

        // Calculate parts needed, ensuring at least 1 part even for 0-byte payloads
        let part_count =
            std::cmp::max(1, (total_size + chunk_size.saturating_sub(1)) / chunk_size) as usize;

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        // 1. Initialize the multipart upload to obtain the authoritative UploadId
        let upload_id = crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();

                async move {
                    let create_res = client
                        .create_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                        .ctx("Failed to create multipart upload initialization context")?;

                    Ok(create_res
                        .upload_id()
                        .ctx("S3 did not return a valid UploadId")?
                        .to_string())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        // 2. Generate the exactly sized array of pre-signed URLs for the parts
        let mut urls = Vec::with_capacity(part_count);

        for part_number in 1..=part_count {
            // Calculate the exact payload size expected for this specific chunk
            let is_last_part = part_number == part_count;
            let part_size = if is_last_part && total_size > 0 {
                // The final chunk absorbs the remainder of the bytes
                total_size - (chunk_size * (part_count as u64 - 1))
            } else if total_size == 0 {
                0
            } else {
                chunk_size
            };

            let url = crate::util::retry::with_retry(
                || {
                    let client = client.clone();
                    let config = presigning_config.clone();
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let upload_id = upload_id.clone();

                    // S3 part numbers are strictly 1-indexed
                    let part_num = part_number as i32;
                    let p_size = part_size as i64; // AWS SDK expects i64 for content_length

                    async move {
                        let presigned_req = client
                            .upload_part()
                            .bucket(&bucket)
                            .key(&key)
                            .upload_id(&upload_id)
                            .part_number(part_num)
                            .content_length(p_size) // Explicitly bind and sign the exact chunk size
                            .presigned(config)
                            .await
                            .ctx("Failed to generate presigned PUT URL for part")?;

                        Ok(presigned_req.uri().to_string())
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            urls.push(url);
        }

        debug!(
            parts = urls.len(),
            %upload_id,
            "Presigned multipart upload URLs successfully generated and signed"
        );

        Ok((upload_id, urls))
    }

    /// Prematurely terminates an active S3 multipart upload via explicit abort.
    #[instrument(skip(self), fields(bucket = %self.bucket, key = %self.key), err)]
    pub async fn abort_presigned_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = upload_id.to_string();

        crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let upload_id = upload_id.clone();
                async move {
                    client
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .send()
                        .await
                        .ctx("Failed to abort multipart upload")?;
                    Ok(())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        info!(%upload_id, "Multipart upload aborted");
        Ok(())
    }

    /// Evaluates collected `ETags` and seals the completed multipart sequence on S3.
    #[instrument(skip(self, etags), fields(bucket = %self.bucket, key = %self.key), err)]
    pub async fn complete_presigned_multipart_upload(
        &self,
        upload_id: impl Into<String> + Debug,
        etags: Vec<(i32, String)>,
    ) -> Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = upload_id.into();

        crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let upload_id = upload_id.clone();
                let etags = etags.clone();
                async move {
                    let completed_parts = etags
                        .iter()
                        .map(|(part_num, etag)| {
                            aws_sdk_s3::types::CompletedPart::builder()
                                .e_tag(etag)
                                .part_number(*part_num)
                                .build()
                        })
                        .collect::<Vec<_>>();

                    let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(completed_parts))
                        .build();

                    client
                        .complete_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .multipart_upload(completed_upload)
                        .send()
                        .await
                        .ctx("Failed to complete multipart upload")?;
                    Ok(())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        info!(%upload_id, "Multipart upload completed securely");
        Ok(())
    }

    /// Fetches S3 object keys with optional prefix and pagination bounds.
    #[instrument(skip(self), fields(bucket = %self.bucket), err)]
    pub async fn list(
        &self,
        page_size: Option<i32>,
        prefix: Option<&str>,
        max_pages: Option<usize>,
    ) -> Result<S3ListResult> {
        let mut result = S3ListResult::default();
        let mut pages_fetched = 0;
        let mut continuation_token: Option<String> = None;

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix_str = prefix.map(std::string::ToString::to_string);

        loop {
            if self.cancel_token.is_cancelled() {
                return Err(Error::Cancelled);
            }

            let page = crate::util::retry::with_retry(
                || {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let p_str = prefix_str.clone();
                    let token = continuation_token.clone();
                    async move {
                        let mut req = client.list_objects_v2().bucket(&bucket);
                        if let Some(p) = p_str {
                            req = req.prefix(p);
                        }
                        if let Some(t) = token {
                            req = req.continuation_token(t);
                        }

                        req.max_keys(page_size.unwrap_or(1000))
                            .send()
                            .await
                            .ctx("Failed to fetch S3 objects page")
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            for object in page.contents() {
                if let Some(k) = object.key() {
                    result.keys.push(k.to_string());
                }
            }

            pages_fetched += 1;
            continuation_token = page
                .next_continuation_token()
                .map(std::string::ToString::to_string);

            if let Some(limit) = max_pages
                && pages_fetched >= limit
            {
                result.has_more = continuation_token.is_some();
                break;
            }

            if continuation_token.is_none() {
                break;
            }
        }

        info!(
            keys_fetched = result.keys.len(),
            has_more = result.has_more,
            "List operation finished"
        );
        Ok(result)
    }

    pub async fn delete_object(&self) -> Result<()> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                async move {
                    client
                        .delete_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                        .ctx("Failed to delete object")?;
                    Ok(())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        info!("Object deleted successfully");
        Ok(())
    }
}

/// The resultant payload schema from iterating S3 collections.
#[derive(Debug, Default, Clone)]
pub struct S3ListResult {
    /// The extracted file keys.
    pub keys: Vec<String>,
    /// Flags whether pagination forcibly terminated prior to total bucket enumeration.
    pub has_more: bool,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ObjectOperation for SdkOperation {
    fn with_range(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Sends a HEAD dispatch to ascertain absolute object byte capacities prior to streaming.
    #[instrument(skip(self), fields(bucket = %self.bucket, key = %self.key), err)]
    async fn get_size(&mut self) -> Result<u64> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        let size = crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                async move {
                    let head_res = client
                        .head_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                        .ctx("Failed to send HEAD request")?;

                    let s = head_res
                        .content_length()
                        .ctx("HEAD response missing Content-Length")?
                        .try_into()
                        .ctx("Content-Length value is invalid")?;
                    Ok(s)
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        debug!(size, "Acquired object size");
        Ok(size)
    }

    #[instrument(skip(self, body), fields(bucket = %self.bucket, key = %self.key), err)]
    async fn put<I>(&mut self, body: I) -> Result<Vec<(i32, String)>>
    where
        I: Into<S3Payload> + MaybeSend + 'static,
    {
        let _guard = ProgressGuard(self.progress.clone());
        let mut byte_stream = body.into();

        let mut current_chunk = bytes::BytesMut::with_capacity(self.multipart_chunk_size as usize);
        let exact_chunk_size = self.multipart_chunk_size as usize;

        info!("Filling lookahead buffer to determine upload strategy");
        let mut stream_exhausted = false;
        loop {
            let chunk_opt = tokio::select! {
                res = byte_stream.try_next() => res.ctx("Failed to read slice from bounded ByteStream")?,
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            };

            if let Some(chunk_res) = chunk_opt {
                current_chunk.extend_from_slice(&chunk_res);
                if current_chunk.len() >= exact_chunk_size {
                    break; // Lookahead buffer full, transition to multipart
                }
            } else {
                stream_exhausted = true;
                break; // Stream ended before hitting the chunk size
            }
        }

        // --- Single-part Pivot ---
        if stream_exhausted {
            info!("Payload within chunk limits, executing standard PUT upload");
            let payload = current_chunk.freeze();
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let progress = self.progress.clone();

            let etag = crate::util::retry::with_retry(
                || {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let payload = payload.clone();
                    let prog = progress.clone();

                    async move {
                        let mut request = client
                            .put_object()
                            .bucket(&bucket)
                            .key(&key)
                            .body(aws_sdk_s3::primitives::ByteStream::from(payload))
                            .customize();

                        if let Some(p) = prog {
                            let interceptor = ProgressInterceptor { progress: Some(p) };
                            request = request.interceptor(interceptor);
                        }

                        let res = request.send().await.ctx("Upload operation failed")?;
                        Ok(res.e_tag().unwrap_or("").to_string())
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            debug!("Standard PUT upload complete");
            return Ok(vec![(1, etag)]);
        }

        // --- Multipart Pivot ---
        info!("Payload exceeds chunk bounds, creating multipart upload sequence");
        let mut etags = Vec::new();
        let mut upload_tasks = futures::stream::FuturesUnordered::new();
        let mut current_part_number = 1;

        let client_c = self.client.clone();
        let bucket_c = self.bucket.clone();
        let key_c = self.key.clone();

        let upload_id = crate::util::retry::with_retry(
            || {
                let client = client_c.clone();
                let bucket = bucket_c.clone();
                let key = key_c.clone();
                async move {
                    let create_res = client
                        .create_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                        .ctx("Failed to create multipart upload initialization context")?;
                    Ok(create_res
                        .upload_id()
                        .ctx("S3 did not return a valid UploadId")?
                        .to_string())
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await?;

        // Process the first overflowing chunk from the lookahead buffer
        let payload = current_chunk.split_to(exact_chunk_size).freeze();
        upload_tasks.push(tokio::spawn(upload_part_sdk(UploadPartContext {
            client: self.client.clone(),
            bucket: self.bucket().to_string(),
            key: self.key().to_string(),
            upload_id: upload_id.clone(),
            part_number: current_part_number,
            payload,
            progress: self.progress.clone(),
            retry_config: self.retry_config.clone(),
            cancel_token: self.cancel_token.clone(),
        })));
        current_part_number += 1;

        // Continue consuming the stream
        loop {
            while current_chunk.len() >= exact_chunk_size {
                let payload = current_chunk.split_to(exact_chunk_size).freeze();

                upload_tasks.push(tokio::spawn(upload_part_sdk(UploadPartContext {
                    client: self.client.clone(),
                    bucket: self.bucket().to_string(),
                    key: self.key().to_string(),
                    upload_id: upload_id.clone(),
                    part_number: current_part_number,
                    payload,
                    progress: self.progress.clone(),
                    retry_config: self.retry_config.clone(),
                    cancel_token: self.cancel_token.clone(),
                })));

                current_part_number += 1;

                while upload_tasks.len() >= self.multipart_concurrency {
                    tokio::select! {
                        res = upload_tasks.next() => {
                            if let Some(res) = res {
                                let task_result = res.ctx("Task panic")?;
                                match task_result {
                                    Ok((part_num, etag)) => etags.push((part_num, etag)),
                                    Err(e) => {
                                        let abort_client = self.client.clone();
                                        let abort_bucket = self.bucket.clone();
                                        let abort_key = self.key.clone();
                                        let abort_id = upload_id.clone();

                                        tokio::spawn(async move {
                                            let _ = abort_client
                                                .abort_multipart_upload()
                                                .bucket(abort_bucket)
                                                .key(abort_key)
                                                .upload_id(abort_id)
                                                .send()
                                                .await;
                                        });

                                        return Err(e);
                                    }
                                }
                            }
                        },
                        () = self.cancel_token.cancelled() => {
                            let abort_client = self.client.clone();
                            let abort_bucket = self.bucket.clone();
                            let abort_key = self.key.clone();
                            let abort_id = upload_id.clone();

                            // Fire-and-forget the cleanup task so we can return immediately
                            tokio::spawn(async move {
                                let _ = abort_client
                                    .abort_multipart_upload()
                                    .bucket(abort_bucket)
                                    .key(abort_key)
                                    .upload_id(abort_id)
                                    .send()
                                    .await;
                            });

                            return Err(Error::Cancelled);
                        }
                    }
                }
            }

            let chunk_opt = tokio::select! {
                res = byte_stream.try_next() => res.ctx("Failed to read slice from bounded ByteStream")?,
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            };

            let Some(chunk_res) = chunk_opt else { break };
            current_chunk.extend_from_slice(&chunk_res);
        }

        if !current_chunk.is_empty() {
            upload_tasks.push(tokio::spawn(upload_part_sdk(UploadPartContext {
                client: self.client.clone(),
                bucket: self.bucket().to_string(),
                key: self.key().to_string(),
                upload_id: upload_id.clone(),
                part_number: current_part_number,
                payload: current_chunk.freeze(),
                progress: self.progress.clone(),
                retry_config: self.retry_config.clone(),
                cancel_token: self.cancel_token.clone(),
            })));
        }

        while !upload_tasks.is_empty() {
            tokio::select! {
                res = upload_tasks.next() => {
                    if let Some(res) = res {
                        let (part_num, etag) = res.ctx("Task panic")??;
                        etags.push((part_num, etag));
                    }
                },
                () = self.cancel_token.cancelled() => {
                    let abort_client = self.client.clone();
                    let abort_bucket = self.bucket.clone();
                    let abort_key = self.key.clone();
                    let abort_id = upload_id.clone();

                    // Fire-and-forget the cleanup task so we can return immediately
                    tokio::spawn(async move {
                        let _ = abort_client
                            .abort_multipart_upload()
                            .bucket(abort_bucket)
                            .key(abort_key)
                            .upload_id(abort_id)
                            .send()
                            .await;
                    });

                    return Err(Error::Cancelled);
                }
            }
        }

        etags.sort_by_key(|k| k.0);

        info!("Sending complete multipart upload directive");
        self.complete_presigned_multipart_upload(&upload_id, etags.clone())
            .await?;

        Ok(etags)
    }

    #[instrument(skip(self, writer), fields(bucket = %self.bucket, key = %self.key), err)]
    async fn get<W>(&mut self, writer: &mut W, total_size: Option<u64>) -> Result<S3Return>
    where
        W: S3Writer,
    {
        let guard = ProgressGuard(self.progress.clone());
        let chunk_size = self.multipart_chunk_size;

        if let Some(range) = &self.range {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let range_str = range.to_string();

            let mut output = crate::util::retry::with_retry(
                || {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let range_str = range_str.clone();
                    async move {
                        client
                            .get_object()
                            .bucket(&bucket)
                            .key(&key)
                            .range(&range_str)
                            .send()
                            .await
                            .ctx("Failed to get object range from S3")
                    }
                },
                &self.retry_config,
                &self.cancel_token,
            )
            .await?;

            // Parse the object size from the AWS SDK's content_range
            let object_size =
                crate::util::range::parse_object_size_from_content_range(output.content_range());

            if let Some(len) = output.content_length() {
                writer.size_hint(len.try_into().unwrap_or(0)).await?;
            }

            let mut total_written = 0;
            while let Some(chunk) = output
                .body
                .try_next()
                .await
                .ctx("Failed to read stream chunk")?
            {
                guard.inc(chunk.len() as u64);
                writer
                    .write_all(&chunk)
                    .await
                    .ctx("Failed to write chunk sequentially")?;
                total_written += chunk.len() as u64;
            }

            return Ok(S3Return {
                read_length: total_written,
                object_size,
            });
        }

        // --- FIRST-CHUNK PROBE ---
        let end_byte = chunk_size.saturating_sub(1);
        let probe_range = format!("bytes=0-{end_byte}");

        info!("Initiating First-Chunk Probe GET request");

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        // 1. Capture the Result instead of immediately unwrapping it
        let first_output_result = crate::util::retry::with_retry(
            || {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let probe = probe_range.clone();
                async move {
                    client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .range(probe)
                        .send()
                        .await
                        .ctx("Failed to fetch initial chunk from S3")
                }
            },
            &self.retry_config,
            &self.cancel_token,
        )
        .await;

        // 2. Evaluate the result and fallback if S3 rejects the range
        let mut first_output = match first_output_result {
            Ok(output) => output,
            // Use debug formatting `{:?}` to expose the inner `source` chain
            Err(e) if format!("{e:?}").contains("InvalidRange") => {
                debug!(
                    "Probe rejected with InvalidRange (likely 0-byte file). Falling back to rangeless GET."
                );
                crate::util::retry::with_retry(
                    || {
                        let client = client.clone();
                        let bucket = bucket.clone();
                        let key = key.clone();
                        async move {
                            client
                                .get_object()
                                .bucket(&bucket)
                                .key(&key)
                                .send()
                                .await
                                .ctx("Failed to fetch empty/tiny object from S3")
                        }
                    },
                    &self.retry_config,
                    &self.cancel_token,
                )
                .await?
            }
            Err(e) => return Err(e), // Bubble up unrelated errors
        };

        // Extract the absolute total size of the file from the Content-Range header
        let parsed_total =
            crate::util::range::parse_object_size_from_content_range(first_output.content_range());
        let actual_size = total_size
            .or(parsed_total)
            .unwrap_or_else(|| first_output.content_length().unwrap_or(0) as u64);

        if actual_size > 0 {
            writer.size_hint(actual_size).await?;
        }

        // Consume the first chunk
        let mut total_written = 0;
        while let Some(chunk) = first_output
            .body
            .try_next()
            .await
            .ctx("Failed to read initial stream chunk")?
        {
            guard.inc(chunk.len() as u64);
            writer
                .write_all(&chunk)
                .await
                .ctx("Failed to write initial chunk")?;
            total_written += chunk.len() as u64;
        }

        // If the entire file fit inside our first chunk, we are completely done!
        if actual_size <= chunk_size {
            debug!("File fit entirely in initial chunk");
            return Ok(S3Return {
                read_length: total_written,
                object_size: Some(actual_size),
            });
        }

        // --- MULTIPART CONCURRENT FETCH ---
        info!("File exceeds chunk size, pivoting to concurrent multipart GETs");
        let client = self.client.clone();
        let bucket = self.bucket().to_string();
        let key = self.key().to_string();
        let progress = self.progress.clone();
        let retry_cfg = self.retry_config.clone();
        let cancel_tok = self.cancel_token.clone();

        let range_tasks = (chunk_size..actual_size)
            .step_by(chunk_size as usize)
            .map(|start| {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let progress = progress.clone();
                let retry_cfg = retry_cfg.clone();
                let cancel_tok = cancel_tok.clone();

                async move {
                    let end = std::cmp::min(start + chunk_size - 1, actual_size - 1);
                    let range_header = format!("bytes={start}-{end}");

                    crate::util::retry::with_retry(
                        || {
                            let client = client.clone();
                            let bucket = bucket.clone();
                            let key = key.clone();
                            let range_str = range_header.clone();
                            let prog = progress.clone();

                            async move {
                                trace!(range = %range_str, "Requesting multipart range slice");

                                let mut output = client
                                    .get_object()
                                    .bucket(&bucket)
                                    .key(&key)
                                    .range(&range_str)
                                    .send()
                                    .await
                                    .ctx("Network bounds request failure")?;

                                let mut chunk_bytes =
                                    bytes::BytesMut::with_capacity((end - start + 1) as usize);
                                while let Some(chunk) = output
                                    .body
                                    .try_next()
                                    .await
                                    .ctx("Network chunk fetch failure")?
                                {
                                    if let Some(pb) = &prog {
                                        pb.inc(chunk.len() as u64);
                                    }
                                    chunk_bytes.extend_from_slice(&chunk);
                                }
                                Ok::<bytes::Bytes, Error>(chunk_bytes.freeze())
                            }
                        },
                        &retry_cfg,
                        &cancel_tok,
                    )
                    .await
                }
            });

        let mut buffered_stream =
            futures::stream::iter(range_tasks).buffered(self.multipart_concurrency);

        loop {
            let chunk_opt = tokio::select! {
                res = buffered_stream.next() => res,
                () = self.cancel_token.cancelled() => return Err(Error::Cancelled),
            };

            let Some(chunk_result) = chunk_opt else { break };
            let chunk = chunk_result?;

            writer
                .write_all(&chunk)
                .await
                .ctx("Failed to write multipart payload segment")?;
            total_written += chunk.len() as u64;
        }

        debug!("Multipart GET stream fulfilled");
        Ok(S3Return {
            read_length: total_written,
            object_size: Some(actual_size),
        })
    }
}

/// Encapsulates the contextual parameters required to dispatch an isolated multipart chunk.
/// Grouped to prevent function signature bloating and to cleanly transfer ownership to Tokio tasks.
pub(crate) struct UploadPartContext {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
    pub payload: bytes::Bytes,
    pub progress: Option<atomic_progress::Progress>,
    pub retry_config: RetryConfig,
    pub cancel_token: CancellationToken,
}

/// Internal helper mapping pure components to an isolated, monitored AWS segment operation
/// wrapped with an explicit retry boundary.
#[instrument(skip(ctx), fields(part = ctx.part_number, size = ctx.payload.len()), err)]
async fn upload_part_sdk(ctx: UploadPartContext) -> Result<(i32, String)> {
    crate::util::retry::with_retry(
        || {
            let client = ctx.client.clone();
            let bucket = ctx.bucket.clone();
            let key = ctx.key.clone();
            let upload_id = ctx.upload_id.clone();
            let payload = ctx.payload.clone();
            let prog = ctx.progress.clone();
            let part_number = ctx.part_number;

            async move {
                let mut req = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(aws_sdk_s3::primitives::ByteStream::from(payload))
                    .customize();

                if let Some(p) = prog {
                    req = req.interceptor(ProgressInterceptor { progress: Some(p) });
                }

                let resp = req
                    .send()
                    .await
                    .ctx("Failed to upload segment against S3 target endpoint")?;

                let etag = resp
                    .e_tag()
                    .ctx("Missing ETag from upload_part response headers")?
                    .to_string();

                Ok(etag)
            }
        },
        &ctx.retry_config,
        &ctx.cancel_token,
    )
    .await
    .map(|etag| {
        trace!(%etag, part_number = ctx.part_number, "Segment committed to S3");
        (ctx.part_number, etag)
    })
}
