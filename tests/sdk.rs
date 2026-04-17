#[cfg(not(target_family = "wasm"))]
mod tests {
    use std::{
        env,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use rand::{RngExt as _, distr::Alphanumeric};
    use s3_omni::{ObjectOperation as _, ReqwestClient, S3Writer, SdkClient};
    use tokio::io::AsyncWrite;
    use tokio_util::sync::CancellationToken;

    use s3_omni::{ByteRange, Result};

    // --- 1. MOCK S3 WRITER ---

    /// A simple in-memory writer to test downloads without hitting the disk.
    struct InMemoryWriter {
        pub buffer: Vec<u8>,
    }

    impl InMemoryWriter {
        const fn new() -> Self {
            Self { buffer: Vec::new() }
        }
    }

    impl AsyncWrite for InMemoryWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.buffer.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[async_trait]
    impl S3Writer for InMemoryWriter {
        async fn size_hint(&mut self, size: u64) -> Result<()> {
            self.buffer.reserve(size as usize);
            Ok(())
        }
    }

    // --- 2. TEST HELPERS ---

    /// Generates a random file key to prevent test collisions.
    fn generate_random_key() -> String {
        let rng = rand::rng();
        let suffix: String = rng
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        format!("integration-test-{suffix}.txt")
    }

    /// Fetches required environment variables, skipping tests if absent.
    fn get_env_vars() -> Option<(String, String, String, String)> {
        let endpoint = env::var("S3_OMNI_ENDPOINT").ok()?;
        let bucket = env::var("S3_OMNI_BUCKET").ok()?;

        let access_key = env::var("S3_OMNI_ACCESS_KEY").ok()?;
        let secret_key = env::var("S3_OMNI_SECRET_KEY").ok()?;

        Some((endpoint, bucket, access_key, secret_key))
    }

    // --- 3. EXISTING CORE TESTS ---

    #[tokio::test]
    async fn test_sdk_client_put_and_get() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            println!("Skipping test: Missing required environment variables.");
            return;
        };

        let key = generate_random_key();
        let payload_data = Bytes::from("Hello from SdkClient Cloudflare R2 test!");

        let mut client = SdkClient::new(endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(bucket, key);

        client
            .put(payload_data.clone())
            .await
            .expect("SDK put_object failed");

        let size = client.get_size().await.expect("SDK get_object_size failed");
        assert_eq!(size, payload_data.len() as u64);

        let mut writer = InMemoryWriter::new();
        let s3_return = client
            .get(&mut writer, None)
            .await
            .expect("SDK get_object failed");

        assert_eq!(s3_return.read_length, payload_data.len() as u64);
        assert_eq!(writer.buffer, payload_data.as_ref());
    }

    #[tokio::test]
    async fn test_reqwest_client_presigned_flow() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();
        let payload_data = Bytes::from("Hello from ReqwestClient pre-signed test!");

        let sdk_client = SdkClient::new(endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_presigned_expires_in(Duration::from_hours(1))
            .op(bucket, key);

        let presigned_put_url = sdk_client
            .create_presigned_upload(payload_data.len() as u64)
            .await
            .expect("Failed to create presigned PUT url");

        let mut reqwest_client = ReqwestClient::new().op(vec![presigned_put_url]);

        let payload_len = payload_data.len() as u64;
        reqwest_client
            .put(payload_data)
            .await
            .expect("Reqwest put_object failed");

        let download_urls = sdk_client
            .create_presigned_multipart_download(payload_len, Duration::from_hours(1))
            .await
            .expect("Failed to create presigned download urls");

        let (start, end, presigned_get_url) = download_urls[0].clone();

        let mut reqwest_download_client = ReqwestClient::new()
            .with_range(ByteRange::Exact(start, end + 1))
            .op(vec![presigned_get_url]);

        let mut writer = InMemoryWriter::new();
        let s3_return = reqwest_download_client
            .get(&mut writer, None)
            .await
            .expect("Reqwest get_object failed");

        assert_eq!(s3_return.read_length, payload_len);
        assert_eq!(
            writer.buffer,
            "Hello from ReqwestClient pre-signed test!".as_bytes()
        );
    }

    #[tokio::test]
    async fn test_sdk_client_multipart_flow() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let payload_size = 15 * 1024 * 1024;
        let payload_data: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
        let payload_bytes = Bytes::from(payload_data);

        let mut client = SdkClient::new(endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_multipart_chunk_size(5 * 1024 * 1024)
            .with_multipart_concurrency(2)
            .op(bucket, key);

        let etags = client
            .put(payload_bytes.clone())
            .await
            .expect("SDK unified put_object failed");

        assert_eq!(etags.len(), 3, "Expected exactly 3 multipart chunks");

        let size = client.get_size().await.expect("SDK get_object_size failed");
        assert_eq!(size, payload_size as u64);

        let mut writer = InMemoryWriter::new();
        let s3_return = client
            .get(&mut writer, Some(size))
            .await
            .expect("SDK unified get_object failed");

        assert_eq!(s3_return.read_length, payload_size as u64);
        assert_eq!(writer.buffer.len(), payload_size);
    }

    // --- 4. NEW THOROUGH INTEGRATION TESTS ---

    #[tokio::test]
    async fn test_sdk_client_empty_payload() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let mut client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(bucket, key);

        // 0-byte payload
        client
            .put(Bytes::new())
            .await
            .expect("Upload of 0 bytes failed");

        let size = client.get_size().await.unwrap();
        assert_eq!(size, 0, "Object size should be 0");

        let mut writer = InMemoryWriter::new();
        let res = client
            .get(&mut writer, None)
            .await
            .expect("Download of 0 bytes failed");

        assert_eq!(res.read_length, 0);
        assert!(writer.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_sdk_client_delete_object() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let mut client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(bucket, key);

        client.put(Bytes::from("Delete me")).await.unwrap();

        // Assert it exists
        assert_eq!(client.get_size().await.unwrap(), 9);

        // Delete it
        client
            .delete_object()
            .await
            .expect("Failed to delete object");

        // Verify absence
        let mut writer = InMemoryWriter::new();
        let err = client.get(&mut writer, None).await.unwrap_err();
        assert!(
            matches!(err, s3_omni::error::Error::Context { .. }),
            "Expected context error on 404 GET"
        );
    }

    #[tokio::test]
    async fn test_sdk_client_list_pagination() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let prefix = format!("list-page-test-{}", generate_random_key());

        let client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(&bucket, format!("{prefix}-init"));

        // Upload 3 items sequentially
        for i in 1..=3 {
            let c = client.aws_client().clone();
            c.put_object()
                .bucket(&bucket)
                .key(format!("{prefix}-{i}.txt"))
                .body(aws_sdk_s3::primitives::ByteStream::from(Bytes::from(
                    "data",
                )))
                .send()
                .await
                .unwrap();
        }

        // Test Paginator limits: page size 2, max pages 1 -> exactly 2 items returned, has_more = true
        let list_result = client
            .list(Some(2), Some(&prefix), Some(1))
            .await
            .expect("List failed");

        assert_eq!(
            list_result.keys.len(),
            2,
            "Expected exactly 2 keys from max_pages = 1 bound"
        );
        assert!(
            list_result.has_more,
            "Expected has_more to be true since 1 item remains"
        );
    }

    #[tokio::test]
    async fn test_sdk_client_range_suffix_get() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let mut base_client = SdkClient::new(&endpoint, &access_key, &secret_key)
            .await
            .unwrap()
            .op(&bucket, &key);

        base_client.put(Bytes::from("0123456789")).await.unwrap();

        let mut range_client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_range(ByteRange::Suffix(3)) // Grab the last 3 bytes
            .op(bucket, key);

        let mut writer = InMemoryWriter::new();
        let res = range_client.get(&mut writer, None).await.unwrap();

        assert_eq!(res.read_length, 3);
        assert_eq!(writer.buffer, b"789");
    }

    #[tokio::test]
    async fn test_sdk_client_range_from_get() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let mut base_client = SdkClient::new(&endpoint, &access_key, &secret_key)
            .await
            .unwrap()
            .op(&bucket, &key);

        base_client.put(Bytes::from("0123456789")).await.unwrap();

        let mut range_client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_range(ByteRange::From(4)) // Grab bytes 4 through end
            .op(bucket, key);

        let mut writer = InMemoryWriter::new();
        let res = range_client.get(&mut writer, None).await.unwrap();

        assert_eq!(res.read_length, 6); // 10 total - 4 start offset
        assert_eq!(writer.buffer, b"456789");
    }

    #[tokio::test]
    async fn test_sdk_client_immediate_cancellation() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let cancel_token = CancellationToken::new();
        cancel_token.cancel(); // Abort before dispatch

        let mut client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_cancel_token(cancel_token)
            .op(bucket, generate_random_key());

        let err = client.put(Bytes::from("Abort me")).await.unwrap_err();
        assert!(
            matches!(err, s3_omni::error::Error::Cancelled),
            "Expected immediate cancellation error"
        );
    }

    #[tokio::test]
    async fn test_sdk_client_midflight_upload_cancellation() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let cancel_token = CancellationToken::new();
        let ct_clone = cancel_token.clone();

        let mut client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .with_cancel_token(cancel_token)
            .with_multipart_chunk_size(5 * 1024 * 1024)
            .op(bucket, generate_random_key());

        // Spawn a thread to cancel the token slightly after the upload starts buffering
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            ct_clone.cancel();
        });

        // 15MB payload to guarantee it pivots to the multipart async pool yielding to tokio time
        let payload = Bytes::from(vec![0u8; 15 * 1024 * 1024]);
        let err = client.put(payload).await.unwrap_err();
        assert!(
            matches!(err, s3_omni::error::Error::Cancelled),
            "Expected mid-flight cancellation error"
        );
    }

    #[tokio::test]
    async fn test_reqwest_client_immediate_cancellation() {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel(); // Abort immediately

        let mut req_client = ReqwestClient::new()
            .with_cancel_token(cancel_token)
            .op(vec!["https://fake-s3-endpoint.com/presigned".to_string()]);

        let err = req_client.put(Bytes::from("Data")).await.unwrap_err();
        assert!(matches!(err, s3_omni::error::Error::Cancelled));
    }

    #[tokio::test]
    async fn test_reqwest_client_multipart_download() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();

        let payload_size = 10 * 1024 * 1024; // 10MB
        let payload_data: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();

        let mut base_client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(bucket, key);

        base_client
            .put(Bytes::from(payload_data.clone()))
            .await
            .unwrap();

        // Map the payload to multiple presigned parts (e.g. 5MB chunks)
        let parts = base_client
            .create_presigned_multipart_download(payload_size as u64, Duration::from_hours(1))
            .await
            .unwrap();
        assert!(
            parts.len() >= 2,
            "Expected multiple parts to test reqwest concurrent fetching"
        );

        let urls: Vec<String> = parts.into_iter().map(|(_, _, url)| url).collect();

        let mut req_client = ReqwestClient::new().with_multipart_concurrency(2).op(urls);

        let mut writer = InMemoryWriter::new();
        let res = req_client
            .get(&mut writer, Some(payload_size as u64))
            .await
            .unwrap();

        assert_eq!(res.read_length, payload_size as u64);
        assert_eq!(writer.buffer.len(), payload_size);
        assert_eq!(
            &writer.buffer[0..100],
            &payload_data[0..100],
            "Integrity check failed"
        );
    }

    #[tokio::test]
    async fn test_sdk_client_progress_tracking_no_panic() {
        let Some((endpoint, bucket, access_key, secret_key)) = get_env_vars() else {
            return;
        };
        let key = generate_random_key();
        let progress = atomic_progress::Progress::new(
            atomic_progress::ProgressType::Bar,
            "Test Progress",
            100u64,
        );

        let mut client = SdkClient::new(&endpoint, access_key, secret_key)
            .await
            .unwrap()
            .op(bucket, key)
            .with_progress(progress.clone());

        let payload = Bytes::from("Testing progress injection");
        client
            .put(payload)
            .await
            .expect("Upload with progress failed");

        // Ensure no internal interceptor panics crashed the sequence
        let mut writer = InMemoryWriter::new();
        client
            .get(&mut writer, None)
            .await
            .expect("Download with progress failed");
    }
}
