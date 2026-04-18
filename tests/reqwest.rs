// This file only compiles if the reqwest feature is enabled
#[cfg(feature = "reqwest")]
mod reqwest_tests {
    use bytes::Bytes;
    use s3_omni::{ByteRange, ObjectOperation as _, ReqwestClient, S3Writer};

    // Only import and configure WASM utilities when compiling for WASM
    #[cfg(target_family = "wasm")]
    use wasm_bindgen_test::*;
    #[cfg(target_family = "wasm")]
    wasm_bindgen_test_configure!(run_in_browser);

    // --- SHARED MOCK WRITER ---
    struct MemoryWriter {
        pub buffer: Vec<u8>,
    }
    impl MemoryWriter {
        const fn new() -> Self {
            Self { buffer: Vec::new() }
        }
    }
    impl tokio::io::AsyncWrite for MemoryWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.buffer.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    #[async_trait::async_trait]
    impl S3Writer for MemoryWriter {
        async fn size_hint(&mut self, size: u64) -> s3_omni::error::Result<()> {
            self.buffer.reserve(size as usize);
            Ok(())
        }
    }

    // --- UNIVERSAL TESTS ---

    // The compiler automatically swaps the test macro depending on the target architecture!
    #[cfg_attr(not(target_family = "wasm"), tokio::test)]
    #[cfg_attr(target_family = "wasm", wasm_bindgen_test)]
    async fn test_universal_reqwest_put_and_get() {
        // Now using universal env vars from tests.sh
        let put_url = option_env!("TEST_PRESIGNED_PUT")
            .map_or_else(|| panic!("Missing PUT URL (Run via tests.sh)"), |url| url);

        let get_url =
            option_env!("TEST_PRESIGNED_GET").map_or_else(|| panic!("Missing GET URL"), |url| url);

        let start: u64 = option_env!("TEST_RANGE_START")
            .map_or_else(|| panic!("Missing START"), |val| val.parse().unwrap());

        let end: u64 = option_env!("TEST_RANGE_END")
            .map_or_else(|| panic!("Missing END"), |val| val.parse().unwrap());

        let payload = Bytes::from("Hello from Universal Reqwest Client!");

        // 1. PUT
        let mut put_client = ReqwestClient::new().op(put_url.to_string());

        put_client
            .put(payload.clone())
            .await
            .expect("Reqwest PUT failed");

        // 2. GET
        let mut get_client = ReqwestClient::new()
            .with_range(ByteRange::Exact(start, end))
            .op(get_url.to_string());

        let mut writer = MemoryWriter::new();
        let s3_return = get_client
            .get(&mut writer, None)
            .await
            .expect("Reqwest GET failed");

        assert_eq!(s3_return.read_length, payload.len() as u64);
        assert_eq!(writer.buffer, payload.as_ref());
    }
}
