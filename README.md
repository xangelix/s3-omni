# s3-omni

[![Crates.io](https://img.shields.io/crates/v/s3-omni)](https://crates.io/crates/s3-omni)
[![Docs.rs](https://docs.rs/s3-omni/badge.svg)](https://docs.rs/s3-omni)
[![License](https://img.shields.io/crates/l/s3-omni)](https://spdx.org/licenses/MIT)

An advanced, dual-target (Native + WebAssembly) S3 client built for maximum throughput, concurrent multipart streaming, and seamless pre-signed URL handling.

## 📖 Overview

Interacting with S3-compatible storage (like AWS S3 or Cloudflare R2) is annoyingly complex. Implementing concurrent multipart uploads, handling exponential network backoffs, and tracking progress is difficult on a native backend. Compiling that same logic to run in a browser via WebAssembly (`wasm32-unknown-unknown`)—where you are restricted to `fetch` and face strict chunking limitations—is often a nightmare.

`s3-omni` solves this by providing a unified, battle-tested `ObjectOperation` trait abstracting two powerful backends:

1. **`SdkClient`**: A heavy, native-only factory wrapping the official `aws-sdk-s3` crate for maximum server-side performance.
2. **`ReqwestClient`**: A lightweight, universal client that uses HTTP `reqwest` to interact with S3 purely via pre-signed URLs, compiling flawlessly to both native and WASM targets.

## ✨ Features

- 🌐 **True WASM Compatibility**: Bypass browser 501 chunking rejections and stream directly to S3 from a web frontend.
- 🚀 **Smart Multipart Concurrency**: Automatically pivots to parallel, multi-connection chunked uploads/downloads when payloads exceed your configured boundaries.
- 🏭 **Zero-Cost Connection Pooling**: Built around a strict Factory/Context pattern. Instantiate your client once and effortlessly spawn 10,000 concurrent `'static` upload tasks without mutex bottlenecks.
- 🔐 **Pre-signed URL Mastery**: Seamlessly generate pre-signed URLs on your backend (`SdkClient`) and consume them safely on your frontend (`ReqwestClient`), even for complex 50-part concurrent uploads.
- ♻️ **Battle-Tested Resilience**: Native exponential backoff with full 64-bit jitter protects your application from transient network drops and 429/50x S3 rate limits.
- 📊 **Progress Tracking**: Inject atomic progress bars directly into the byte streams to monitor massive download **and** upload transfers in real-time.

## 📦 Installation

To add `s3-omni` to your project, run:

```bash
cargo add s3-omni
```

### Features

By default, `s3-omni` enables both the `sdk` and `reqwest` features. You should tailor this depending on your target environment to drastically reduce compile times and binary sizes.

- **`sdk`**: Enables the `SdkClient` and its dependencies (`aws-sdk-s3`, `tokio`). _Native only._
- **`reqwest`**: Enables the `ReqwestClient` and its dependencies. _Native and WASM compatible._

**Backend Example:**

```toml
[dependencies]
s3-omni = { version = "X.X.X", default-features = false, features = ["sdk"] }
```

**Frontend (WASM) Example:**

```toml
[dependencies]
s3-omni = { version = "X.X.X", default-features = false, features = ["reqwest"] }
```

## 🚀 Usage

### 1. High-Performance Backend Uploads (`SdkClient`)

The `SdkClient` acts as a connection factory. You clone it to create lightweight `SdkOperation` contexts, allowing you to spawn highly concurrent, `'static` Tokio tasks that all share a single underlying HTTP connection pool.

```rust
use s3_omni::{SdkClient, ObjectOperation};
use bytes::Bytes;

#[tokio::main]
async fn main() {
    // 1. Create the global factory ONCE. This initializes the connection pool.
    let factory = SdkClient::new(
        "https://<account_id>.r2.cloudflarestorage.com",
        "ACCESS_KEY",
        "SECRET_KEY"
    ).await.unwrap();

    // 2. Create an execution context for a specific file.
    //    The factory is cloned cheaply (Arc under the hood).
    let mut upload_op = factory
        .with_multipart_chunk_size(10 * 1024 * 1024) // 10MB chunks
        .with_multipart_concurrency(4)               // 4 concurrent connections
        .op("my-bucket", "user-uploads/video.mp4")
        .await
        .unwrap();

    // 3. Execute the upload!
    //    If the file is > 10MB, it automatically parallelizes the upload.
    let payload = Bytes::from(vec![0u8; 15 * 1024 * 1024]);
    let etags = upload_op.put(payload).await.unwrap();

    println!("Upload completed successfully with ETags: {etags:?}");
}
```

### 2. Secure Frontend Uploads via Pre-Signed URLs (`ReqwestClient`)

A common enterprise pattern is keeping AWS credentials strictly on the backend, generating pre-signed URLs, and sending those URLs to a web frontend (or mobile app) to upload the file directly to S3.

`s3-omni` makes this complex multipart dance incredibly simple.

**On your Backend (Native):**

```rust
use std::time::Duration;

use s3_omni::SdkClient;

// Calculate how many chunks your file will need
let file_size: u64 = 50 * 1024 * 1024; // 50MB
let chunk_size: u64 = 5 * 1024 * 1024; // 5MB chunks

let mut op = backend_factory
    .with_presigned_expires_in(Duration::from_hours(1))
    .with_multipart_chunk_size(chunk_size)
    .op("my-bucket", "frontend-upload.zip")
    .await
    .unwrap();

// Generate exactly enough pre-signed URLs for the file size
// In a real app, you return this array of URLs to your frontend via an API.
let download_urls = op.create_presigned_multipart_download(file_size, Duration::from_hours(1)).await.unwrap();
let just_the_urls: Vec<String> = download_urls.into_iter().map(|(_, _, url)| url).collect();
```

**On your Frontend (WebAssembly / Native):**

```rust
use s3_omni::{ReqwestClient, ObjectOperation};
use bytes::Bytes;

// 1. Give the ReqwestClient the array of pre-signed URLs from your backend.
let mut frontend_op = ReqwestClient::new()
    .with_multipart_chunk_size(5 * 1024 * 1024)
    .with_multipart_concurrency(3)
    .op(just_the_urls)
    .unwrap();

// 2. Upload the data.
//    `s3-omni` automatically slices the payload and maps each
//    concurrent chunk to the correct pre-signed URL in the array!
let payload = Bytes::from(vec![0u8; 50 * 1024 * 1024]);
frontend_op.put(payload).await.unwrap();

println!("Direct-to-S3 frontend upload complete!");
```

### 3. Downloading with Pre-Allocation

When downloading massive files, constantly reallocating memory or fragmenting the disk slows everything down. `s3-omni` provides an `S3Writer` trait that triggers a `.size_hint()` _before_ the stream begins.

```rust
use s3_omni::{S3Writer, ObjectOperation, Result};
use tokio::io::AsyncWriteExt as _;

struct MyFileWriter {
    buffer: Vec<u8>
}

#[async_trait::async_trait]
impl S3Writer for MyFileWriter {
    async fn size_hint(&mut self, size: u64) -> Result<()> {
        // Pre-allocate the exact capacity needed before the download begins!
        self.buffer.reserve_exact(size as usize);
        Ok(())
    }
}

// Implement standard AsyncWrite...
// impl tokio::io::AsyncWrite for MyFileWriter { ... }

// Usage:
// let mut writer = MyFileWriter { buffer: Vec::new() };
// client_op.get(&mut writer, None).await.unwrap();
```

## ⚙️ Cancellation & Graceful Shutdown

Every operation in `s3-omni` respects `tokio_util::sync::CancellationToken`.

If an upload is taking too long or the user navigates away, simply trigger `.cancel()` on the token you passed into the builder. `s3-omni` will immediately halt network I/O, safely abort any orphaned S3 multipart sequences in the background, and return an `Error::Cancelled`.

## ⚖️ License

This project is licensed under the [MIT License](https://spdx.org/licenses/MIT).
