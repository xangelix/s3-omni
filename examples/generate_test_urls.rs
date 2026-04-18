#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() {
    use std::{env, time::Duration};

    use s3_omni::SdkClient;

    let endpoint = env::var("S3_OMNI_ENDPOINT").expect("Missing S3_OMNI_ENDPOINT");
    let bucket = env::var("S3_OMNI_BUCKET").expect("Missing S3_OMNI_BUCKET");

    // Ensure AWS credentials exist
    let access_key = env::var("S3_OMNI_ACCESS_KEY").expect("Missing S3_OMNI_ACCESS_KEY");
    let secret_access_key = env::var("S3_OMNI_SECRET_KEY").expect("Missing S3_OMNI_SECRET_KEY");

    // Randomize the key to prevent test collisions
    let key = format!(
        "wasm-test-{}.txt",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    let payload_size = 36; // Size of our test string

    let sdk_client = SdkClient::new(endpoint, access_key, secret_access_key)
        .await
        .unwrap()
        .with_presigned_expires_in(Duration::from_hours(1))
        .op(bucket, key);

    // 1. Generate the Upload Struct and extract the first part's URL
    let presigned_upload = sdk_client
        .create_presigned_upload(payload_size)
        .await
        .expect("Failed to generate PUT URL");

    let put_url = presigned_upload.parts.into_iter().next().unwrap().url;

    // 2. Generate the Download Struct and extract the first part's URL and bounds
    let presigned_download = sdk_client
        .create_presigned_download(payload_size)
        .await
        .expect("Failed to generate GET URLs");

    let download_part = presigned_download.parts.into_iter().next().unwrap();
    let get_url = download_part.url;
    let start = download_part.start;
    let end = download_part.end;

    // Print strictly the export commands so our bash script can evaluate them
    println!("export TEST_PRESIGNED_PUT='{put_url}'");
    println!("export TEST_PRESIGNED_GET='{get_url}'");
    println!("export TEST_RANGE_START='{start}'");
    println!("export TEST_RANGE_END='{}'", end + 1);
}

#[cfg(target_family = "wasm")]
fn main() {}
