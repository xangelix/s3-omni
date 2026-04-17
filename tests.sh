#!/usr/bin/env bash

set -e

echo "================================================="
echo "1. Generating Pre-signed URLs via Native SDK..."
echo "================================================="

URL_EXPORTS=$(cargo run --example generate_test_urls --features "sdk" --quiet)
eval "$URL_EXPORTS"

echo "Successfully generated URLs."

echo ""
echo "================================================="
echo "2. Running Native Reqwest & SDK Tests..."
echo "================================================="
# Runs your sdk_tests.rs and the native half of reqwest_tests.rs
cargo test --no-default-features --features "reqwest,sdk"

echo ""
echo "================================================="
echo "3. Running WASM Headless Browser Tests..."
echo "================================================="
# Runs the WASM half of reqwest_tests.rs
wasm-pack test --headless --chrome -- --no-default-features --features "reqwest"

echo "================================================="
echo "Done!"
