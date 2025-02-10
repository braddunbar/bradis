#!/usr/bin/env sh

set -ex

cargo build --release --locked
cargo build --release --locked --no-default-features
cargo test --quiet --locked
cargo test --quiet --locked --no-default-features
