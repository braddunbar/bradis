#!/usr/bin/env sh

set -ex

cargo fmt --all -- --check
cargo clippy --locked --all --tests -- -D warnings
cargo clippy --no-default-features --locked --all --tests -- -D warnings
