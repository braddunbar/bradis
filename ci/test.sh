#!/usr/bin/env sh

set -ex

cargo build --release --locked
cargo test --quiet --locked
