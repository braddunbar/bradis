#!/usr/bin/env sh

set -ex

cargo fmt --all -- --check
cargo clippy --all --tests -- \
  -D clippy::all \
  -D clippy::dbg_macro \
  -A clippy::enum_glob_use \
  -A clippy::module_name_repetitions \
  -A clippy::missing_errors_doc \
  -A clippy::too_many_lines \
  -D warnings
