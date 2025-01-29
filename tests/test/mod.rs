mod client;
mod command;
mod error;
mod nu_test;

pub use client::TestClient;
pub use error::{TestError, TestResult};
pub use nu_test::{run, Test};
use std::time::Duration;

/// How long do we wait before a test times out?
pub static TIMEOUT: Duration = Duration::from_millis(500);
