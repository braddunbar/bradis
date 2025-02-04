use miette::Diagnostic;
use nu_protocol::Span;
use respite::{RespError, RespValue};
use thiserror::Error;

pub type TestResult<T> = Result<T, TestError>;

#[derive(Debug, Diagnostic, Error)]
pub enum TestError {
    #[error(transparent)]
    Resp(#[from] RespError),

    #[error("timed out")]
    Timeout(#[label("here")] Span),

    #[error("the reader is closed")]
    ReaderClosed,

    #[error("the writer is disconnected")]
    WriterDisconnected,

    #[error("invalid info")]
    InvalidInfo,

    #[error("unexpected value")]
    UnexpectedValue(RespValue),

    #[error("running only a subset of tests")]
    Only,

    #[error("No client for the current index")]
    MissingClient,
}
