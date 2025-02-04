use miette::Diagnostic;
use nu_protocol::Span;
use respite::{RespError, RespValue};
use std::num::TryFromIntError;
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

    #[error("try from int error")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Expected client to be closed")]
    NotClosed,

    #[error("Key not found: {0:?}")]
    KeyNotFound(String),

    #[error("running only a subset of tests")]
    Only,

    #[error("duplicate map key")]
    DuplicateKey,

    #[error("duplicate set value")]
    DuplicateValue,

    #[error("overflow")]
    Overflow,

    #[error("No client for the current index")]
    MissingClient,
}
