use bytes::Bytes;
use miette::{Diagnostic, SourceSpan};
use respite::{RespError, RespValue};
use std::{
    num::{ParseIntError, TryFromIntError},
    ops::RangeInclusive,
    str::Utf8Error,
};
use thiserror::Error;
use tokio::time::error::Elapsed;

pub type TestResult<T> = Result<T, TestError>;

#[derive(Debug, Diagnostic, Error)]
pub enum TestError {
    #[error(transparent)]
    Resp(#[from] RespError),

    #[error(transparent)]
    Elapsed(#[from] Elapsed),

    #[error("invalid node name")]
    InvalidNode(#[label("invalid")] SourceSpan),

    #[error("invalid test command")]
    InvalidCommand(#[label("this command")] SourceSpan),

    #[error("expected {0:?}\ngot {1:?}")]
    UnexpectedResponse(RespValue, RespValue),

    #[error("the reader is closed")]
    ReaderClosed,

    #[error("the writer is disconnected")]
    WriterDisconnected,

    #[error("expected string")]
    ExpectedString(#[label("not a string")] SourceSpan),

    #[error("expected int")]
    ExpectedInt(#[label("not an i64")] SourceSpan),

    #[error("expected float")]
    ExpectedFloat(#[label("not a float")] SourceSpan),

    #[error("expected usize")]
    ExpectedUsize(#[label("not a usize")] SourceSpan),

    #[error("expected an array but got {0:?}")]
    ExpectedArray(RespValue),

    #[error("expected {0:?} to contain {1:?}")]
    DoesNotContain(Vec<RespValue>, RespValue),

    #[error("expected one node")]
    ExpectedOneNode,

    #[error("invalid info")]
    InvalidInfo,

    #[error("invalid utf8")]
    InvalidUtf8(#[from] Utf8Error),

    #[error("invalid int")]
    InvalidInt(#[from] ParseIntError),

    #[error("expected {0:?}\ngot {1:?}")]
    Dirty(usize, usize),

    #[error("unexpected value")]
    UnexpectedValue(RespValue),

    #[error("missing variable")]
    MissingVariable,

    #[error("pttl {0:?} is not within expected range: {1:?}")]
    OutOfRange(RespValue, RangeInclusive<i64>),

    #[error("expected children")]
    ExpectedChildren,

    #[error("wrong var type")]
    WrongType,

    #[error("try from int error")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Expected client to be closed")]
    NotClosed,

    #[error("Not enough arguments")]
    NotEnoughArguments,

    #[error("Expected {0:?} arguments, got {1:?}")]
    WrongArguments(usize, usize),

    #[error("Expected {0:?} to contain {1:?}")]
    ExpectedToContain(Bytes, Bytes),

    #[error("Expected {0:?} not to contain {1:?}")]
    ExpectedNotToContain(Bytes, Bytes),

    #[error("Expected {0:?} to end with {1:?}")]
    ExpectedToEndWith(Bytes, Bytes),

    #[error("Key not found: {0:?}")]
    KeyNotFound(String),

    #[error("expected {0:?}\ngot {1:?}")]
    NotEqual(Bytes, Bytes),

    #[error("running only a subset of tests")]
    Only,

    #[error("duplicate map key")]
    DuplicateKey,

    #[error("duplicate set value")]
    DuplicateValue,

    #[error("can't find info: {0:?}")]
    InfoNotFound(Bytes),

    #[error("overflow")]
    Overflow,

    #[error("expected an even number of nodes")]
    OddNodes,

    #[error("No client for the current index")]
    MissingClient,
}
