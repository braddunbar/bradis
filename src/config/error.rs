use bytes::Bytes;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("dbfilename can't be a path, just a filename")]
    Dbfilename,

    #[error("Can't chdir to {:?}: {}", .0, .1)]
    Dir(Bytes, io::Error),

    #[error("argument couldn't be parsed into an integer")]
    Integer,

    #[error("argument must be a memory value")]
    Memory,

    #[error("argument must be 'yes' or 'no'")]
    YesNo,
}
