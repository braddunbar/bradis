use crate::{
    Command,
    bytes::{AsciiUpper, Output},
    config::{Config, ConfigError},
};
use bytes::Bytes;
use respite::RespError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplyError {
    #[error("ERR The bit argument must be 1 or 0.")]
    BitArgument,

    #[error("ERR BITFIELD_RO only supports the GET subcommand")]
    Bitfieldro,

    #[error("ERR bit offset is not an integer or out of range")]
    BitOffset,

    #[error("ERR BITOP NOT must be called with a single source key.")]
    BitopNot,

    #[error("BUSYKEY Target key name already exists.")]
    BusyKey,

    #[error("ERR Client names cannot contain spaces, newlines or special characters.")]
    ClientName,

    #[error("ERR Invalid argument '{}' for CONFIG SET '{}' - {}", Output(.0), .1.name, .2)]
    ConfigSet(Bytes, &'static Config, ConfigError),

    #[error("ERR count should be greater than 0")]
    CountZero,

    #[error("{}", Output(&.0[..]))]
    Custom(Bytes),

    #[error("ERR DB index is out of range")]
    DBIndex,

    #[error("EXECABORT Transaction discarded because of previous errors.")]
    ExecAbort,

    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,

    #[error("ERR invalid expire time in {} command", .0.name)]
    ExpireTime(&'static Command),

    #[error("ERR value is not a valid float")]
    Float,

    #[error("ERR GT, LT, and/or NX options at the same time are not compatible")]
    GtLtNx,

    #[error("ERR Syntax error in HELLO option {:?}", .0)]
    Hello(Bytes),

    #[error("ERR increment or decrement would overflow")]
    IncrOverflow,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR timeout is not finite")]
    InfiniteTimeout,

    #[error("ERR value is not an integer or out of range")]
    Integer,

    #[error("ERR Invalid argument(s)")]
    InvalidArgument,

    #[error(
        "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
    )]
    InvalidBitfield,

    #[error("ERR Invalid client ID")]
    InvalidClientId,

    #[error("ERR Invalid command specified")]
    InvalidCommand,

    #[error("ERR Invalid arguments specified for command")]
    InvalidCommandArguments,

    #[error("ERR Invalid number of arguments specified for command")]
    InvalidNumberOfArguments,

    #[error("ERR Invalid OVERFLOW type specified")]
    InvalidOverflow,

    #[error("ERR timeout is not a float or out of range")]
    InvalidTimeout,

    #[error("ERR Invalid TTL value, must be >= 0")]
    InvalidTtl,

    #[error("ERR invalid usize reply")]
    InvalidUsize,

    #[error("ERR MULTI calls can not be nested")]
    MultiNested,

    #[error("ERR increment would produce NaN or Infinity")]
    NanOrInfinity,

    #[error("ERR Number of keys can't be negative")]
    NegativeKeys,

    #[error("ERR timeout is negative")]
    NegativeTimeout,

    #[error("The command has no key arguments")]
    Nokeys,

    #[error("NOPROTO unsupported protocol version")]
    Noproto,

    #[error("NOSCRIPT No matching script. Please use EVAL.")]
    Noscript,

    #[error("ERR no such key")]
    NoSuchKey,

    #[error("ERR Number of keys can't be greater than number of args")]
    NumberOfKeys,

    #[error("ERR numkeys should be greater than 0")]
    NumkeysZero,

    #[error("ERR offset is out of range")]
    OffsetRange,

    #[error("ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", .0.name)]
    Pubsub(&'static Command),

    #[error("ERR Replica can't interact with the keyspace")]
    Replica,

    #[error("ERR Protocol Error: {}", .0)]
    Resp(#[from] RespError),

    #[error("ERR source and destination objects are the same")]
    SameObject,

    #[error("ERR string exceeds maximum allowed size (proto-max-bulk-len)")]
    StringLength,

    #[error("ERR syntax error")]
    Syntax,

    #[error("UNBLOCKED client unblocked via CLIENT UNBLOCK")]
    Unblocked,

    #[error("ERR unknown command")]
    UnknownCommand,

    #[error("ERR Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.", Output(.1), AsciiUpper(.0.name))]
    UnknownSubcommand(&'static Command, Bytes),

    #[error("ERR Unknown option or number of arguments for CONFIG SET - '{}'", Output(.0))]
    UnsupportedParameter(Bytes),

    #[error("ERR WATCH inside MULTI is not allowed")]
    WatchInMulti,

    #[error("ERR wrong number of arguments for '{}' command", .0.name)]
    WrongArguments(&'static Command),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR XX and NX options at the same time are not compatible")]
    XxAndNx,

    #[error(
        "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
    )]
    ZrangeLimit,
}
