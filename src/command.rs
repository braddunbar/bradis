mod bitops;
mod client;
mod config;
mod db;
mod debug;
mod eval;
mod expire;
mod hash;
mod keys;
mod list;
mod pubsub;
mod set;
mod sorted_set;
mod string;

pub use bitops::*;
pub use client::*;
pub use config::*;
pub use db::*;
pub use debug::*;
pub use eval::*;
pub use expire::*;
pub use hash::*;
pub use keys::*;
pub use list::*;
pub use pubsub::*;
pub use set::*;
pub use sorted_set::*;
pub use string::*;

use crate::{bytes::lex, client::Client, db::Edge, reply::Reply, store::Store};
use logos::Logos;
use std::{iter::StepBy, ops::Range, time::Duration};

/// A description of the number of arguments a command accepts.
#[derive(Debug)]
pub enum Arity {
    Exact(u8),
    Minimum(u8),
}

/// A description of where the keys are in the arguments to a command.
#[derive(Debug)]
pub enum Keys {
    All,
    Argument(usize),
    Double,
    Odd,
    None,
    Single,
    SkipOne,
    Trailing,
}

impl Keys {
    /// Get the representation of a commands keys for the `COMMAND` command.
    pub fn first_last_step(&self) -> (usize, i64, usize) {
        use Keys::*;
        match self {
            All => (1, -1, 1),
            Argument(_) => (0, 0, 0),
            Double => (1, 2, 1),
            Odd => (1, -1, 2),
            None => (0, 0, 0),
            Single => (1, 1, 1),
            SkipOne => (2, -1, 1),
            Trailing => (1, -2, 1),
        }
    }
}

/// The result of a blocking command.
pub struct BlockResult {
    /// They keys a command is blocking on.
    pub keys: StepBy<Range<usize>>,

    /// The timeout for a blocking operation.
    pub timeout: Duration,
}

impl BlockResult {
    /// Create a new [`BlockResult`].
    fn new(timeout: Duration, keys: StepBy<Range<usize>>) -> Self {
        Self { timeout, keys }
    }
}

/// The result of a command being run.
pub type CommandResult = Result<Option<BlockResult>, Reply>;

/// Information about a particular command that can be run.
pub struct Command {
    /// What kind of command is this?
    pub kind: CommandKind,

    /// The name of the command.
    pub name: &'static str,

    /// What are the arguments to this command?
    pub arity: Arity,

    /// What function runs this command?
    pub run: fn(&mut Client, &mut Store) -> CommandResult,

    /// Where are the keys in this command?
    pub keys: Keys,

    /// Is this command read only?
    pub readonly: bool,

    /// Is this an admin command?
    pub admin: bool,

    /// Is this command disallowed during scripting?
    pub noscript: bool,

    /// Is this a pubsub command?
    pub pubsub: bool,

    /// Does this command write data?
    pub write: bool,
}

impl From<&[u8]> for &'static Command {
    fn from(value: &[u8]) -> &'static Command {
        lex::<CommandKind>(value).map_or(&UNKNOWN, |kind| kind.command())
    }
}

impl Command {
    /// Can this command be executed while monitoring?
    pub fn monitor_allowed(&self) -> bool {
        // TODO: Also disallow may_replicate commands.
        !self.readonly && !self.write
    }

    /// Is this command allowed in pubsub mode?
    pub fn pubsub_allowed(&self) -> bool {
        use CommandKind::*;
        matches!(
            self.kind,
            Subscribe | Psubscribe | Unsubscribe | Punsubscribe | Ping | Quit | Reset
        )
    }

    /// Is this command queueable during a transaction?
    pub fn queueable(&self) -> bool {
        use CommandKind::*;
        !matches!(self.kind, Exec | Discard | Multi | Quit | Reset | Watch)
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Command")
            .field("admin", &self.admin)
            .field("arity", &self.arity)
            .field("keys", &self.keys)
            .field("kind", &self.kind)
            .field("name", &self.name)
            .field("noscript", &self.noscript)
            .field("pubsub", &self.pubsub)
            .field("readonly", &self.readonly)
            .field("write", &self.write)
            .finish()
    }
}

pub static ALL: [&Command; 125] = [
    &APPEND,
    &BITCOUNT,
    &BITFIELD,
    &BITOP,
    &BITPOS,
    &BLMOVE,
    &BLPOP,
    &BRPOP,
    &BRPOPLPUSH,
    &BZMPOP,
    &BZPOPMAX,
    &BZPOPMIN,
    &CLIENT,
    &COMMAND,
    &CONFIG,
    &COPY,
    &DBSIZE,
    &DECR,
    &DECRBY,
    &DEL,
    &DISCARD,
    &ECHO,
    &EVAL,
    &EXEC,
    &EXISTS,
    &EXPIRE,
    &EXPIREAT,
    &EXPIRETIME,
    &FLUSHALL,
    &FLUSHDB,
    &GET,
    &GETDEL,
    &GETEX,
    &GETBIT,
    &GETRANGE,
    &GETSET,
    &HDEL,
    &HELLO,
    &HEXISTS,
    &HGET,
    &HGETALL,
    &HINCRBY,
    &HINCRBYFLOAT,
    &HKEYS,
    &HLEN,
    &HMGET,
    &HSET,
    &HSETNX,
    &HMSET,
    &HSTRLEN,
    &HVALS,
    &INCR,
    &INCRBY,
    &INCRBYFLOAT,
    &KEYS,
    &LINDEX,
    &LINSERT,
    &LLEN,
    &LMOVE,
    &LPOP,
    &LPOS,
    &LPUSH,
    &LPUSHX,
    &LRANGE,
    &LREM,
    &LSET,
    &LTRIM,
    &MGET,
    &MOVE,
    &MSET,
    &MSETNX,
    &MULTI,
    &PERSIST,
    &PEXPIRE,
    &PEXPIREAT,
    &PEXPIRETIME,
    &PING,
    &PSETEX,
    &PSUBSCRIBE,
    &PTTL,
    &PUBLISH,
    &PUBSUB,
    &PUNSUBSCRIBE,
    &QUIT,
    &RENAME,
    &RENAMENX,
    &RESET,
    &RPOP,
    &RPOPLPUSH,
    &RPUSH,
    &RPUSHX,
    &SADD,
    &SCARD,
    &SELECT,
    &SET,
    &SETBIT,
    &SETEX,
    &SETNX,
    &SETRANGE,
    &SISMEMBER,
    &SMEMBERS,
    &SMISMEMBER,
    &SPOP,
    &SREM,
    &STRLEN,
    &SUBSCRIBE,
    &SWAPDB,
    &TTL,
    &TYPE,
    &UNLINK,
    &UNSUBSCRIBE,
    &UNWATCH,
    &WATCH,
    &ZADD,
    &ZCARD,
    &ZCOUNT,
    &ZMPOP,
    &ZPOPMIN,
    &ZRANGEBYSCORE,
    &ZRANK,
    &ZREM,
    &ZREMRANGEBYSCORE,
    &ZREVRANGE,
    &ZREVRANGEBYSCORE,
    &ZSCORE,
];

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum CommandKind {
    #[regex(b"(?i:append)")]
    Append,

    #[regex(b"(?i:bitcount)")]
    Bitcount,

    #[regex(b"(?i:bitfield)")]
    Bitfield,

    #[regex(b"(?i:bitfield_ro)")]
    Bitfieldro,

    #[regex(b"(?i:bitop)")]
    Bitop,

    #[regex(b"(?i:bitpos)")]
    Bitpos,

    #[regex(b"(?i:blmove)")]
    Blmove,

    #[regex(b"(?i:blmpop)")]
    Blmpop,

    #[regex(b"(?i:blpop)")]
    Blpop,

    #[regex(b"(?i:brpop)")]
    Brpop,

    #[regex(b"(?i:brpoplpush)")]
    Brpoplpush,

    #[regex(b"(?i:bzmpop)")]
    Bzmpop,

    #[regex(b"(?i:bzpopmax)")]
    Bzpopmax,

    #[regex(b"(?i:bzpopmin)")]
    Bzpopmin,

    #[regex(b"(?i:client)")]
    Client,

    #[regex(b"(?i:command)")]
    Command,

    #[regex(b"(?i:config)")]
    Config,

    #[regex(b"(?i:copy)")]
    Copy,

    #[regex(b"(?i:dbsize)")]
    Dbsize,

    #[regex(b"(?i:decr)")]
    Decr,

    #[regex(b"(?i:debug)")]
    Debug,

    #[regex(b"(?i:decrby)")]
    Decrby,

    #[regex(b"(?i:del)")]
    Del,

    #[regex(b"(?i:discard)")]
    Discard,

    #[regex(b"(?i:echo)")]
    Echo,

    #[regex(b"(?i:eval)")]
    Eval,

    #[regex(b"(?i:exists)")]
    Exists,

    #[regex(b"(?i:expire)")]
    Expire,

    #[regex(b"(?i:expireat)")]
    Expireat,

    #[regex(b"(?i:expiretime)")]
    Expiretime,

    #[regex(b"(?i:exec)")]
    Exec,

    #[regex(b"(?i:flushall)")]
    Flushall,

    #[regex(b"(?i:flushdb)")]
    Flushdb,

    #[regex(b"(?i:get)")]
    Get,

    #[regex(b"(?i:getdel)")]
    Getdel,

    #[regex(b"(?i:getex)")]
    Getex,

    #[regex(b"(?i:getbit)")]
    Getbit,

    #[regex(b"(?i:getrange)")]
    Getrange,

    #[regex(b"(?i:getset)")]
    Getset,

    #[regex(b"(?i:hdel)")]
    Hdel,

    #[regex(b"(?i:hello)")]
    Hello,

    #[regex(b"(?i:hexists)")]
    Hexists,

    #[regex(b"(?i:hget)")]
    Hget,

    #[regex(b"(?i:hgetall)")]
    Hgetall,

    #[regex(b"(?i:hincrby)")]
    Hincrby,

    #[regex(b"(?i:hincrbyfloat)")]
    Hincrbyfloat,

    #[regex(b"(?i:hkeys)")]
    Hkeys,

    #[regex(b"(?i:hlen)")]
    Hlen,

    #[regex(b"(?i:hmget)")]
    Hmget,

    #[regex(b"(?i:hset)")]
    Hset,

    #[regex(b"(?i:hsetnx)")]
    Hsetnx,

    #[regex(b"(?i:hmset)")]
    Hmset,

    #[regex(b"(?i:hstrlen)")]
    Hstrlen,

    #[regex(b"(?i:hvals)")]
    Hvals,

    #[regex(b"(?i:incr)")]
    Incr,

    #[regex(b"(?i:incrby)")]
    Incrby,

    #[regex(b"(?i:incrbyfloat)")]
    Incrbyfloat,

    #[regex(b"(?i:info)")]
    Info,

    #[regex(b"(?i:linsert)")]
    Linsert,

    #[regex(b"(?i:rpop)")]
    Rpop,

    #[regex(b"(?i:rpoplpush)")]
    Rpoplpush,

    #[regex(b"(?i:keys)")]
    Keys,

    #[regex(b"(?i:lindex)")]
    Lindex,

    #[regex(b"(?i:llen)")]
    Llen,

    #[regex(b"(?i:lmove)")]
    Lmove,

    #[regex(b"(?i:lmpop)")]
    Lmpop,

    #[regex(b"(?i:lpop)")]
    Lpop,

    #[regex(b"(?i:lpos)")]
    Lpos,

    #[regex(b"(?i:lpush)")]
    Lpush,

    #[regex(b"(?i:lpushx)")]
    Lpushx,

    #[regex(b"(?i:lrange)")]
    Lrange,

    #[regex(b"(?i:lrem)")]
    Lrem,

    #[regex(b"(?i:lset)")]
    Lset,

    #[regex(b"(?i:ltrim)")]
    Ltrim,

    #[regex(b"(?i:mget)")]
    Mget,

    #[regex(b"(?i:monitor)")]
    Monitor,

    #[regex(b"(?i:move)")]
    Move,

    #[regex(b"(?i:mset)")]
    Mset,

    #[regex(b"(?i:msetnx)")]
    Msetnx,

    #[regex(b"(?i:multi)")]
    Multi,

    #[regex(b"(?i:object)")]
    Object,

    #[regex(b"(?i:persist)")]
    Persist,

    #[regex(b"(?i:pexpire)")]
    Pexpire,

    #[regex(b"(?i:pexpireat)")]
    Pexpireat,

    #[regex(b"(?i:pexpiretime)")]
    Pexpiretime,

    #[regex(b"(?i:ping)")]
    Ping,

    #[regex(b"(?i:psetex)")]
    Psetex,

    #[regex(b"(?i:pttl)")]
    Pttl,

    #[regex(b"(?i:publish)")]
    Publish,

    #[regex(b"(?i:pubsub)")]
    Pubsub,

    #[regex(b"(?i:psubscribe)")]
    Psubscribe,

    #[regex(b"(?i:punsubscribe)")]
    Punsubscribe,

    #[regex(b"(?i:quit)")]
    Quit,

    #[regex(b"(?i:rename)")]
    Rename,

    #[regex(b"(?i:renamenx)")]
    Renamenx,

    #[regex(b"(?i:reset)")]
    Reset,

    #[regex(b"(?i:rpush)")]
    Rpush,

    #[regex(b"(?i:rpushx)")]
    Rpushx,

    #[regex(b"(?i:sadd)")]
    Sadd,

    #[regex(b"(?i:scard)")]
    Scard,

    #[regex(b"(?i:select)")]
    Select,

    #[regex(b"(?i:set)")]
    Set,

    #[regex(b"(?i:setbit)")]
    Setbit,

    #[regex(b"(?i:setex)")]
    Setex,

    #[regex(b"(?i:setnx)")]
    Setnx,

    #[regex(b"(?i:setrange)")]
    Setrange,

    #[regex(b"(?i:sismember)")]
    Sismember,

    #[regex(b"(?i:smembers)")]
    Smembers,

    #[regex(b"(?i:smismember)")]
    Smismember,

    #[regex(b"(?i:spop)")]
    Spop,

    #[regex(b"(?i:srem)")]
    Srem,

    #[regex(b"(?i:strlen)")]
    Strlen,

    #[regex(b"(?i:subscribe)")]
    Subscribe,

    #[regex(b"(?i:swapdb)")]
    Swapdb,

    #[regex(b"(?i:ttl)")]
    Ttl,

    #[regex(b"(?i:type)")]
    Type,

    #[regex(b"(?i:watch)")]
    Watch,

    #[regex(b"(?i:unlink)")]
    Unlink,

    #[regex(b"(?i:unsubscribe)")]
    Unsubscribe,

    #[regex(b"(?i:unwatch)")]
    Unwatch,

    #[regex(b"(?i:zadd)")]
    Zadd,

    #[regex(b"(?i:zcard)")]
    Zcard,

    #[regex(b"(?i:zcount)")]
    Zcount,

    #[regex(b"(?i:zmpop)")]
    Zmpop,

    #[regex(b"(?i:zpopmax)")]
    Zpopmax,

    #[regex(b"(?i:zpopmin)")]
    Zpopmin,

    #[regex(b"(?i:zrange)")]
    Zrange,

    #[regex(b"(?i:zrank)")]
    Zrank,

    #[regex(b"(?i:zrangebyscore)")]
    Zrangebyscore,

    #[regex(b"(?i:zrem)")]
    Zrem,

    #[regex(b"(?i:zremrangebyscore)")]
    Zremrangebyscore,

    #[regex(b"(?i:zrevrange)")]
    Zrevrange,

    #[regex(b"(?i:zrevrangebyscore)")]
    Zrevrangebyscore,

    #[regex(b"(?i:zscore)")]
    Zscore,

    Unknown,
}

impl CommandKind {
    pub fn command(self) -> &'static Command {
        use CommandKind::*;

        match self {
            Append => &APPEND,
            Bitcount => &BITCOUNT,
            Bitfield => &BITFIELD,
            Bitfieldro => &BITFIELD_RO,
            Bitop => &BITOP,
            Bitpos => &BITPOS,
            Blmove => &BLMOVE,
            Blmpop => &BLMPOP,
            Blpop => &BLPOP,
            Brpop => &BRPOP,
            Brpoplpush => &BRPOPLPUSH,
            Bzmpop => &BZMPOP,
            Bzpopmax => &BZPOPMAX,
            Bzpopmin => &BZPOPMIN,
            Client => &CLIENT,
            Command => &COMMAND,
            Config => &CONFIG,
            Copy => &COPY,
            Dbsize => &DBSIZE,
            Debug => &DEBUG,
            Decr => &DECR,
            Decrby => &DECRBY,
            Del => &DEL,
            Discard => &DISCARD,
            Echo => &ECHO,
            Eval => &EVAL,
            Exec => &EXEC,
            Exists => &EXISTS,
            Expire => &EXPIRE,
            Expireat => &EXPIREAT,
            Expiretime => &EXPIRETIME,
            Flushall => &FLUSHALL,
            Flushdb => &FLUSHDB,
            Get => &GET,
            Getdel => &GETDEL,
            Getex => &GETEX,
            Getbit => &GETBIT,
            Getrange => &GETRANGE,
            Getset => &GETSET,
            Hdel => &HDEL,
            Hello => &HELLO,
            Hexists => &HEXISTS,
            Hget => &HGET,
            Hgetall => &HGETALL,
            Hincrby => &HINCRBY,
            Hincrbyfloat => &HINCRBYFLOAT,
            Hkeys => &HKEYS,
            Hlen => &HLEN,
            Hmget => &HMGET,
            Hset => &HSET,
            Hsetnx => &HSETNX,
            Hmset => &HMSET,
            Hstrlen => &HSTRLEN,
            Hvals => &HVALS,
            Incr => &INCR,
            Incrby => &INCRBY,
            Incrbyfloat => &INCRBYFLOAT,
            Info => &INFO,
            Keys => &KEYS,
            Lindex => &LINDEX,
            Linsert => &LINSERT,
            Llen => &LLEN,
            Lmove => &LMOVE,
            Lmpop => &LMPOP,
            Lpop => &LPOP,
            Lpos => &LPOS,
            Lpush => &LPUSH,
            Lpushx => &LPUSHX,
            Lrange => &LRANGE,
            Lrem => &LREM,
            Lset => &LSET,
            Ltrim => &LTRIM,
            Mget => &MGET,
            Monitor => &MONITOR,
            Move => &MOVE,
            Mset => &MSET,
            Msetnx => &MSETNX,
            Multi => &MULTI,
            Object => &OBJECT,
            Persist => &PERSIST,
            Pexpire => &PEXPIRE,
            Pexpireat => &PEXPIREAT,
            Pexpiretime => &PEXPIRETIME,
            Ping => &PING,
            Psetex => &PSETEX,
            Psubscribe => &PSUBSCRIBE,
            Pttl => &PTTL,
            Publish => &PUBLISH,
            Pubsub => &PUBSUB,
            Punsubscribe => &PUNSUBSCRIBE,
            Quit => &QUIT,
            Rename => &RENAME,
            Renamenx => &RENAMENX,
            Reset => &RESET,
            Rpop => &RPOP,
            Rpoplpush => &RPOPLPUSH,
            Rpush => &RPUSH,
            Rpushx => &RPUSHX,
            Sadd => &SADD,
            Scard => &SCARD,
            Select => &SELECT,
            Set => &SET,
            Setbit => &SETBIT,
            Setex => &SETEX,
            Setnx => &SETNX,
            Setrange => &SETRANGE,
            Sismember => &SISMEMBER,
            Smembers => &SMEMBERS,
            Smismember => &SMISMEMBER,
            Spop => &SPOP,
            Srem => &SREM,
            Strlen => &STRLEN,
            Subscribe => &SUBSCRIBE,
            Swapdb => &SWAPDB,
            Ttl => &TTL,
            Type => &TYPE,
            Unlink => &UNLINK,
            Unsubscribe => &UNSUBSCRIBE,
            Unwatch => &UNWATCH,
            Unknown => &UNKNOWN,
            Watch => &WATCH,
            Zadd => &ZADD,
            Zcard => &ZCARD,
            Zcount => &ZCOUNT,
            Zmpop => &ZMPOP,
            Zpopmax => &ZPOPMAX,
            Zpopmin => &ZPOPMIN,
            Zrange => &ZRANGE,
            Zrank => &ZRANK,
            Zrangebyscore => &ZRANGEBYSCORE,
            Zrem => &ZREM,
            Zremrangebyscore => &ZREMRANGEBYSCORE,
            Zrevrange => &ZREVRANGE,
            Zrevrangebyscore => &ZREVRANGEBYSCORE,
            Zscore => &ZSCORE,
        }
    }
}
