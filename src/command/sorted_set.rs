use crate::{
    BlockResult, CommandResult,
    bytes::{lex, parse},
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    db::{Extreme, Insertion, SortedSetRef},
    reply::{Reply, ReplyError},
    slice::slice,
    store::Store,
};
use logos::Logos;
use std::{ops::Bound, time::Duration};

/// Parse a float, do not allow NaN.
fn parse_float(value: &[u8]) -> Result<f64, Reply> {
    let value: f64 = parse(value).ok_or(ReplyError::Float)?;
    if value.is_nan() {
        return Err(ReplyError::Float.into());
    }
    Ok(value)
}

/// Parse a score bound.
fn score_bound(client: &mut Client) -> Result<Bound<f64>, Reply> {
    let argument = client.request.pop()?;
    use Bound::*;
    Ok(match &argument[..] {
        [b'(', rest @ ..] => Excluded(parse_float(rest)?),
        rest => Included(parse_float(rest)?),
    })
}

pub static BZMPOP: Command = Command {
    kind: CommandKind::Bzmpop,
    name: "bzmpop",
    arity: Arity::Minimum(5),
    run: zmpop,
    keys: Keys::Argument(2),
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static BZPOPMAX: Command = Command {
    kind: CommandKind::Bzpopmax,
    name: "bzpopmax",
    arity: Arity::Minimum(3),
    run: bzpop,
    keys: Keys::Trailing,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static BZPOPMIN: Command = Command {
    kind: CommandKind::Bzpopmin,
    name: "bzpopmin",
    arity: Arity::Minimum(3),
    run: bzpop,
    keys: Keys::Trailing,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn bzpop(client: &mut Client, store: &mut Store) -> CommandResult {
    let extreme = match client.request.command.kind {
        CommandKind::Bzpopmax => Extreme::Max,
        CommandKind::Bzpopmin => Extreme::Min,
        _ => unreachable!(),
    };

    // Validate the timeout before executing.
    client.request.reset(client.request.len() - 1);
    let timeout = client.request.timeout()?;
    client.request.reset(1);

    let db = store.mut_db(client.db())?;

    while client.request.remaining() > 1 {
        let key = client.request.pop()?;
        let Some(set) = db.mut_sorted_set(&key)? else {
            continue;
        };
        let Some((score, value)) = set.pop(extreme) else {
            continue;
        };

        client.reply(Reply::Array(3));
        client.reply(&key);
        client.reply(value);
        client.reply(score);

        if set.is_empty() {
            db.remove(&key);
        }

        store.touch(client.db(), &key);
        return Ok(None);
    }

    if client.in_exec {
        client.reply(Reply::Nil);
        return Ok(None);
    }

    let len = client.request.len();
    let block = BlockResult::new(timeout, (1..len - 1).step_by(1));
    Ok(Some(block))
}

pub static ZADD: Command = Command {
    kind: CommandKind::Zadd,
    name: "zadd",
    arity: Arity::Minimum(4),
    run: zadd,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Logos)]
pub enum ZaddOption {
    #[regex(b"(?i:ch)")]
    Ch,

    #[regex(b"(?i:gt)")]
    Gt,

    #[regex(b"(?i:lt)")]
    Lt,

    #[regex(b"(?i:nx)")]
    Nx,

    #[regex(b"(?i:xx)")]
    Xx,
}

fn zadd(client: &mut Client, store: &mut Store) -> CommandResult {
    let max_len = store.zset_max_listpack_entries;
    let max_size = store.zset_max_listpack_value;
    let key = client.request.pop()?;
    let mut ch = false;
    let mut gt = false;
    let mut lt = false;
    let mut nx = false;
    let mut xx = false;

    loop {
        let Some(arg) = client.request.try_pop() else {
            break;
        };
        let Some(option) = lex(&arg[..]) else {
            client.request.reset(client.request.next() - 1);
            break;
        };

        use ZaddOption::*;
        match option {
            Ch => {
                ch = true;
            }
            Gt => {
                gt = true;
            }
            Lt => {
                lt = true;
            }
            Nx => {
                nx = true;
            }
            Xx => {
                xx = true;
            }
        }
    }

    if nx && xx {
        return Err(ReplyError::XxAndNx.into());
    }

    if (gt || lt || nx) && !(gt ^ lt ^ nx) {
        return Err(ReplyError::GtLtNx.into());
    }

    let db = store.mut_db(client.db())?;

    // If XX was passed and the key doesn't exist, there is nothing to be done.
    if xx && !db.exists(&key) {
        client.reply(0);
        return Ok(None);
    }

    let set = db.sorted_set_or_default(&key)?;

    client.request.assert_pairs()?;

    // Ensure that scores are valid before starting.
    let next = client.request.next();
    while !client.request.is_empty() {
        client.request.not_nan()?;
        client.request.pop()?;
    }
    client.request.reset(next);

    let mut added = 0;
    let mut changed = 0;
    while !client.request.is_empty() {
        let score = client.request.not_nan()?;
        let member = client.request.pop()?;

        if gt || lt {
            if let Some(current) = set.score(&member) {
                if gt && *score <= current {
                    continue;
                }

                if lt && *score >= current {
                    continue;
                }
            }
        }

        if nx && set.contains(&member) {
            continue;
        }

        if xx && !set.contains(&member) {
            continue;
        }

        match set.insert(score, &member[..], max_len, max_size) {
            Some(Insertion::Added) => {
                added += 1;
            }
            Some(Insertion::Changed) => {
                changed += 1;
            }
            _ => {}
        }
    }

    store.dirty += added + changed;
    store.touch(client.db(), &key);
    store.mark_ready(client.db(), &key);
    client.reply(if ch { added + changed } else { added });
    Ok(None)
}

pub static ZCARD: Command = Command {
    kind: CommandKind::Zcard,
    name: "zcard",
    arity: Arity::Exact(2),
    run: zcard,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn zcard(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(0)?;
    client.reply(set.len());
    Ok(None)
}

pub static ZCOUNT: Command = Command {
    kind: CommandKind::Zcount,
    name: "zcount",
    arity: Arity::Exact(4),
    run: zcount,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn zcount(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let min = score_bound(client)?;
    let max = score_bound(client)?;

    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(0)?;
    client.reply(set.count(&(min, max)));
    Ok(None)
}

pub static ZMPOP: Command = Command {
    kind: CommandKind::Zmpop,
    name: "zmpop",
    arity: Arity::Minimum(4),
    run: zmpop,
    keys: Keys::Argument(1),
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ZmpopOption {
    #[regex(b"(?i:count)")]
    Count,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ExtremeOption {
    #[regex(b"(?i:max)")]
    Max,

    #[regex(b"(?i:min)")]
    Min,
}

pub fn extreme(client: &mut Client) -> Result<Extreme, ReplyError> {
    use ExtremeOption::*;
    match lex(&client.request.pop()?[..]) {
        Some(Max) => Ok(Extreme::Max),
        Some(Min) => Ok(Extreme::Min),
        _ => Err(ReplyError::Syntax),
    }
}

fn zmpop(client: &mut Client, store: &mut Store) -> CommandResult {
    let blocking = client.request.kind() == CommandKind::Bzmpop;
    let timeout = if blocking {
        client.request.timeout()?
    } else {
        Duration::ZERO
    };
    let numkeys = client
        .request
        .usize()
        .map_err(|_| ReplyError::NumkeysZero)?;
    let start = client.request.next();

    if numkeys == 0 {
        return Err(ReplyError::NumkeysZero.into());
    }

    if client.request.len() < start + numkeys + 1 {
        return Err(ReplyError::Syntax.into());
    }

    client.request.reset(start + numkeys);
    let extreme = extreme(client)?;
    let mut count = None;
    while let Some(argument) = client.request.try_pop() {
        match lex(&argument[..]) {
            Some(ZmpopOption::Count) if count.is_none() => {
                count = Some(client.request.usize().map_err(|_| ReplyError::CountZero)?);
            }
            _ => return Err(ReplyError::Syntax.into()),
        }
    }
    if count == Some(0) {
        return Err(ReplyError::CountZero.into());
    }
    let count = count.unwrap_or(1);
    let db = store.mut_db(client.db())?;

    client.request.reset(start);
    for _ in 0..numkeys {
        let key = client.request.pop()?;
        let Some(set) = db.mut_sorted_set(&key)? else {
            continue;
        };
        if set.is_empty() {
            continue;
        }
        client.reply(Reply::Array(2));
        client.reply(key.clone());
        let count = std::cmp::min(count, set.len());
        client.reply(Reply::Array(count));
        for _ in 0..count {
            if let Some((score, value)) = set.pop(extreme) {
                client.reply(Reply::Array(2));
                client.reply(value);
                client.reply(score);
            }
        }
        if set.is_empty() {
            db.remove(&key);
        }
        store.touch(client.db(), &key);
        return Ok(None);
    }

    if !blocking || client.in_exec {
        client.reply(Reply::Nil);
        return Ok(None);
    }

    let range = start..start + numkeys;
    let block = BlockResult::new(timeout, range.step_by(1));
    Ok(Some(block))
}

fn zpop(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let mut count = 1i64;
    let mut nested = false;

    let extreme = match client.request.command.kind {
        CommandKind::Zpopmin => Extreme::Min,
        CommandKind::Zpopmax => Extreme::Max,
        _ => unreachable!(),
    };

    match client.request.remaining() {
        0 => {}
        1 => {
            count = client.request.i64()?;
            nested = client.v3();
        }
        _ => return Err(ReplyError::Syntax.into()),
    }

    let db = store.mut_db(client.db())?;
    let set = db.mut_sorted_set(&key)?.ok_or(Reply::Array(0))?;

    let count = usize::try_from(count).unwrap_or(0);
    let count = std::cmp::min(count, set.len());

    client.reply(Reply::Array(if nested { count } else { count * 2 }));

    for _ in 0..count {
        if let Some((score, value)) = set.pop(extreme) {
            if nested {
                client.reply(Reply::Array(2));
            }
            client.reply(value);
            client.reply(score);
        }
    }

    if set.is_empty() {
        db.remove(&key);
    }

    Ok(None)
}

pub static ZPOPMAX: Command = Command {
    kind: CommandKind::Zpopmax,
    name: "zpopmax",
    arity: Arity::Minimum(2),
    run: zpop,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static ZPOPMIN: Command = Command {
    kind: CommandKind::Zpopmin,
    name: "zpopmin",
    arity: Arity::Minimum(2),
    run: zpop,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static ZRANGE: Command = Command {
    kind: CommandKind::Zrange,
    name: "zrange",
    arity: Arity::Minimum(4),
    run: zrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

pub static ZRANGEBYSCORE: Command = Command {
    kind: CommandKind::Zrangebyscore,
    name: "zrangebyscore",
    arity: Arity::Minimum(4),
    run: zrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

pub static ZREVRANGE: Command = Command {
    kind: CommandKind::Zrevrange,
    name: "zrevrange",
    arity: Arity::Minimum(4),
    run: zrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

pub static ZREVRANGEBYSCORE: Command = Command {
    kind: CommandKind::Zrevrangebyscore,
    name: "zrevrangebyscore",
    arity: Arity::Minimum(4),
    run: zrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

pub struct ZrangeOptions {
    pub by: Zrangeby,
    pub withscores: bool,
    pub limit: Option<(usize, usize)>,
    pub reverse: bool,
}

impl Default for ZrangeOptions {
    fn default() -> Self {
        Self {
            by: Zrangeby::Rank,
            withscores: false,
            limit: None,
            reverse: false,
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum Zrangeby {
    Lex,
    Rank,
    Score,
}

#[derive(Logos)]
pub enum ZrangeOption {
    #[regex(b"(?i:bylex)")]
    Bylex,

    #[regex(b"(?i:byscore)")]
    Byscore,

    #[regex(b"(?i:limit)")]
    Limit,

    #[regex(b"(?i:rev)")]
    Rev,

    #[regex(b"(?i:withscores)")]
    Withscores,
}

fn zrange(client: &mut Client, store: &mut Store) -> CommandResult {
    // TODO: All the options
    client.request.reset(4);
    let mut options = ZrangeOptions::default();

    use CommandKind::*;
    match client.request.kind() {
        Zrangebyscore => {
            options.by = Zrangeby::Score;
        }
        Zrevrange | Zrevrangebyscore => {
            options.reverse = true;
        }
        _ => {}
    }

    let by_allowed = client.request.kind() == CommandKind::Zrange;
    let limit_allowed = client.request.kind() != CommandKind::Zrevrange;
    let rev_allowed = client.request.kind() == CommandKind::Zrange;

    while !client.request.is_empty() {
        use ZrangeOption::*;

        let argument = client.request.pop()?;
        let Some(option) = lex(&argument[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        match option {
            Bylex if by_allowed && options.by == Zrangeby::Rank => {
                options.by = Zrangeby::Lex;
            }
            Byscore if by_allowed && options.by == Zrangeby::Rank => {
                options.by = Zrangeby::Score;
            }
            Limit if limit_allowed => {
                let offset = client.request.usize()?;
                let count = client.request.usize()?;
                options.limit = Some((offset, count));
            }
            Rev if rev_allowed => {
                options.reverse = true;
            }
            Withscores => {
                options.withscores = true;
            }
            _ => return Err(ReplyError::Syntax.into()),
        }
    }

    client.request.reset(1);

    use Zrangeby::*;
    let f = match options.by {
        Lex => zrangebylex,
        Rank => zrangebyrank,
        Score => zrangebyscore,
    };

    f(client, store, options)
}

fn zrangebylex(_client: &mut Client, _store: &mut Store, _options: ZrangeOptions) -> CommandResult {
    todo!()
}

fn zrangebyrank(client: &mut Client, store: &mut Store, options: ZrangeOptions) -> CommandResult {
    if options.limit.is_some() {
        return Err(ReplyError::ZrangeLimit.into());
    }

    let key = client.request.pop()?;
    let min = client.request.i64()?;
    let max = client.request.i64()?;
    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(Reply::Array(0))?;

    let range = slice(set.len(), min, max).ok_or(Reply::Array(0))?;

    if options.reverse {
        zrange_reply(client, set.rev_range(range), options);
    } else {
        zrange_reply(client, set.range(range), options);
    }

    Ok(None)
}

fn zrangebyscore(client: &mut Client, store: &mut Store, options: ZrangeOptions) -> CommandResult {
    let key = client.request.pop()?;
    let min = score_bound(client)?;
    let max = score_bound(client)?;
    let range = (min, max);
    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(Reply::Array(0))?;

    if options.reverse {
        zrange_reply(client, set.rev_range_score(&range), options);
    } else {
        zrange_reply(client, set.range_score(&range), options);
    }

    Ok(None)
}

fn zrange_reply<'a, I: Iterator<Item = (f64, SortedSetRef<'a>)> + ExactSizeIterator>(
    client: &mut Client,
    iterator: I,
    options: ZrangeOptions,
) {
    let mut size = iterator.len();
    let (offset, limit) = options.limit.unwrap_or((0, usize::MAX));
    size -= offset;
    size = std::cmp::min(size, limit);
    if options.withscores {
        size *= 2;
    }
    client.reply(Reply::Array(size));

    for (score, value) in iterator.skip(offset).take(limit) {
        client.reply(value);
        if options.withscores {
            client.reply(score);
        }
    }
}

pub static ZRANK: Command = Command {
    kind: CommandKind::Zrank,
    name: "zrank",
    arity: Arity::Exact(3),
    run: zrank,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn zrank(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let member = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(Reply::Nil)?;

    let rank = set.rank(&member);

    client.reply(rank);
    Ok(None)
}

pub static ZREM: Command = Command {
    kind: CommandKind::Zrem,
    name: "zrem",
    arity: Arity::Minimum(3),
    run: zrem,
    keys: Keys::All,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn zrem(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let set = db.mut_sorted_set(&key)?.ok_or(0)?;
    let mut count = 0;

    for value in client.request.iter() {
        if set.remove(value) {
            count += 1;
        }
    }

    if set.is_empty() {
        db.remove(&key);
    }

    client.reply(count);
    store.touch(client.db(), &key);
    Ok(None)
}

pub static ZREMRANGEBYSCORE: Command = Command {
    kind: CommandKind::Zrem,
    name: "zremrangebyscore",
    arity: Arity::Exact(4),
    run: zremrangebyscore,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn zremrangebyscore(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let min = score_bound(client)?;
    let max = score_bound(client)?;
    let range = (min, max);
    let db = store.mut_db(client.db())?;
    let set = db.mut_sorted_set(&key)?.ok_or(0)?;

    client.reply(set.remove_range_score(&range));

    if set.is_empty() {
        db.remove(&key);
    }

    Ok(None)
}

pub static ZSCORE: Command = Command {
    kind: CommandKind::Zscore,
    name: "zscore",
    arity: Arity::Exact(3),
    run: zscore,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn zscore(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let member = client.request.pop()?;

    let db = store.get_db(client.db())?;
    let set = db.get_sorted_set(&key)?.ok_or(Reply::Nil)?;
    let score = set.score(&member).ok_or(Reply::Nil)?;

    client.bulk(score);
    Ok(None)
}
