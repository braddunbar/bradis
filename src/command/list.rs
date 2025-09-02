use crate::{
    BlockResult, CommandResult,
    bytes::{lex, parse},
    client::Client,
    command::{Arity, Command, CommandKind, Edge, Keys},
    db::Value,
    pack::Packable,
    reply::{Reply, ReplyError},
    slice::slice,
    store::Store,
};
use logos::Logos;
use std::{cmp::min, time::Duration};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum EdgeOption {
    #[regex(b"(?i:left)")]
    Left,

    #[regex(b"(?i:right)")]
    Right,
}

pub fn edge(client: &mut Client) -> Result<Edge, ReplyError> {
    use EdgeOption::*;
    match lex(&client.request.pop()?[..]) {
        Some(Left) => Ok(Edge::Left),
        Some(Right) => Ok(Edge::Right),
        _ => Err(ReplyError::Syntax),
    }
}

pub fn integer_with_edge(client: &mut Client) -> Result<(Edge, usize), ReplyError> {
    Ok(match &client.request.pop()?[..] {
        [b'-', rest @ ..] => {
            let count = parse(rest).ok_or(ReplyError::Integer)?;
            (Edge::Right, count)
        }
        rest => {
            let count = parse(rest).ok_or(ReplyError::Integer)?;
            (Edge::Left, count)
        }
    })
}

pub static BLMOVE: Command = Command {
    kind: CommandKind::Blmove,
    name: "blmove",
    arity: Arity::Exact(6),
    run: blmove,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: true,
};

pub static BRPOPLPUSH: Command = Command {
    kind: CommandKind::Brpoplpush,
    name: "brpoplpush",
    arity: Arity::Exact(4),
    run: blmove,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: true,
};

fn blmove(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let source_key = client.request.pop()?;
    let destination_key = client.request.pop()?;
    let (from, to) = if client.request.kind() == CommandKind::Brpoplpush {
        (Edge::Right, Edge::Left)
    } else {
        (edge(client)?, edge(client)?)
    };
    let timeout = client.request.timeout()?;

    let db = store.mut_db(client.db())?;
    let exists = db.get_list(&source_key)?.is_some();

    if !exists {
        if client.in_exec {
            return Err(Reply::Nil);
        }
        let block = BlockResult::new(timeout, (1..2).step_by(1));
        return Ok(Some(block));
    }

    // Ensure destination exists, and is a list.
    db.list_or_default(&destination_key)?;

    let [source, destination] = db
        .get_many_mut([&source_key[..], &destination_key[..]])
        .map(|value| value.unwrap().mut_list().unwrap());
    let element = source.peek(from).unwrap();
    client.reply(&element);
    destination.push(&element, to, max);
    source.trim(from, 1, max);
    if source.is_empty() {
        db.remove(&source_key);
    }
    store.touch(client.db(), &source_key);
    store.touch(client.db(), &destination_key);
    store.mark_ready(client.db(), &destination_key);

    Ok(None)
}

pub static BLPOP: Command = Command {
    kind: CommandKind::Blpop,
    name: "blpop",
    arity: Arity::Minimum(3),
    run: bpop,
    keys: Keys::Trailing,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: true,
};

pub static BRPOP: Command = Command {
    kind: CommandKind::Brpop,
    name: "brpop",
    arity: Arity::Minimum(3),
    run: bpop,
    keys: Keys::Trailing,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: true,
};

fn bpop(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let edge = match client.request.command.kind {
        CommandKind::Blpop => Edge::Left,
        CommandKind::Brpop => Edge::Right,
        _ => unreachable!(),
    };

    // Validate the timeout before executing.
    client.request.reset(client.request.len() - 1);
    let timeout = client.request.timeout()?;
    client.request.reset(1);

    let db = store.mut_db(client.db())?;

    while client.request.remaining() > 1 {
        let key = client.request.pop()?;
        let Some(list) = db.mut_list(&key)? else {
            continue;
        };
        let Some(value) = list.peek(edge) else {
            continue;
        };

        client.reply(Reply::Array(2));
        client.reply(&key);
        client.reply(value);

        list.trim(edge, 1, max);
        if list.is_empty() {
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

pub static LINDEX: Command = Command {
    kind: CommandKind::Lindex,
    name: "lindex",
    arity: Arity::Exact(3),
    run: lindex,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn lindex(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let mut index = client.request.i64()?;
    let list = store
        .get_db(client.db())?
        .get_list(&key)?
        .ok_or(Reply::Nil)?;
    let len = list.len();

    if index < 0 {
        index = i64::try_from(len)
            .ok()
            .and_then(|len| index.checked_add(len))
            .ok_or(Reply::Nil)?;
    }

    let index = usize::try_from(index).or(Err(Reply::Nil))?;

    if index >= len {
        return Err(Reply::Nil);
    }

    let value = list.iter().nth(index);

    client.reply(value);
    Ok(None)
}

pub static LINSERT: Command = Command {
    kind: CommandKind::Linsert,
    name: "linsert",
    arity: Arity::Exact(5),
    run: linsert,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum LinsertOption {
    #[regex(b"(?i:after)")]
    After,

    #[regex(b"(?i:before)")]
    Before,
}

fn linsert(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let key = client.request.pop()?;
    let position = client.request.pop()?;
    use LinsertOption::*;
    let before = match lex(&position) {
        Some(After) => false,
        Some(Before) => true,
        _ => return Err(ReplyError::Syntax.into()),
    };
    let pivot = client.request.pop()?;
    let element = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let list = db.mut_list(&key)?.ok_or(0)?;

    if list.insert(&element[..], &pivot[..], before, max) {
        let len = list.len();
        client.reply(len);
        store.touch(client.db(), &key);
    } else {
        client.reply(-1);
    }

    Ok(None)
}

pub static LLEN: Command = Command {
    kind: CommandKind::Llen,
    name: "llen",
    arity: Arity::Exact(2),
    run: llen,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn llen(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let len = store
        .get_db(client.db())?
        .get_list(&key[..])?
        .ok_or(0)?
        .len();

    client.reply(len);
    Ok(None)
}

pub static LMOVE: Command = Command {
    kind: CommandKind::Lmove,
    name: "lmove",
    arity: Arity::Exact(5),
    run: lmove,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static RPOPLPUSH: Command = Command {
    kind: CommandKind::Rpoplpush,
    name: "rpoplpush",
    arity: Arity::Exact(3),
    run: lmove,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lmove(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let source_key = client.request.pop()?;
    let destination_key = client.request.pop()?;
    let (from, to) = if client.request.kind() == CommandKind::Rpoplpush {
        (Edge::Right, Edge::Left)
    } else {
        (edge(client)?, edge(client)?)
    };
    let db = store.mut_db(client.db())?;

    if source_key == destination_key {
        let list = db.mut_list(&source_key)?.ok_or(Reply::Nil)?;
        if from == to || list.len() <= 1 {
            client.reply(list.peek(to));
        } else {
            list.mv(from, to, max);
            client.reply(list.peek(to));
            store.touch(client.db(), &source_key);
        }
    } else {
        db.get_list(&source_key)?.ok_or(Reply::Nil)?;
        db.list_or_default(&destination_key)?;

        let [source, dest] = db
            .get_many_mut([&source_key[..], &destination_key[..]])
            .map(|value| value.unwrap().mut_list().unwrap());
        let element = source.peek(from).unwrap();
        client.reply(&element);
        dest.push(&element, to, max);
        source.trim(from, 1, max);
        if source.is_empty() {
            db.remove(&source_key);
        }
        store.touch(client.db(), &source_key);
        store.touch(client.db(), &destination_key);
    }

    Ok(None)
}

pub static LMPOP: Command = Command {
    kind: CommandKind::Lmpop,
    name: "lmpop",
    arity: Arity::Minimum(4),
    run: lmpop,
    keys: Keys::Argument(1),
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static BLMPOP: Command = Command {
    kind: CommandKind::Blmpop,
    name: "blmpop",
    arity: Arity::Minimum(5),
    run: lmpop,
    keys: Keys::Argument(2),
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum MpopOption {
    #[regex(b"(?i:count)")]
    Count,
}

fn lmpop(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let blocking = client.request.kind() == CommandKind::Blmpop;
    let timeout = if blocking {
        client.request.timeout()?
    } else {
        Duration::ZERO
    };
    let numkeys = client.request.usize()?;
    let start = client.request.next();

    if numkeys == 0 {
        return Err(ReplyError::NumkeysZero.into());
    }

    if client.request.len() < start + numkeys + 1 {
        return Err(ReplyError::Syntax.into());
    }

    client.request.reset(start + numkeys);
    let edge = edge(client)?;
    let mut count = None;
    while let Some(argument) = client.request.try_pop() {
        match lex(&argument[..]) {
            Some(MpopOption::Count) if count.is_none() => {
                count = Some(client.request.usize()?);
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
        let Some(list) = db.mut_list(&key)? else {
            continue;
        };
        if list.is_empty() {
            continue;
        }
        client.reply(Reply::Array(2));
        client.reply(key.clone());
        let count = min(count, list.len());
        client.reply(Reply::Array(count));
        for element in list.iter_from(edge).take(count) {
            client.reply(element);
        }
        list.trim(edge, count, max);
        if list.is_empty() {
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

fn pop(client: &mut Client, store: &mut Store, edge: Edge) -> CommandResult {
    let max = store.list_max_listpack_size;
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let list = db.mut_list(&key)?.ok_or(Reply::Nil)?;
    let len = list.len();

    if client.request.is_empty() {
        client.reply(list.peek(edge));
        list.trim(edge, 1, max);
    } else {
        let count = client.request.usize().map_err(|_| ReplyError::Integer)?;
        let count = min(count, list.len());
        client.reply(Reply::Array(count));
        for element in list.iter_from(edge).take(count) {
            client.reply(element);
        }
        list.trim(edge, count, max);
    }

    let modified = list.len() != len;

    if list.is_empty() {
        db.remove(&key);
    }

    if modified {
        store.touch(client.db(), &key);
    }

    Ok(None)
}

pub static LPOP: Command = Command {
    kind: CommandKind::Lpop,
    name: "lpop",
    arity: Arity::Minimum(2),
    run: lpop,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lpop(client: &mut Client, store: &mut Store) -> CommandResult {
    pop(client, store, Edge::Left)
}

pub static LPOS: Command = Command {
    kind: CommandKind::Lpos,
    name: "lpos",
    arity: Arity::Minimum(3),
    run: lpos,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum LposOption {
    #[regex(b"(?i:count)")]
    Count,

    #[regex(b"(?i:maxlen)")]
    Maxlen,

    #[regex(b"(?i:rank)")]
    Rank,
}

fn lpos(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let element = client.request.pop()?;
    let mut count = None;
    let mut edge = Edge::Left;
    let mut maxlen = 0;
    let mut rank = 1;
    let mut seen = 0;
    let mut sender = None;
    let mut sent = 0;

    while !client.request.is_empty() {
        use LposOption::*;
        match lex(&client.request.pop()?[..]) {
            Some(Count) => {
                count = Some(client.request.integer()?);
            }
            Some(Maxlen) => {
                maxlen = client.request.integer()?;
            }
            Some(Rank) => {
                (edge, rank) = integer_with_edge(client)?;
            }
            _ => return Err(ReplyError::Syntax.into()),
        }
    }

    let db = store.get_db(client.db())?;
    let Some(list) = db.get_list(&key)? else {
        if count.is_some() {
            return Err(Reply::Array(0));
        } else {
            return Err(Reply::Nil);
        }
    };
    let len = list.len();
    if maxlen == 0 {
        maxlen = len;
    }

    if count.is_some() {
        let channel = oneshot::channel();
        sender = Some(channel.0);
        client.reply(Reply::DeferredArray(channel.1));
    }

    for (index, value) in list.iter_from(edge).enumerate().take(maxlen) {
        if !element.pack_eq(&value) {
            continue;
        }

        seen += 1;
        if seen < rank {
            continue;
        }

        sent += 1;
        if edge == Edge::Right {
            client.reply(len - index - 1);
        } else {
            client.reply(index);
        }

        if Some(sent) == count {
            break;
        }

        if count.is_none() {
            return Ok(None);
        }
    }

    if let Some(sender) = sender {
        _ = sender.send(sent);
    } else {
        client.reply(Reply::Nil);
    }
    Ok(None)
}

fn push(client: &mut Client, store: &mut Store, edge: Edge) -> CommandResult {
    let max = store.list_max_listpack_size;
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let list = db.entry_ref(&key).or_insert_with(Value::list).mut_list()?;

    for value in client.request.iter() {
        list.push(&&value[..], edge, max);
    }

    let len = list.len();
    store.touch(client.db(), &key);
    store.mark_ready(client.db(), &key);

    client.reply(len);
    Ok(None)
}

pub static LPUSH: Command = Command {
    kind: CommandKind::Lpush,
    name: "lpush",
    arity: Arity::Minimum(3),
    run: lpush,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lpush(client: &mut Client, store: &mut Store) -> CommandResult {
    push(client, store, Edge::Left)
}

pub static LPUSHX: Command = Command {
    kind: CommandKind::Lpushx,
    name: "lpushx",
    arity: Arity::Minimum(3),
    run: lpushx,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lpushx(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    client.request.reset(1);
    if !store.get_db(client.db())?.exists(&key) {
        return Err(0.into());
    }

    push(client, store, Edge::Left)
}

pub static LRANGE: Command = Command {
    kind: CommandKind::Lrange,
    name: "lrange",
    arity: Arity::Exact(4),
    run: lrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn lrange(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let start = client.request.i64()?;
    let end = client.request.i64()?;
    let db = store.get_db(client.db())?;
    let list = db.get_list(&key[..])?.ok_or(Reply::Array(0))?;
    let range = slice(list.len(), start, end).ok_or(Reply::Array(0))?;
    let len = range.end - range.start;

    client.reply(Reply::Array(len));

    for value in list.iter().skip(range.start).take(len) {
        client.reply(value);
    }

    Ok(None)
}

pub static LREM: Command = Command {
    kind: CommandKind::Lrem,
    name: "lrem",
    arity: Arity::Exact(4),
    run: lrem,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lrem(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let (edge, count) = integer_with_edge(client)?;
    let element = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let list = db.mut_list(&key)?.ok_or(0)?;

    let result = list.remove(element, count, edge);

    if result > 0 {
        store.touch(client.db(), &key);
    }

    client.reply(result);
    Ok(None)
}

pub static LSET: Command = Command {
    kind: CommandKind::Lset,
    name: "lset",
    arity: Arity::Exact(4),
    run: lset,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn lset(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let index = client.request.usize()?;
    let value = client.request.pop()?;
    let list = store
        .mut_db(client.db())?
        .mut_list(&key)?
        .ok_or(ReplyError::NoSuchKey)?;

    if list.set(&value[..], index) {
        store.touch(client.db(), &key);
        client.reply("OK");
    } else {
        client.reply(ReplyError::IndexOutOfRange);
    }

    Ok(None)
}

pub static LTRIM: Command = Command {
    kind: CommandKind::Ltrim,
    name: "ltrim",
    arity: Arity::Exact(4),
    run: ltrim,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn ltrim(client: &mut Client, store: &mut Store) -> CommandResult {
    let max = store.list_max_listpack_size;
    let key = client.request.pop()?;
    let start = client.request.i64()?;
    let end = client.request.i64()?;
    let db = store.mut_db(client.db())?;
    let list = db.mut_list(&key)?.ok_or("OK")?;
    let len = list.len();
    let range = slice(len, start, end).ok_or("OK")?;

    // Is the list changed?
    if range.contains(&0) && range.contains(&(len - 1)) {
        client.reply("OK");
        return Ok(None);
    }

    list.trim(Edge::Right, len.saturating_sub(range.end), max);
    list.trim(Edge::Left, range.start, max);
    if list.is_empty() {
        db.remove(&key);
    }

    store.touch(client.db(), &key);
    client.reply("OK");
    Ok(None)
}

pub static RPOP: Command = Command {
    kind: CommandKind::Rpop,
    name: "rpop",
    arity: Arity::Minimum(2),
    run: rpop,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn rpop(client: &mut Client, store: &mut Store) -> CommandResult {
    pop(client, store, Edge::Right)
}

pub static RPUSH: Command = Command {
    kind: CommandKind::Rpush,
    name: "rpush",
    arity: Arity::Minimum(3),
    run: rpush,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn rpush(client: &mut Client, store: &mut Store) -> CommandResult {
    push(client, store, Edge::Right)
}

pub static RPUSHX: Command = Command {
    kind: CommandKind::Rpushx,
    name: "rpushx",
    arity: Arity::Minimum(3),
    run: rpushx,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn rpushx(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    client.request.reset(1);
    if !store.get_db(client.db())?.exists(&key) {
        return Err(0.into());
    }

    push(client, store, Edge::Right)
}
