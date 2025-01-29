use crate::{
    buffer::ArrayBuffer,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    epoch,
    reply::{Reply, ReplyError},
    slice::slice,
    store::Store,
    CommandResult,
};
use bytes::Bytes;
use logos::Logos;

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Ttl {
    Ex(u128),
    Exat(u128),
    Keep,
    #[default]
    None,
    Px(u128),
    Pxat(u128),
}

pub static APPEND: Command = Command {
    kind: CommandKind::Append,
    name: "append",
    arity: Arity::Exact(3),
    run: append,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn append(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let bytes = client.request.pop()?;
    let max = store.reader_config.blob_limit();
    let db = store.mut_db(client.db())?;
    let value = db.string_or_default(&key)?;

    if max.saturating_sub(value.len()) < bytes.len() {
        return Err(ReplyError::StringLength.into());
    }

    value.append(&bytes[..]);
    let len = value.len();
    client.reply(len as i64);

    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

pub static DECR: Command = Command {
    kind: CommandKind::Decr,
    name: "decr",
    arity: Arity::Exact(2),
    run: decr,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn decr(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    increment(client, store, key, -1)
}

pub static DECRBY: Command = Command {
    kind: CommandKind::Decrby,
    name: "decrby",
    arity: Arity::Exact(3),
    run: decrby,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn decrby(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let by = client
        .request
        .i64()?
        .checked_neg()
        .ok_or(ReplyError::IncrOverflow)?;
    increment(client, store, key, by)
}

pub static GET: Command = Command {
    kind: CommandKind::Get,
    name: "get",
    arity: Arity::Exact(2),
    run: get,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn get(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let value = db.get_string(&key[..])?.ok_or(Reply::Nil)?;

    client.reply(value);
    Ok(None)
}

pub static GETDEL: Command = Command {
    kind: CommandKind::Getdel,
    name: "getdel",
    arity: Arity::Exact(2),
    run: getdel,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn getdel(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let value = db.get_string(&key)?.ok_or(Reply::Nil)?;
    client.reply(value);
    db.remove(&key);
    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

pub static GETEX: Command = Command {
    kind: CommandKind::Getex,
    name: "getex",
    arity: Arity::Minimum(2),
    run: getex,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Debug, Eq, PartialEq)]
pub enum GetexTtl {
    Ex(u128),
    Exat(u128),
    Persist,
    Px(u128),
    Pxat(u128),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum GetexOption {
    #[regex(b"(?i:ex)")]
    Ex,

    #[regex(b"(?i:exat)")]
    Exat,

    #[regex(b"(?i:persist)")]
    Persist,

    #[regex(b"(?i:px)")]
    Px,

    #[regex(b"(?i:pxat)")]
    Pxat,
}

fn getex(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let mut ttl = None;

    while !client.request.is_empty() {
        let Some(option) = lex(&client.request.pop()?[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        use GetexTtl::*;
        match (option, ttl) {
            (GetexOption::Ex, Some(Ex(_)) | None) => {
                let at = client.request.ttl()?;
                ttl = Some(Ex(at));
            }
            (GetexOption::Exat, Some(Exat(_)) | None) => {
                let at = client.request.expiretime()?;
                ttl = Some(Exat(at));
            }
            (GetexOption::Persist, Some(Persist) | None) => {
                ttl = Some(Persist);
            }
            (GetexOption::Px, Some(Px(_)) | None) => {
                let at = client.request.pttl()?;
                ttl = Some(Px(at));
            }
            (GetexOption::Pxat, Some(Pxat(_)) | None) => {
                let at = client.request.pexpiretime()?;
                ttl = Some(Pxat(at));
            }
            _ => {
                return Err(ReplyError::Syntax.into());
            }
        }
    }

    let db = store.mut_db(client.db())?;
    let value = db.get_string(&key)?.ok_or(Reply::Nil)?.clone();

    if let Some(ttl) = ttl {
        use GetexTtl::*;

        match ttl {
            Ex(at) | Exat(at) | Px(at) | Pxat(at) => {
                if epoch().as_millis() > at {
                    db.remove(&key);
                } else {
                    db.expire(&key, at);
                }
            }
            Persist => {
                db.persist(&key);
            }
        };

        store.dirty += 1;
        store.touch(client.db(), &key);
    }

    client.reply(value);
    Ok(None)
}

pub static GETRANGE: Command = Command {
    kind: CommandKind::Getrange,
    name: "getrange",
    arity: Arity::Exact(4),
    run: getrange,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn getrange(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let start = client.request.i64()?;
    let end = client.request.i64()?;
    let db = store.get_db(client.db())?;
    let value = db.get_string(&key)?.ok_or("")?;
    let mut buffer = ArrayBuffer::default();
    let len = value.as_bytes(&mut buffer).len();
    let range = slice(len, start, end).ok_or("")?;

    client.reply(value.slice(range));
    Ok(None)
}

pub static INCR: Command = Command {
    kind: CommandKind::Incr,
    name: "incr",
    arity: Arity::Exact(2),
    run: incr,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn incr(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    increment(client, store, key, 1)
}

pub static INCRBY: Command = Command {
    kind: CommandKind::Incrby,
    name: "incrby",
    arity: Arity::Exact(3),
    run: incrby,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn incrby(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let by = client.request.i64()?;
    increment(client, store, key, by)
}

pub static INCRBYFLOAT: Command = Command {
    kind: CommandKind::Incrbyfloat,
    name: "incrbyfloat",
    arity: Arity::Exact(3),
    run: incrbyfloat,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn incrbyfloat(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let by = client.request.finite_f64()?;
    let db = store.mut_db(client.db())?;

    let value = db
        .entry_ref(&key)
        .or_insert_with(|| 0f64.into())
        .mut_string()?
        .float()
        .ok_or(ReplyError::Float)?;

    let sum = *value + by;

    if !sum.is_finite() {
        return Err(ReplyError::NanOrInfinity.into());
    }

    *value = sum;
    client.reply(sum);

    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

fn increment(client: &mut Client, store: &mut Store, key: Bytes, by: i64) -> CommandResult {
    let db = store.mut_db(client.db())?;
    let value = db
        .entry_ref(&key)
        .or_insert_with(|| 0i64.into())
        .mut_string()?
        .integer()
        .ok_or(ReplyError::Integer)?;

    *value = value.checked_add(by).ok_or(ReplyError::IncrOverflow)?;
    client.reply(*value);

    store.dirty += 1;
    store.touch(client.db(), &key);

    Ok(None)
}

pub static GETSET: Command = Command {
    kind: CommandKind::Getset,
    name: "getset",
    arity: Arity::Exact(3),
    run: getset,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn getset(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let value = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let original = db.get_string(&key)?.cloned();

    db.set(&key, &value);
    store.dirty += 1;
    store.touch(client.db(), &key);
    client.reply(original);
    Ok(None)
}

pub static MGET: Command = Command {
    kind: CommandKind::Mget,
    name: "mget",
    arity: Arity::Minimum(2),
    run: mget,
    keys: Keys::All,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn mget(client: &mut Client, store: &mut Store) -> CommandResult {
    client.reply(Reply::Array(client.request.remaining()));

    let db = store.get_db(client.db())?;
    while !client.request.is_empty() {
        let key = client.request.pop()?;
        let value = db.get_string(&key[..]).ok().flatten();
        client.reply(value);
    }

    Ok(None)
}

pub static MSET: Command = Command {
    kind: CommandKind::Mset,
    name: "mset",
    arity: Arity::Minimum(3),
    run: mset,
    keys: Keys::Odd,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn mset(client: &mut Client, store: &mut Store) -> CommandResult {
    client.request.assert_pairs()?;

    while !client.request.is_empty() {
        let key = client.request.pop()?;
        let value = client.request.pop()?;
        let db = store.mut_db(client.db())?;
        db.set(&key, value);
        store.dirty += 1;
        store.touch(client.db(), &key);
    }

    client.reply("OK");
    Ok(None)
}

pub static MSETNX: Command = Command {
    kind: CommandKind::Msetnx,
    name: "msetnx",
    arity: Arity::Minimum(3),
    run: msetnx,
    keys: Keys::Odd,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn msetnx(client: &mut Client, store: &mut Store) -> CommandResult {
    client.request.assert_pairs()?;
    let db = store.get_db(client.db())?;
    while !client.request.is_empty() {
        let key = client.request.pop()?;
        _ = client.request.pop()?;
        if db.exists(&key) {
            return Err(0.into());
        }
    }

    client.request.reset(1);

    while !client.request.is_empty() {
        let key = client.request.pop()?;
        let value = client.request.pop()?;
        let db = store.mut_db(client.db())?;
        db.set(&key, value);
        store.dirty += 1;
        store.touch(client.db(), &key);
    }

    client.reply(1);
    Ok(None)
}

pub static PSETEX: Command = Command {
    kind: CommandKind::Psetex,
    name: "psetex",
    arity: Arity::Exact(4),
    run: psetex,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn psetex(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let millis = client.request.u128()?;
    let value = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    db.setex(&key, &value, epoch().as_millis() + millis);
    store.dirty += 1;
    store.touch(client.db(), &key);
    client.reply("OK");
    Ok(None)
}

pub static SET: Command = Command {
    kind: CommandKind::Set,
    name: "set",
    arity: Arity::Minimum(3),
    run: set,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum SetOption {
    #[regex(b"(?i:ex)")]
    Ex,

    #[regex(b"(?i:exat)")]
    Exat,

    #[regex(b"(?i:get)")]
    Get,

    #[regex(b"(?i:keepttl)")]
    Keepttl,

    #[regex(b"(?i:px)")]
    Px,

    #[regex(b"(?i:pxat)")]
    Pxat,

    #[regex(b"(?i:nx)")]
    Nx,

    #[regex(b"(?i:xx)")]
    Xx,
}

fn set(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let value = client.request.pop()?;
    let mut ttl = Ttl::None;
    let mut exists = None;
    let mut get = false;

    while !client.request.is_empty() {
        let Some(option) = lex(&client.request.pop()?[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        use SetOption::*;
        match option {
            Ex if matches!(ttl, Ttl::Ex(_) | Ttl::None) => {
                ttl = Ttl::Ex(client.request.u128()?);
            }
            Exat if matches!(ttl, Ttl::Exat(_) | Ttl::None) => {
                ttl = Ttl::Exat(client.request.u128()?);
            }
            Get => {
                get = true;
            }
            Keepttl if matches!(ttl, Ttl::Keep | Ttl::None) => {
                ttl = Ttl::Keep;
            }
            Nx if exists != Some(true) => {
                exists = Some(false);
            }
            Px if matches!(ttl, Ttl::Px(_) | Ttl::None) => {
                ttl = Ttl::Px(client.request.u128()?);
            }
            Pxat if matches!(ttl, Ttl::Pxat(_) | Ttl::None) => {
                ttl = Ttl::Pxat(client.request.u128()?);
            }
            Xx if exists != Some(false) => {
                exists = Some(true);
            }
            _ => return Err(ReplyError::Syntax.into()),
        }
    }

    let db = store.mut_db(client.db())?;

    match exists {
        Some(false) if !db.exists(&key) => {}
        Some(true) if db.exists(&key) => {}
        None => {}
        _ => return Err(Reply::Nil),
    }

    let previous = get
        .then(|| db.get(&key))
        .flatten()
        .map(|value| value.as_string())
        .transpose()?
        .cloned();

    match ttl {
        Ttl::Ex(s) => db.setex(&key, value, epoch().as_millis() + (s * 1000)),
        Ttl::Exat(at) => db.setex(&key, value, at * 1000),
        Ttl::Keep => db.overwrite(&key, value),
        Ttl::None => db.set(&key, value),
        Ttl::Px(ms) => db.setex(&key, value, epoch().as_millis() + ms),
        Ttl::Pxat(at) => db.setex(&key, value, at),
    };

    store.dirty += 1;
    store.touch(client.db(), &key);

    if get {
        client.reply(previous);
    } else {
        client.reply("OK");
    }

    Ok(None)
}

pub static SETEX: Command = Command {
    kind: CommandKind::Setex,
    name: "setex",
    arity: Arity::Exact(4),
    run: setex,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn setex(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let seconds = client.request.u128()?;
    let value = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    db.setex(&key, &value, epoch().as_millis() + seconds * 1_000);
    store.dirty += 1;
    store.touch(client.db(), &key);
    client.reply("OK");
    Ok(None)
}

pub static SETNX: Command = Command {
    kind: CommandKind::Setnx,
    name: "setnx",
    arity: Arity::Exact(3),
    run: setnx,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn setnx(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let value = client.request.pop()?;
    let db = store.mut_db(client.db())?;

    if db.exists(&key) {
        client.reply(0);
    } else {
        db.set(&key, &value);
        store.dirty += 1;
        store.touch(client.db(), &key);
        client.reply(1);
    }

    Ok(None)
}

pub static SETRANGE: Command = Command {
    kind: CommandKind::Setrange,
    name: "setrange",
    arity: Arity::Exact(4),
    run: setrange,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn setrange(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let start = client.request.usize()?;
    let bytes = client.request.pop()?;

    if start + bytes.len() > store.reader_config.blob_limit() {
        return Err(ReplyError::StringLength.into());
    }

    let db = store.mut_db(client.db())?;
    let value = db.string_or_default(&key)?;

    value.set_range(&bytes[..], start);

    let len = value.len();
    client.reply(len as i64);

    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

pub static STRLEN: Command = Command {
    kind: CommandKind::Strlen,
    name: "strlen",
    arity: Arity::Exact(2),
    run: strlen,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn strlen(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let len = db.get_string(&key)?.ok_or(0)?.len();

    client.reply(len as i64);
    Ok(None)
}
