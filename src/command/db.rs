use crate::{
    CommandResult,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    reply::ReplyError,
    store::Store,
};
use logos::Logos;
use std::mem;

pub static COPY: Command = Command {
    kind: CommandKind::Copy,
    name: "copy",
    arity: Arity::Minimum(3),
    run: copy,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum CopyOption {
    #[regex(b"(?i:db)")]
    Db,

    #[regex(b"(?i:replace)")]
    Replace,
}

fn copy(client: &mut Client, store: &mut Store) -> CommandResult {
    // TODO: Not allowed in cluster mode.

    let source = client.request.pop()?;
    let destination = client.request.pop()?;
    let mut db = client.db();
    let mut replace = false;

    while !client.request.is_empty() {
        let Some(option) = lex(&client.request.pop()?) else {
            return Err(ReplyError::Syntax.into());
        };

        use CopyOption::*;
        match option {
            Db => {
                db = client.request.db_index()?;
            }
            Replace => {
                replace = true;
            }
        }
    }

    if client.db() == db && source == destination {
        return Err(ReplyError::SameObject.into());
    }

    // Check for valid database id.
    let to = store.dbs.get(db.0).ok_or(ReplyError::DBIndex)?;

    // Does the key already exist?
    if !replace && to.exists(&destination) {
        client.reply(0);
        return Ok(None);
    }

    let from = store.mut_db(client.db())?;
    let ttl = from.expires_at(&source);
    let value = from.get(&source).ok_or(0)?.clone();
    let to = store.dbs.get_mut(db.0).ok_or(ReplyError::DBIndex)?;
    if let Some(ttl) = ttl {
        to.setex(&destination, value, ttl);
    } else {
        to.set(&destination, value);
    }
    store.touch(db, &destination);
    client.reply(1);
    Ok(None)
}

pub static DBSIZE: Command = Command {
    kind: CommandKind::Dbsize,
    name: "dbsize",
    arity: Arity::Exact(1),
    run: dbsize,
    keys: Keys::None,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn dbsize(client: &mut Client, store: &mut Store) -> CommandResult {
    let db = store.get_db(client.db())?;
    let size = db.size();
    client.reply(size);
    Ok(None)
}

pub static FLUSHALL: Command = Command {
    kind: CommandKind::Flushall,
    name: "flushall",
    arity: Arity::Minimum(1),
    run: flushall,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum FlushOption {
    #[regex(b"(?i:async)")]
    Async,

    #[regex(b"(?i:sync)")]
    Sync,
}

fn flushall(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut lazy = store.lazy_user_flush;

    if !client.request.is_empty() {
        let argument = client.request.pop()?;
        let Some(option) = lex(&argument[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        use FlushOption::*;
        match option {
            Async => lazy = true,
            Sync => lazy = false,
        }
    }

    for db in &mut store.dbs {
        let db = mem::take(db);
        if lazy {
            _ = store.drop.send(db.into());
        } else {
            drop(db);
        }
    }
    client.reply("OK");
    Ok(None)
}

pub static FLUSHDB: Command = Command {
    kind: CommandKind::Flushdb,
    name: "flushdb",
    arity: Arity::Minimum(1),
    run: flushdb,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn flushdb(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut lazy = store.lazy_user_flush;

    if !client.request.is_empty() {
        let argument = client.request.pop()?;
        let Some(option) = lex(&argument[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        use FlushOption::*;
        match option {
            Async => lazy = true,
            Sync => lazy = false,
        }
    }

    let db = store.mut_db(client.db())?;
    let db = mem::take(db);
    if lazy {
        _ = store.drop.send(db.into());
    } else {
        drop(db);
    }
    client.reply("OK");
    Ok(None)
}

pub static MOVE: Command = Command {
    kind: CommandKind::Move,
    name: "move",
    arity: Arity::Exact(3),
    run: move_,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn move_(client: &mut Client, store: &mut Store) -> CommandResult {
    // TODO: Not allowed in cluster mode.

    let key = client.request.pop()?;
    let db = client.request.db_index()?;

    if client.db() == db {
        return Err(ReplyError::SameObject.into());
    }

    // Check for valid database id.
    let to = store.dbs.get(db.0).ok_or(ReplyError::DBIndex)?;

    // Does the key already exist?
    if to.exists(&key) {
        client.reply(0);
        return Ok(None);
    }

    let from = store.mut_db(client.db())?;
    let ttl = from.expires_at(&key);
    let value = from.remove(&key).ok_or(0)?;
    let to = store.dbs.get_mut(db.0).ok_or(ReplyError::DBIndex)?;
    if let Some(ttl) = ttl {
        to.setex(&key, value, ttl);
    } else {
        to.set(&key, value);
    }
    store.touch(client.db(), &key);
    store.touch(db, &key);
    client.reply(1);
    Ok(None)
}

pub static RENAME: Command = Command {
    kind: CommandKind::Rename,
    name: "rename",
    arity: Arity::Exact(3),
    run: rename,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static RENAMENX: Command = Command {
    kind: CommandKind::Renamenx,
    name: "renamenx",
    arity: Arity::Exact(3),
    run: rename,
    keys: Keys::Double,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn rename(client: &mut Client, store: &mut Store) -> CommandResult {
    let nx = client.request.kind() == CommandKind::Renamenx;
    let from = client.request.pop()?;
    let to = client.request.pop()?;
    let db = store.mut_db(client.db())?;

    if !db.exists(&from) {
        return Err(ReplyError::NoSuchKey.into());
    }

    if from == to {
        if nx {
            client.reply(0);
        } else {
            client.reply("OK");
        }
        return Ok(None);
    }

    if nx && db.exists(&to) {
        return Err(0.into());
    }

    let at = db.expires_at(&from);
    if let Some(value) = db.remove(&from) {
        if let Some(at) = at {
            db.setex(&to, value, at);
        } else {
            db.set(&to, value);
        }
    }

    store.touch(client.db(), &from);
    store.touch(client.db(), &to);

    if nx {
        client.reply(1);
    } else {
        client.reply("OK");
    }
    Ok(None)
}

pub static SELECT: Command = Command {
    kind: CommandKind::Select,
    name: "select",
    arity: Arity::Exact(2),
    run: select,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn select(client: &mut Client, store: &mut Store) -> CommandResult {
    let index = client.request.db_index()?;
    store.dbs.get(index.0).ok_or(ReplyError::DBIndex)?;

    client.set_db(index);
    client.reply("OK");
    Ok(None)
}

pub static SWAPDB: Command = Command {
    kind: CommandKind::Swapdb,
    name: "swapdb",
    arity: Arity::Exact(3),
    run: swapdb,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn swapdb(client: &mut Client, store: &mut Store) -> CommandResult {
    let a = client.request.db_index()?;
    let b = client.request.db_index()?;

    if a.0 >= store.dbs.len() || b.0 >= store.dbs.len() {
        return Err(ReplyError::DBIndex.into());
    }

    store.dbs.swap(a.0, b.0);

    // TODO: Check blocked clients.

    client.reply("OK");
    Ok(None)
}
