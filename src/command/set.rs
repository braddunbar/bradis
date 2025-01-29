use crate::{
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    reply::{Reply, ReplyError},
    store::Store,
    CommandResult,
};
use std::cmp::min;

pub static SADD: Command = Command {
    kind: CommandKind::Sadd,
    name: "sadd",
    arity: Arity::Minimum(3),
    run: sadd,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn sadd(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let config = store.set_config;
    let db = store.mut_db(client.db())?;
    let set = db.set_or_default(&key)?;
    let mut count = 0;

    for value in client.request.iter() {
        if set.insert(&value[..], &config) {
            count += 1;
        }
    }

    if count > 0 {
        store.dirty += count;
        store.touch(client.db(), &key);
    }

    client.reply(count);
    Ok(None)
}

pub static SCARD: Command = Command {
    kind: CommandKind::Scard,
    name: "scard",
    arity: Arity::Exact(2),
    run: scard,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn scard(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let set = db.get_set(&key[..])?.ok_or(0)?;

    client.reply(set.len());
    Ok(None)
}

pub static SISMEMBER: Command = Command {
    kind: CommandKind::Sismember,
    name: "sismember",
    arity: Arity::Exact(3),
    run: sismember,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn sismember(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let value = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let set = db.get_set(&key)?.ok_or(0)?;
    let result = i64::from(set.contains(&value[..]));

    client.reply(result);
    Ok(None)
}

pub static SMEMBERS: Command = Command {
    kind: CommandKind::Smembers,
    name: "smembers",
    arity: Arity::Exact(2),
    run: smembers,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn smembers(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let set = db.get_set(&key)?.ok_or(Reply::Set(0))?;

    client.reply(Reply::Set(set.len()));
    for item in set.iter() {
        client.reply(item);
    }

    Ok(None)
}

pub static SMISMEMBER: Command = Command {
    kind: CommandKind::Smismember,
    name: "smismember",
    arity: Arity::Minimum(3),
    run: smismember,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn smismember(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;

    let len = client.request.remaining();
    client.reply(Reply::Array(len));

    if let Some(value) = db.get(&key) {
        let set = value.as_set()?;
        while !client.request.is_empty() {
            let item = client.request.pop()?;
            client.reply(i64::from(set.contains(&item[..])));
        }
    } else {
        for _ in 0..len {
            client.reply(0);
        }
    }

    Ok(None)
}

pub static SPOP: Command = Command {
    kind: CommandKind::Spop,
    name: "spop",
    arity: Arity::Minimum(2),
    run: spop,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn spop(client: &mut Client, store: &mut Store) -> CommandResult {
    if client.request.len() > 3 {
        return Err(ReplyError::Syntax.into());
    }

    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let set = db.mut_set(&key)?.ok_or(Reply::Array(0))?;

    if client.request.is_empty() {
        let member = set.pop().ok_or(Reply::Nil)?;
        client.reply(member);
        if set.is_empty() {
            db.remove(&key);
        }
        store.dirty += 1;
        store.touch(client.db(), &key);
        return Ok(None);
    }

    let count = min(client.request.usize()?, set.len());
    client.reply(Reply::Array(count));
    for _ in 0..count {
        let member = set.pop().ok_or(Reply::Nil)?;
        client.reply(member);
    }
    if set.is_empty() {
        db.remove(&key);
    }
    if count > 0 {
        store.dirty += count;
        store.touch(client.db(), &key);
    }

    Ok(None)
}

pub static SREM: Command = Command {
    kind: CommandKind::Srem,
    name: "srem",
    arity: Arity::Minimum(3),
    run: srem,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn srem(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let set = db.set_or_default(&key)?;
    let mut count = 0;

    for value in client.request.iter() {
        if set.remove(&value[..]) {
            count += 1;
        }
    }

    if set.is_empty() {
        db.remove(&key);
    }

    if count > 0 {
        store.dirty += count;
        store.touch(client.db(), &key);
    }

    client.reply(count);
    Ok(None)
}
