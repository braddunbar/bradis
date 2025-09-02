use crate::{
    CommandResult,
    buffer::ArrayBuffer,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    db::Hash,
    reply::Reply,
    store::Store,
};

pub static HDEL: Command = Command {
    kind: CommandKind::Hdel,
    name: "hdel",
    arity: Arity::Minimum(3),
    run: hdel,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn hdel(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let hash = db.mut_hash(&key)?.ok_or(0)?;

    // TODO: Shink the allocation one time after all deletions?
    let mut count = 0;
    for field in client.request.iter() {
        if hash.remove(&field[..]) {
            count += 1;
        }
    }

    if hash.is_empty() {
        db.remove(&key);
    }

    if count > 0 {
        store.dirty += count;
        store.touch(client.db(), &key);
    }

    client.reply(count);
    Ok(None)
}

pub static HEXISTS: Command = Command {
    kind: CommandKind::Hexists,
    name: "hexists",
    arity: Arity::Exact(3),
    run: hexists,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hexists(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    let result = i64::from(hash.contains_key(&field[..]));

    client.reply(result);
    Ok(None)
}

pub static HGET: Command = Command {
    kind: CommandKind::Hget,
    name: "hget",
    arity: Arity::Exact(3),
    run: hget,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hget(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    let reply: Reply = hash.get(&field[..]).into();
    client.reply(reply);
    Ok(None)
}

pub static HGETALL: Command = Command {
    kind: CommandKind::Hgetall,
    name: "hgetall",
    arity: Arity::Exact(2),
    run: hgetall,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hgetall(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;

    client.reply(Reply::Map(hash.len()));

    for (key, value) in hash.iter() {
        client.reply(key);
        client.reply(value);
    }

    Ok(None)
}

pub static HINCRBY: Command = Command {
    kind: CommandKind::Hincrby,
    name: "hincrby",
    arity: Arity::Exact(4),
    run: hincrby,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn hincrby(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let by = client.request.i64()?;
    let max_len = store.hash_max_listpack_entries;
    let max_size = store.hash_max_listpack_value;
    let db = store.mut_db(client.db())?;
    let hash = db.hash_or_default(&key)?;
    let result = hash.incrby(&field[..], by, max_len, max_size)?;
    client.reply(result);
    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

pub static HINCRBYFLOAT: Command = Command {
    kind: CommandKind::Hincrbyfloat,
    name: "hincrbyfloat",
    arity: Arity::Exact(4),
    run: hincrbyfloat,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn hincrbyfloat(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let by = client.request.f64()?;
    let max_len = store.hash_max_listpack_entries;
    let max_size = store.hash_max_listpack_value;
    let db = store.mut_db(client.db())?;
    let hash = db.hash_or_default(&key)?;
    let result = hash.incrbyfloat(&field[..], by, max_len, max_size)?;
    client.reply(result);
    store.dirty += 1;
    store.touch(client.db(), &key);
    Ok(None)
}

pub static HKEYS: Command = Command {
    kind: CommandKind::Hkeys,
    name: "hkeys",
    arity: Arity::Exact(2),
    run: hkeys,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hkeys(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    client.reply(Reply::Array(hash.len()));
    for key in hash.keys() {
        client.reply(key);
    }
    Ok(None)
}

pub static HLEN: Command = Command {
    kind: CommandKind::Hlen,
    name: "hlen",
    arity: Arity::Exact(2),
    run: hlen,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hlen(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    client.reply(hash.len());
    Ok(None)
}

pub static HMGET: Command = Command {
    kind: CommandKind::Hmget,
    name: "hmget",
    arity: Arity::Minimum(3),
    run: hmget,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hmget(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    client.reply(Reply::Array(client.request.remaining()));
    while !client.request.is_empty() {
        let field = client.request.pop()?;
        let value = hash.get(&field[..]);
        client.reply(value);
    }
    Ok(None)
}

pub static HSET: Command = Command {
    kind: CommandKind::Hset,
    name: "hset",
    arity: Arity::Minimum(4),
    run: hset,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static HMSET: Command = Command {
    kind: CommandKind::Hmset,
    name: "hmset",
    arity: Arity::Minimum(4),
    run: hset,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn hset(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let max_len = store.hash_max_listpack_entries;
    let max_size = store.hash_max_listpack_value;
    client.request.assert_pairs()?;
    let db = store.mut_db(client.db())?;
    let hash = db.hash_or_default(&key)?;

    let mut count = 0;
    while !client.request.is_empty() {
        let key = client.request.pop()?;
        let value = client.request.pop()?;
        if hash.insert(&key[..], &value[..], max_len, max_size) {
            count += 1;
        }
    }

    if count > 0 {
        store.dirty += count;
        store.touch(client.db(), &key);
    }

    if client.request.kind() == CommandKind::Hmset {
        client.reply("OK");
    } else {
        client.reply(count);
    }
    Ok(None)
}

pub static HSETNX: Command = Command {
    kind: CommandKind::Hsetnx,
    name: "hsetnx",
    arity: Arity::Exact(4),
    run: hsetnx,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn hsetnx(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let value = client.request.pop()?;
    let max_len = store.hash_max_listpack_entries;
    let max_size = store.hash_max_listpack_value;
    let db = store.mut_db(client.db())?;

    if let Some(hash) = db.mut_hash(&key)? {
        if hash.contains_key(&field[..]) {
            return Err(0.into());
        }
        hash.insert(&field[..], &value[..], max_len, max_size);
    } else {
        let mut hash = Hash::default();
        hash.insert(&field[..], &value[..], max_len, max_size);
        db.set(&key, hash);
    }

    store.dirty += 1;
    store.touch(client.db(), &key);
    client.reply(1);
    Ok(None)
}

pub static HSTRLEN: Command = Command {
    kind: CommandKind::Hstrlen,
    name: "hstrlen",
    arity: Arity::Exact(3),
    run: hstrlen,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hstrlen(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let field = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    let mut buffer = ArrayBuffer::default();
    let len = hash
        .get(&field[..])
        .map_or(0, |value| value.as_bytes(&mut buffer).len());
    client.reply(len);
    Ok(None)
}

pub static HVALS: Command = Command {
    kind: CommandKind::Hvals,
    name: "hvals",
    arity: Arity::Exact(2),
    run: hvals,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn hvals(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let hash = db.get_hash(&key)?.ok_or(Reply::Nil)?;
    client.reply(Reply::Array(hash.len()));
    for value in hash.values() {
        client.reply(value);
    }
    Ok(None)
}
