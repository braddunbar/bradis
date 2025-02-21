use crate::{
    CommandResult,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    epoch,
    reply::Reply,
    store::Store,
};
use bytes::Bytes;
use logos::Logos;

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ExpireOption {
    #[regex(b"(?i:nx)")]
    Nx,

    #[regex(b"(?i:xx)")]
    Xx,

    #[regex(b"(?i:gt)")]
    Gt,

    #[regex(b"(?i:lt)")]
    Lt,
}

pub static EXPIRE: Command = Command {
    kind: CommandKind::Expire,
    name: "expire",
    arity: Arity::Minimum(3),
    run: expire,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn expire(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let at = client.request.ttl()?;
    set_expiration(client, store, key, at)
}

pub static EXPIRETIME: Command = Command {
    kind: CommandKind::Expiretime,
    name: "expiretime",
    arity: Arity::Exact(2),
    run: expiretime,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn expiretime(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut result = get_expiretime(client, store)?;
    if result >= 0 {
        result /= 1000;
    }
    client.reply(result);
    Ok(None)
}

pub static EXPIREAT: Command = Command {
    kind: CommandKind::Expireat,
    name: "expireat",
    arity: Arity::Minimum(3),
    run: expireat,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn expireat(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let at = client.request.expiretime()?;
    set_expiration(client, store, key, at)
}

pub static PERSIST: Command = Command {
    kind: CommandKind::Persist,
    name: "persist",
    arity: Arity::Exact(2),
    run: persist,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn persist(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let result = i64::from(db.persist(&key));
    client.reply(result);
    Ok(None)
}

pub static PEXPIRE: Command = Command {
    kind: CommandKind::Pexpire,
    name: "pexpire",
    arity: Arity::Minimum(3),
    run: pexpire,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn pexpire(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let at = client.request.pttl()?;
    set_expiration(client, store, key, at)
}

pub static PEXPIREAT: Command = Command {
    kind: CommandKind::Pexpireat,
    name: "pexpireat",
    arity: Arity::Minimum(3),
    run: pexpireat,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn pexpireat(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let at = client.request.pexpiretime()?;
    set_expiration(client, store, key, at)
}

pub static PEXPIRETIME: Command = Command {
    kind: CommandKind::Pexpiretime,
    name: "pexpiretime",
    arity: Arity::Exact(2),
    run: pexpiretime,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn pexpiretime(client: &mut Client, store: &mut Store) -> CommandResult {
    let result = get_expiretime(client, store)?;
    client.reply(result);
    Ok(None)
}

fn get_expiretime(client: &mut Client, store: &mut Store) -> Result<i64, Reply> {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    Ok(match db.expires_at(&key) {
        Some(time) => i64::try_from(time).unwrap(),
        None if db.exists(&key) => -1,
        None => -2,
    })
}

fn set_expiration(client: &mut Client, store: &mut Store, key: Bytes, at: u128) -> CommandResult {
    let lazy = store.lazy_expire;

    if client.request.remaining() > 1 {
        return Err(client.request.wrong_arguments().into());
    }

    if let Some(option) = client.request.try_pop() {
        let db = store.get_db(client.db())?;
        let expires = db.expires_at(&key[..]);

        use ExpireOption::*;
        let skip = match (lex(&option[..]), expires) {
            (Some(Nx), Some(_)) => true,
            (Some(Xx), None) => true,
            (Some(Gt), None) => true,
            (Some(Gt), Some(x)) if at <= x => true,
            (Some(Lt), Some(x)) if at >= x => true,
            _ => false,
        };

        if skip {
            return Err(0.into());
        }
    }

    let db = store.mut_db(client.db())?;

    if epoch().as_millis() > at {
        if let Some(value) = db.remove(&key) {
            store.drop_value(value, lazy);
            store.touch(client.db(), &key);
            client.reply(1);
        } else {
            client.reply(0);
        }
        return Ok(None);
    }

    if db.expire(&key[..], at) {
        store.touch(client.db(), &key);
        client.reply(1);
    } else {
        client.reply(0);
    }

    Ok(None)
}

pub static TTL: Command = Command {
    kind: CommandKind::Ttl,
    name: "ttl",
    arity: Arity::Exact(2),
    run: ttl,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn ttl(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    get_expiration::<1000>(client, store, &key)
}

pub static PTTL: Command = Command {
    kind: CommandKind::Pttl,
    name: "pttl",
    arity: Arity::Exact(2),
    run: pttl,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn pttl(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    get_expiration::<1>(client, store, &key)
}

fn get_expiration<const UNIT: i64>(
    client: &mut Client,
    store: &mut Store,
    key: &Bytes,
) -> CommandResult {
    let db = store.mut_db(client.db())?;
    let result = match db.ttl(&key[..]) {
        Some(ttl) => i64::try_from(ttl).unwrap() / UNIT,
        None if db.exists(&key[..]) => -1,
        None => -2,
    };

    client.reply(result);
    Ok(None)
}
