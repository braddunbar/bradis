use crate::{
    buffer::ArrayBuffer,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    db::{Hash, List, SortedSet, StringValue, Value},
    glob,
    reply::Reply,
    store::Store,
    CommandResult, Set,
};
use logos::Logos;

pub static EXISTS: Command = Command {
    kind: CommandKind::Exists,
    name: "exists",
    arity: Arity::Minimum(2),
    run: exists,
    keys: Keys::All,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn exists(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut count = 0;
    let db = store.get_db(client.db())?;
    for key in client.request.iter() {
        if db.exists(&key) {
            count += 1;
        }
    }
    client.reply(count);
    Ok(None)
}

pub static DEL: Command = Command {
    kind: CommandKind::Del,
    name: "del",
    arity: Arity::Minimum(2),
    run: del,
    keys: Keys::All,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static UNLINK: Command = Command {
    kind: CommandKind::Unlink,
    name: "unlink",
    arity: Arity::Minimum(2),
    run: unlink,
    keys: Keys::All,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn delete(client: &mut Client, store: &mut Store, lazy: bool) -> CommandResult {
    let mut reply = 0;
    for key in client.request.iter() {
        let db = store.mut_db(client.db())?;
        if let Some(value) = db.remove(&key) {
            store.dirty += 1;
            store.drop_value(value, lazy);
            store.touch(client.db(), &key);
            reply += 1;
        }
    }

    client.reply(reply);
    Ok(None)
}

fn del(client: &mut Client, store: &mut Store) -> CommandResult {
    let lazy = store.lazy_user_del;
    delete(client, store, lazy)
}

fn unlink(client: &mut Client, store: &mut Store) -> CommandResult {
    delete(client, store, true)
}

pub static KEYS: Command = Command {
    kind: CommandKind::Keys,
    name: "keys",
    arity: Arity::Exact(2),
    run: keys,
    keys: Keys::None,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn keys(client: &mut Client, store: &mut Store) -> CommandResult {
    let pattern = client.request.pop()?;
    let mut buffer = ArrayBuffer::default();
    client.deferred_array(store.get_db(client.db())?.keys().filter_map(|key| {
        let bytes = key.as_bytes(&mut buffer);
        glob::matches(bytes, &pattern[..]).then_some(key)
    }));
    Ok(None)
}

pub static TYPE: Command = Command {
    kind: CommandKind::Type,
    name: "type",
    arity: Arity::Exact(2),
    run: type_,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn type_(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let result = match store.get_db(client.db())?.get(&key[..]) {
        Some(Value::String(_)) => "string",
        Some(Value::Hash(_)) => "hash",
        Some(Value::List(_)) => "list",
        Some(Value::Set(_)) => "set",
        Some(Value::SortedSet(_)) => "zset",
        None => "none",
    };

    client.reply(result);
    Ok(None)
}

pub static OBJECT: Command = Command {
    kind: CommandKind::Object,
    name: "object",
    arity: Arity::Minimum(2),
    run: object,
    keys: Keys::None,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ObjectSubcommand {
    #[regex(b"(?i:encoding)")]
    Encoding,

    #[regex(b"(?i:freq)")]
    Freq,

    #[regex(b"(?i:help)")]
    Help,

    #[regex(b"(?i:numpat)")]
    Idletime,

    #[regex(b"(?i:refcount)")]
    Refcount,
}

fn object(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    let subcommand = client.request.pop()?;

    use ObjectSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Encoding), _) => object_encoding,
        (Some(Freq), _) => todo!(),
        (Some(Help), 2) => object_help,
        (Some(Idletime), _) => todo!(),
        (Some(Refcount), _) => object_refcount,
        _ => return Err(client.request.unknown_subcommand().into()),
    };

    subcommand(client, store)
}

fn object_encoding(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    // TODO: Use encodings from redis…?
    let encoding = match db.get(&key).ok_or(Reply::Nil)? {
        Value::Hash(hash) => match **hash {
            Hash::HashMap(_) => "hashtable",
            Hash::PackMap(_) => "listpack",
        },
        Value::List(list) => match **list {
            List::Pack(_) => "listpack",
            List::Quick(_) => "quicklist",
        },
        Value::Set(set) => match **set {
            Set::Int(_) => "intset",
            Set::Pack(_) => "listpack",
            Set::Hash(_) => "hashtable",
        },
        Value::SortedSet(set) => match **set {
            SortedSet::Pack(_) => "listpack",
            SortedSet::Skiplist(_, _) => "skiplist",
        },
        Value::String(value) => match value {
            StringValue::Array(..) => "embstr",
            StringValue::Float(_) => "float",
            StringValue::Integer(_) => "int",
            StringValue::Raw(_) => "raw",
        },
    };
    client.reply(encoding);
    Ok(None)
}

fn object_help(client: &mut Client, _: &mut Store) -> CommandResult {
    client.verbatim("txt", include_str!("../help/object.txt"));
    Ok(None)
}

fn object_refcount(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(1);
    Ok(None)
}
