use crate::{
    CommandResult,
    buffer::ArrayBuffer,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    glob,
    reply::Reply,
    store::Store,
};
use logos::Logos;

pub static PUBSUB: Command = Command {
    kind: CommandKind::Pubsub,
    name: "pubsub",
    arity: Arity::Minimum(2),
    run: pubsub,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: true,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum PubsubSubcommand {
    #[regex(b"(?i:channels)")]
    Channels,

    #[regex(b"(?i:help)")]
    Help,

    #[regex(b"(?i:numpat)")]
    Numpat,

    #[regex(b"(?i:numsub)")]
    Numsub,
}

fn pubsub(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    let subcommand = client.request.pop()?;

    use PubsubSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Channels), 2..=3) => channels,
        (Some(Help), 2) => help,
        (Some(Numpat), 2) => numpat,
        (Some(Numsub), 2..) => numsub,
        _ => return Err(client.request.unknown_subcommand().into()),
    };

    subcommand(client, store)
}

fn help(client: &mut Client, _: &mut Store) -> CommandResult {
    client.verbatim("txt", include_str!("../help/pubsub.txt"));
    Ok(None)
}

fn numpat(client: &mut Client, store: &mut Store) -> CommandResult {
    let reply = store.pubsub.numpat() as i64;
    client.reply(reply);
    Ok(None)
}

fn numsub(client: &mut Client, store: &mut Store) -> CommandResult {
    client.reply(Reply::Array(client.request.remaining() * 2));
    while !client.request.is_empty() {
        let key = client.request.pop()?;
        let count = store.pubsub.numsub(&key);
        client.reply(key);
        client.reply(count as i64);
    }
    Ok(None)
}

fn channels(client: &mut Client, store: &mut Store) -> CommandResult {
    if let Some(pattern) = client.request.try_pop() {
        let mut buffer = ArrayBuffer::default();
        client.deferred_array(store.pubsub.channels().filter(|channel| {
            let bytes = channel.as_bytes(&mut buffer);
            glob::matches(bytes, &pattern)
        }));
    } else {
        client.deferred_array(store.pubsub.channels());
    }
    Ok(None)
}

pub static SUBSCRIBE: Command = Command {
    kind: CommandKind::Subscribe,
    name: "subscribe",
    arity: Arity::Minimum(2),
    run: subscribe,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: true,
    write: false,
};

fn subscribe(client: &mut Client, store: &mut Store) -> CommandResult {
    while !client.request.is_empty() {
        let channel = client.request.pop()?;
        store.pubsub.subscribe(channel, client);
    }
    Ok(None)
}

pub static PSUBSCRIBE: Command = Command {
    kind: CommandKind::Psubscribe,
    name: "psubscribe",
    arity: Arity::Minimum(2),
    run: psubscribe,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: true,
    write: false,
};

fn psubscribe(client: &mut Client, store: &mut Store) -> CommandResult {
    while !client.request.is_empty() {
        let pattern = client.request.pop()?;
        store.pubsub.psubscribe(pattern, client);
    }
    Ok(None)
}

pub static PUBLISH: Command = Command {
    kind: CommandKind::Publish,
    name: "publish",
    arity: Arity::Exact(3),
    run: publish,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: true,
    write: false,
};

fn publish(client: &mut Client, store: &mut Store) -> CommandResult {
    let channel = client.request.pop()?;
    let message = client.request.pop()?;
    let count = store.pubsub.publish(&channel, &message);
    client.reply(count as i64);
    Ok(None)
}

pub static UNSUBSCRIBE: Command = Command {
    kind: CommandKind::Unsubscribe,
    name: "unsubscribe",
    arity: Arity::Minimum(1),
    run: unsubscribe,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: true,
    write: false,
};

fn unsubscribe(client: &mut Client, store: &mut Store) -> CommandResult {
    if client.request.is_empty() {
        store.pubsub.unsubscribe_all(client);
    }
    while !client.request.is_empty() {
        let channel = client.request.pop()?;
        store.pubsub.unsubscribe(channel, client);
    }
    Ok(None)
}

pub static PUNSUBSCRIBE: Command = Command {
    kind: CommandKind::Punsubscribe,
    name: "punsubscribe",
    arity: Arity::Minimum(1),
    run: punsubscribe,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: true,
    write: false,
};

fn punsubscribe(client: &mut Client, store: &mut Store) -> CommandResult {
    if client.request.is_empty() {
        store.pubsub.punsubscribe_all(client);
    }
    while !client.request.is_empty() {
        let pattern = client.request.pop()?;
        store.pubsub.punsubscribe(pattern, client);
    }
    Ok(None)
}
