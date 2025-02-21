use crate::{
    Client, CommandResult, ReplyError, Store,
    bytes::lex,
    command::{Arity, Command, CommandKind, Keys},
    config::*,
    glob,
};
use logos::Logos;

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
enum ConfigSubcommand {
    #[regex(b"(?i:get)")]
    Get,

    #[regex(b"(?i:help)")]
    Help,

    #[regex(b"(?i:resetstat)")]
    Resetstat,

    #[regex(b"(?i:set)")]
    Set,
}

pub static CONFIG: Command = Command {
    kind: CommandKind::Config,
    name: "config",
    arity: Arity::Minimum(2),
    run: config,
    keys: Keys::None,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

static CONFIGS: [&Config; 15] = [
    &HASH_MAX_LISTPACK_ENTRIES,
    &HASH_MAX_LISTPACK_VALUE,
    &HASH_MAX_ZIPLIST_ENTRIES,
    &HASH_MAX_ZIPLIST_VALUE,
    &LAZY_EXPIRE,
    &LAZY_USER_DEL,
    &LAZY_USER_FLUSH,
    &LIST_MAX_LISTPACK_SIZE,
    &LIST_MAX_ZIPLIST_SIZE,
    &PROTOMAXBULKLEN,
    &SET_MAX_INTSET_ENTRIES,
    &ZSET_MAX_LISTPACK_ENTRIES,
    &ZSET_MAX_LISTPACK_VALUE,
    &ZSET_MAX_ZIPLIST_ENTRIES,
    &ZSET_MAX_ZIPLIST_VALUE,
];

fn config(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    let subcommand = client.request.pop()?;

    use ConfigSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Get), 3) => get,
        (Some(Help), 2) => help,
        (Some(Resetstat), 2) => resetstat,
        (Some(Set), 4) => set,
        _ => return Err(client.request.unknown_subcommand().into()),
    };

    subcommand(client, store)
}

fn get(client: &mut Client, store: &mut Store) -> CommandResult {
    let pattern = client.request.pop()?;
    let configs = CONFIGS.iter();
    client.deferred_map(configs.filter_map(|config| {
        let bytes = config.name.as_bytes();
        let matches = glob::matches_nocase(bytes, &pattern[..]);
        matches.then(|| (config.name, (config.getter)(store)))
    }));
    Ok(None)
}

fn help(client: &mut Client, _: &mut Store) -> CommandResult {
    client.verbatim("txt", include_str!("../help/config.txt"));
    Ok(None)
}

fn resetstat(client: &mut Client, store: &mut Store) -> CommandResult {
    store.numcommands = 0;
    store.numconnections = 0;
    client.reply("OK");
    Ok(None)
}

fn set(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let value = client.request.pop()?;
    let Some(key) = lex::<ConfigKey>(&key[..]) else {
        return Err(ReplyError::UnsupportedParameter(key).into());
    };

    match (key.config().setter)(&value, store) {
        Ok(()) => {
            client.reply("OK");
            Ok(None)
        }
        Err(error) => Err(ReplyError::ConfigSet(value, key.config(), error).into()),
    }
}
