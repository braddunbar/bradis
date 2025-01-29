use crate::{
    bytes::lex,
    client::{Argument, Client, ClientId, ReplyMode, Tx},
    command::{Arity, Command, CommandKind, Keys, ALL},
    config::YesNoOption,
    db::DBIndex,
    epoch, glob,
    reply::{Reply, ReplyError},
    store::{Monitor, Store},
    CommandResult, VERSION,
};
use bytes::Bytes;
use logos::Logos;
use respite::RespVersion;
use std::io::Write;

pub static CLIENT: Command = Command {
    kind: CommandKind::Client,
    name: "client",
    arity: Arity::Minimum(2),
    run: client,
    keys: Keys::None,
    readonly: false,
    admin: true,
    noscript: true,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ClientSubcommand {
    #[regex(b"(?i:getname)")]
    Getname,

    #[regex(b"(?i:help)")]
    Help,

    #[regex(b"(?i:id)")]
    Id,

    #[regex(b"(?i:info)")]
    Info,

    #[regex(b"(?i:kill)")]
    Kill,

    #[regex(b"(?i:list)")]
    List,

    #[regex(b"(?i:reply)")]
    Reply,

    #[regex(b"(?i:setname)")]
    Setname,

    #[regex(b"(?i:unblock)")]
    Unblock,
}

fn client(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    let subcommand = client.request.pop()?;

    use ClientSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Getname), 2) => getname,
        (Some(Help), 2) => client_help,
        (Some(Id), 2) => client_id,
        (Some(Info), 2) => client_info,
        (Some(Kill), _) => kill,
        (Some(List), _) => list,
        (Some(Reply), 3) => client_reply,
        (Some(Setname), 3) => setname,
        (Some(Unblock), 3..=4) => unblock,
        _ => return Err(client.request.unknown_subcommand().into()),
    };

    subcommand(client, store)
}

fn client_help(client: &mut Client, _: &mut Store) -> CommandResult {
    client.verbatim("txt", include_str!("../help/client.txt"));
    Ok(None)
}

fn client_id(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(client.id);
    Ok(None)
}

fn client_info(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut buffer = Vec::new();
    let info = store.clients.get(&client.id).unwrap();
    info.write_info(store, &mut buffer);
    client.verbatim("txt", buffer);
    Ok(None)
}

fn getname(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(client.name.clone());
    Ok(None)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ReplyModeOption {
    #[regex(b"(?i:on)")]
    On,

    #[regex(b"(?i:off)")]
    Off,

    #[regex(b"(?i:skip)")]
    Skip,
}

fn client_reply(client: &mut Client, _: &mut Store) -> CommandResult {
    let Some(option) = lex(&client.request.pop()?[..]) else {
        return Err(ReplyError::Syntax.into());
    };

    use ReplyModeOption::*;
    let reply_mode = match option {
        On => ReplyMode::On,
        Off => ReplyMode::Off,
        Skip => ReplyMode::Skip,
    };
    client.set_reply_mode(reply_mode);
    client.reply("OK");
    Ok(None)
}

fn setname(client: &mut Client, store: &mut Store) -> CommandResult {
    let name = client_name(client)?;
    store.set_name(client, name);
    client.reply("OK");
    Ok(None)
}

fn client_name(client: &mut Client) -> Result<Option<Bytes>, ReplyError> {
    let name = client.request.pop()?;

    if name.iter().any(|byte| !(b'!'..=b'~').contains(byte)) {
        return Err(ReplyError::ClientName);
    }

    if name.is_empty() {
        Ok(None)
    } else {
        Ok(Some(name))
    }
}

pub static HELLO: Command = Command {
    kind: CommandKind::Hello,
    name: "hello",
    arity: Arity::Minimum(1),
    run: hello,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum HelloOption {
    #[regex(b"(?i:setname)")]
    Setname,
}

fn hello(client: &mut Client, store: &mut Store) -> CommandResult {
    let version = client.request.usize().map_err(|_| ReplyError::Noproto)?;
    let version = match version {
        2 => RespVersion::V2,
        3 => RespVersion::V3,
        _ => return Err(ReplyError::Noproto.into()),
    };

    while !client.request.is_empty() {
        use HelloOption::*;
        let argument = client.request.pop()?;

        match lex(&argument[..]) {
            Some(Setname) => {
                let name = client_name(client)?;
                store.set_name(client, name);
            }
            None => return Err(ReplyError::Hello(argument).into()),
        }
    }

    client.set_protocol(version);

    client.reply(Reply::Map(4));

    client.reply("server");
    client.reply("bradis");

    client.reply("version");
    client.bulk(VERSION);

    client.reply("proto");
    client.bulk(version);

    client.reply("id");
    client.bulk(client.id.0);

    Ok(None)
}

pub static QUIT: Command = Command {
    kind: CommandKind::Quit,
    name: "quit",
    arity: Arity::Minimum(1),
    run: quit,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn quit(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply("OK");
    client.quit();
    Ok(None)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum KillOption {
    #[regex(b"(?i:addr)")]
    Addr,

    #[regex(b"(?i:id)")]
    Id,

    #[regex(b"(?i:laddr)")]
    Laddr,

    #[regex(b"(?i:skipme)")]
    Skipme,
}

fn kill(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut addr = None;
    let mut id = None;
    let mut laddr = None;
    let mut skipme = true;

    if client.request.remaining() == 1 {
        if let Some(x) = client.request.addr()? {
            addr = Some(x);
        } else {
            return Err(ReplyError::Syntax.into());
        }
    }

    while !client.request.is_empty() {
        let Some(option) = lex(&client.request.pop()?[..]) else {
            return Err(ReplyError::Syntax.into());
        };

        use KillOption::*;
        use YesNoOption::*;
        match option {
            // TODO: TYPE/USER
            Addr => {
                addr = client.request.addr()?;
            }
            Id => {
                id = Some(ClientId(client.request.i64()?));
            }
            Laddr => {
                laddr = client.request.addr()?;
            }
            Skipme => match lex(&client.request.pop()?[..]) {
                Some(Yes) => {
                    skipme = true;
                }
                Some(No) => {
                    skipme = false;
                }
                None => return Err(ReplyError::Syntax.into()),
            },
        }
    }

    // Should the current client quit after replying?
    let mut quit = false;

    let count = store
        .clients
        .values_mut()
        .filter(|other| {
            if skipme && other.id == client.id {
                return false;
            }

            if id == Some(other.id) {
                return true;
            }

            if laddr == Some(other.addr.local) {
                return true;
            }

            if addr == Some(other.addr.peer) {
                return true;
            }

            false
        })
        .map(|other| {
            if other.id == client.id {
                quit = true;
            } else {
                other.quit();
                store.blocking.remove(other.id);
            }
        })
        .count();

    client.reply(count);
    if quit {
        client.quit();
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ListOption {
    #[regex(b"(?i:id)")]
    Id,
}

fn list(client: &mut Client, store: &mut Store) -> CommandResult {
    if client.request.is_empty() {
        let mut buffer = Vec::new();
        for info in store.clients.values() {
            info.write_info(store, &mut buffer);
        }
        client.verbatim("txt", buffer);
        return Ok(None);
    }

    match lex(&client.request.pop()?) {
        Some(ListOption::Id) => {
            let mut buffer = Vec::new();
            while !client.request.is_empty() {
                let id = client.request.client_id()?;
                if let Some(info) = store.clients.get(&id) {
                    info.write_info(store, &mut buffer);
                }
            }
            client.verbatim("txt", buffer);
            Ok(None)
        }
        _ => Err(ReplyError::Syntax.into()),
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum UnblockOption {
    #[regex(b"(?i:error)")]
    Error,

    #[regex(b"(?i:timeout)")]
    Timeout,
}

fn unblock(client: &mut Client, store: &mut Store) -> CommandResult {
    let id = ClientId(client.request.i64()?);
    let mut reply = Reply::Nil;

    if !client.request.is_empty() {
        reply = match lex(&client.request.pop()?[..]) {
            Some(UnblockOption::Error) => ReplyError::Unblocked.into(),
            Some(UnblockOption::Timeout) => Reply::Nil,
            None => return Err(Reply::from(ReplyError::Syntax)),
        };
    }

    if store.blocking.unblock_with(id, reply) {
        client.reply(1);
    } else {
        client.reply(0);
    }
    Ok(None)
}

pub static DISCARD: Command = Command {
    kind: CommandKind::Discard,
    name: "discard",
    arity: Arity::Exact(1),
    run: discard,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn discard(client: &mut Client, store: &mut Store) -> CommandResult {
    client.discard(store);
    client.reply("OK");
    Ok(None)
}

pub static EXEC: Command = Command {
    kind: CommandKind::Exec,
    name: "exec",
    arity: Arity::Exact(1),
    run: exec,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn exec(client: &mut Client, store: &mut Store) -> CommandResult {
    let count = match client.set_tx(Tx::None) {
        Tx::None => return Err(ReplyError::ExecWithoutMulti.into()),
        Tx::Error(_) => {
            client.queue.clear();
            return Err(ReplyError::ExecAbort.into());
        }
        Tx::Some(count) => count,
    };

    if store.is_dirty(client.id) {
        client.queue.clear();
        store.unwatch(client.id);
        return Err(Reply::Nil);
    }

    client.reply(Reply::Array(count));
    client.in_exec = true;
    client.request.clear();

    for _ in 0..count {
        while let Some(Argument::Push(argument)) = client.queue.pop_front() {
            client.request.push_back(argument);
        }
        client.run(store);
    }

    client.queue.clear();
    client.in_exec = false;

    store.unwatch(client.id);
    Ok(None)
}

pub static MULTI: Command = Command {
    kind: CommandKind::Multi,
    name: "multi",
    arity: Arity::Exact(1),
    run: multi,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn multi(client: &mut Client, _: &mut Store) -> CommandResult {
    if !matches!(client.tx(), Tx::None) {
        return Err(ReplyError::MultiNested.into());
    }
    debug_assert!(client.queue.is_empty());
    client.set_tx(Tx::Some(0));
    client.reply("OK");
    Ok(None)
}

pub static WATCH: Command = Command {
    kind: CommandKind::Watch,
    name: "watch",
    arity: Arity::Minimum(2),
    run: watch,
    keys: Keys::All,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn watch(client: &mut Client, store: &mut Store) -> CommandResult {
    if !matches!(client.tx(), Tx::None) {
        client.reply(ReplyError::WatchInMulti);
        return Ok(None);
    }

    if store.is_dirty(client.id) {
        return Err("OK".into());
    }

    while !client.request.is_empty() {
        let key = client.request.pop()?;
        store.watching.add(client.db(), key, client.id);
    }

    client.reply("OK");
    Ok(None)
}

pub static UNWATCH: Command = Command {
    kind: CommandKind::Unwatch,
    name: "unwatch",
    arity: Arity::Exact(1),
    run: unwatch,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn unwatch(client: &mut Client, store: &mut Store) -> CommandResult {
    store.unwatch(client.id);
    client.reply("OK");
    Ok(None)
}

pub static COMMAND: Command = Command {
    kind: CommandKind::Command,
    name: "command",
    arity: Arity::Minimum(1),
    run: command,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum CommandSubcommand {
    #[regex(b"(?i:count)")]
    Count,

    #[regex(b"(?i:getkeys)")]
    Getkeys,

    #[regex(b"(?i:help)")]
    Help,

    #[regex(b"(?i:info)")]
    Info,

    #[regex(b"(?i:list)")]
    List,
}

fn command(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    if len == 1 {
        client.reply(Reply::Array(ALL.len()));
        for command in ALL {
            command_reply(client, command);
        }
        return Ok(None);
    }

    let subcommand = client.request.pop()?;
    use CommandSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Count), 2) => command_count,
        (Some(Getkeys), 3..) => command_getkeys,
        (Some(Help), 2) => command_help,
        (Some(Info), _) => command_info,
        (Some(List), _) => command_list,
        _ => return Err(client.request.unknown_subcommand().into()),
    };

    subcommand(client, store)
}

fn command_count(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(ALL.len());
    Ok(None)
}

fn command_getkeys(client: &mut Client, _: &mut Store) -> CommandResult {
    let command = client.request.pop_front().unwrap();
    let getkeys = client.request.pop_front().unwrap();

    if client.request.kind() == CommandKind::Unknown {
        return Err(ReplyError::InvalidCommand.into());
    }

    if !client.request.is_valid() {
        return Err(ReplyError::InvalidNumberOfArguments.into());
    }

    let keys = client.request.keys()?;
    client.reply(Reply::Array(keys.clone().count()));
    for index in keys {
        client.reply(client.request.get(index));
    }

    // Restore arguments for monitors
    client.request.push_front(getkeys);
    client.request.push_front(command);

    Ok(None)
}

fn command_help(client: &mut Client, _: &mut Store) -> CommandResult {
    client.verbatim("txt", include_str!("../help/command.txt"));
    Ok(None)
}

fn command_info(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(Reply::Array(client.request.remaining()));
    for _ in 0..client.request.remaining() {
        let arg = client.request.pop()?;
        match lex::<CommandKind>(&arg[..]) {
            Some(name) => {
                command_reply(client, name.command());
            }
            None => {
                client.reply(Reply::Nil);
            }
        }
    }
    Ok(None)
}

fn command_reply(client: &mut Client, command: &Command) {
    client.reply(Reply::Array(6));
    client.reply(command.name);
    client.reply(&command.arity);

    let flags = [
        (command.readonly, "readonly"),
        (command.admin, "admin"),
        (command.pubsub, "pubsub"),
        (command.noscript, "noscript"),
    ];

    let filtered = flags.iter().filter(|(value, _)| *value);

    client.reply(Reply::Array(filtered.clone().count()));
    for (_, name) in filtered {
        client.reply(*name);
    }

    let (first, last, step) = command.keys.first_last_step();
    client.reply(first);
    client.reply(last);
    client.reply(step);
}

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum FilterBy {
    #[regex(b"(?i:pattern)")]
    Pattern,
}

fn command_list(client: &mut Client, _: &mut Store) -> CommandResult {
    match client.request.len() {
        2 => {
            client.array(ALL.iter().map(|command| command.name));
            return Ok(None);
        }
        5 => {}
        _ => return Err(ReplyError::Syntax.into()),
    }

    let filterby = client.request.pop()?;
    if !filterby.eq_ignore_ascii_case(b"filterby") {
        return Err(ReplyError::Syntax.into());
    }
    match lex(&client.request.pop()?[..]) {
        Some(FilterBy::Pattern) => {
            let pattern = client.request.pop()?;
            client.deferred_array(ALL.iter().filter_map(|command| {
                let name = command.name.as_bytes();
                glob::matches_nocase(name, &pattern).then_some(command.name)
            }));
        }
        _ => return Err(ReplyError::Syntax.into()),
    }
    Ok(None)
}

pub static ECHO: Command = Command {
    kind: CommandKind::Echo,
    name: "echo",
    arity: Arity::Exact(2),
    run: echo,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn echo(client: &mut Client, _: &mut Store) -> CommandResult {
    let value = client.request.pop()?;
    client.reply(value);
    Ok(None)
}

pub static PING: Command = Command {
    kind: CommandKind::Ping,
    name: "ping",
    arity: Arity::Minimum(1),
    run: ping,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn ping(client: &mut Client, _: &mut Store) -> CommandResult {
    if client.request.len() > 2 {
        return Err(client.request.wrong_arguments().into());
    }

    if client.pubsub_mode() {
        client.reply(Reply::Array(2));
        client.reply("pong");
        if client.request.is_empty() {
            client.reply("");
        } else {
            let value = client.request.pop()?;
            client.reply(value);
        }
        return Ok(None);
    }

    if client.request.is_empty() {
        client.reply("PONG");
    } else {
        let value = client.request.pop()?;
        client.reply(value);
    }

    Ok(None)
}

pub static INFO: Command = Command {
    kind: CommandKind::Info,
    name: "info",
    arity: Arity::Minimum(1),
    run: info,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum InfoSection {
    #[regex(b"(?i:all)")]
    All,

    #[regex(b"(?i:clients)")]
    Clients,

    #[regex(b"(?i:cluster)")]
    Cluster,

    #[regex(b"(?i:commandstats)")]
    Commandstats,

    #[regex(b"(?i:cpu)")]
    Cpu,

    #[regex(b"(?i:default)")]
    Default,

    #[regex(b"(?i:errorstats)")]
    Errorstats,

    #[regex(b"(?i:everything)")]
    Everything,

    #[regex(b"(?i:keyspace)")]
    Keyspace,

    #[regex(b"(?i:latencystats)")]
    Latencystats,

    #[regex(b"(?i:memory)")]
    Memory,

    #[regex(b"(?i:modules)")]
    Modules,

    #[regex(b"(?i:persistence)")]
    Persistence,

    #[regex(b"(?i:replication)")]
    Replication,

    #[regex(b"(?i:server)")]
    Server,

    #[regex(b"(?i:stats)")]
    Stats,
}

impl InfoSection {
    fn default(self) -> bool {
        use InfoSection::*;
        matches!(
            self,
            Clients
                | Cluster
                | Cpu
                | Errorstats
                | Keyspace
                | Memory
                | Modules
                | Persistence
                | Replication
                | Server
                | Stats
        )
    }
}

// TODO: Finish implementing this.
fn info(client: &mut Client, store: &mut Store) -> CommandResult {
    let mut buffer = Vec::new();

    macro_rules! info {
        ($($value:expr),+) => {{
            _ = write!(buffer, $( $value ),+);
            _ = write!(buffer, "\r\n");
        }};
    }

    let mut include = |section: InfoSection| {
        // Assume default when no section is provided.
        if client.request.len() == 1 {
            return section.default();
        }

        client.request.reset(1);
        for argument in client.request.iter() {
            use InfoSection::*;
            match lex(&argument) {
                // All sections except modules.
                Some(All) => return section != Modules,

                // All sections, including modules.
                Some(Everything) => return true,

                // Default sections.
                Some(Default) => return section.default(),

                Some(x) if x == section => return true,

                _ => {}
            }
        }

        false
    };

    if include(InfoSection::Server) {
        info!("#Server");
        info!("arch_bits:{}", 8 * std::mem::size_of::<usize>());
        info!("process_id:{}", std::process::id());
        info!("redis_version:{}", VERSION);
        info!("server_time_usec:{}", epoch().as_micros());
    }

    if include(InfoSection::Persistence) {
        info!("#Persistence");
        info!("rdb_changes_since_last_save:{}", store.dirty);
    }

    if include(InfoSection::Stats) {
        info!("#Stats");
        info!("total_connections_received:{}", store.numconnections);
        info!("total_commands_processed:{}", store.numcommands);
    }

    client.verbatim("txt", buffer);

    Ok(None)
}

pub static MONITOR: Command = Command {
    kind: CommandKind::Monitor,
    name: "monitor",
    arity: Arity::Exact(1),
    run: monitor,
    keys: Keys::None,
    readonly: false,
    admin: true,
    noscript: true,
    pubsub: false,
    write: false,
};

fn monitor(client: &mut Client, store: &mut Store) -> CommandResult {
    let reply_sender = client.reply_sender.clone();
    let monitor = Monitor::new(client.id, reply_sender);
    store.monitors.insert_back(monitor);
    client.set_monitor(true);
    client.reply("OK");
    Ok(None)
}

pub static RESET: Command = Command {
    kind: CommandKind::Reset,
    name: "reset",
    arity: Arity::Exact(1),
    run: reset,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: false,
};

fn reset(client: &mut Client, store: &mut Store) -> CommandResult {
    client.discard(store);
    store.set_name(client, None);
    client.set_reply_mode(ReplyMode::On);
    client.set_db(DBIndex(0));
    client.set_protocol(RespVersion::V2);
    store.pubsub.reset(client);
    store.monitors.remove(&client.id);
    client.set_monitor(false);

    // TODO: Remaining resets

    client.reply("RESET");
    Ok(None)
}

pub static UNKNOWN: Command = Command {
    kind: CommandKind::Unknown,
    name: "unknown",
    arity: Arity::Minimum(1),
    run: unknown,
    keys: Keys::None,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn unknown(client: &mut Client, _: &mut Store) -> CommandResult {
    client.reply(ReplyError::UnknownCommand);
    Ok(None)
}
