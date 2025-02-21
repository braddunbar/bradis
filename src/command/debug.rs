use crate::{
    CommandResult,
    bytes::lex,
    client::Client,
    command::{Arity, Command, CommandKind, Keys},
    store::Store,
};
use logos::Logos;

pub static DEBUG: Command = Command {
    kind: CommandKind::Debug,
    name: "debug",
    arity: Arity::Minimum(2),
    run: debug,
    keys: Keys::None,
    readonly: true,
    admin: true,
    noscript: true,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum DebugSubcommand {
    #[regex(b"(?i:log)")]
    Log,
}

fn debug(client: &mut Client, store: &mut Store) -> CommandResult {
    let len = client.request.len();
    let subcommand = client.request.pop()?;

    use DebugSubcommand::*;
    let subcommand = match (lex(&subcommand[..]), len) {
        (Some(Log), _) => debug_log,
        _ => return Err(client.request.unknown_subcommand().into()),
    };
    subcommand(client, store)
}

// TODO: Test this…?
fn debug_log(client: &mut Client, _: &mut Store) -> CommandResult {
    let message = client.request.pop()?;
    let message = std::str::from_utf8(&message).unwrap_or("[invalid utf8]");
    // TODO: Log level
    println!("DEBUG LOG: {message}");
    client.reply("OK");
    Ok(None)
}
