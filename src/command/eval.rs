use crate::{
    Client, CommandResult, Reply, Store,
    command::{Arity, Command, CommandKind, Keys},
};
use piccolo::{Closure, Executor, Lua};

pub static EVAL: Command = Command {
    kind: CommandKind::Eval,
    name: "eval",
    arity: Arity::Minimum(3),
    run: eval,
    keys: Keys::Argument(2),
    readonly: false,
    admin: false,
    noscript: true,
    pubsub: false,
    write: true,
};

fn eval(client: &mut Client, _store: &mut Store) -> CommandResult {
    let code = client.request.pop()?;
    let mut lua = Lua::core();
    let executor = lua
        .try_enter(|context| {
            let closure = Closure::load(context, None, &code[..])?;
            Ok(context.stash(Executor::start(context, closure.into(), ())))
        })
        .unwrap();
    let result = lua.execute::<Reply>(&executor).unwrap();
    client.reply(result);
    Ok(None)
}
