use crate::db::{DB, Value};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum DropMessage {
    DB(DB),
    Value(Value),
}

impl From<DB> for DropMessage {
    fn from(value: DB) -> Self {
        DropMessage::DB(value)
    }
}

impl From<Value> for DropMessage {
    fn from(value: Value) -> Self {
        DropMessage::Value(value)
    }
}

pub fn spawn() -> mpsc::UnboundedSender<DropMessage> {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    crate::spawn(async move {
        while let Some(message) = receiver.recv().await {
            drop(message);
        }
    });
    sender
}
