use crate::{
    client::{ClientId, ReplyMessage},
    reply::Reply,
};
use hashbrown::Equivalent;
use std::hash::{Hash, Hasher};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct Monitor {
    id: ClientId,
    reply_sender: mpsc::UnboundedSender<ReplyMessage>,
}

impl Eq for Monitor {}

impl PartialEq for Monitor {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Hash for Monitor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Equivalent<Monitor> for ClientId {
    fn equivalent(&self, key: &Monitor) -> bool {
        *self == key.id
    }
}

impl Monitor {
    pub fn new(id: ClientId, reply_sender: mpsc::UnboundedSender<ReplyMessage>) -> Self {
        Self { id, reply_sender }
    }

    pub fn reply(&self, reply: impl Into<Reply>) {
        _ = self.reply_sender.send(reply.into().into());
    }
}
