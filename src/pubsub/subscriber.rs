use crate::{
    client::{ClientId, ReplyMessage},
    reply::Reply,
};
use hashbrown::Equivalent;
use std::hash::{Hash, Hasher};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct Subscriber {
    id: ClientId,
    reply_sender: mpsc::UnboundedSender<ReplyMessage>,
}

impl Eq for Subscriber {}

impl PartialEq for Subscriber {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Hash for Subscriber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Equivalent<Subscriber> for ClientId {
    fn equivalent(&self, key: &Subscriber) -> bool {
        *self == key.id
    }
}

impl Subscriber {
    pub fn new(id: ClientId, reply_sender: mpsc::UnboundedSender<ReplyMessage>) -> Self {
        Subscriber { id, reply_sender }
    }

    pub fn reply(&self, reply: impl Into<Reply>) {
        _ = self.reply_sender.send(reply.into().into());
    }
}
