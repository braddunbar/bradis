use crate::Reply;
use respite::RespVersion;

/// A message from a client or the store to a [`crate::client::Replier`].
/// Indicates what to reply with or how to reply.
#[derive(Debug)]
pub enum ReplyMessage {
    /// Indicate what protocol to reply with.
    Protocol(RespVersion),

    /// Turn replies on or off.
    On(bool),

    /// Stop replying.
    Quit,

    /// Send a reply to the client.
    Reply(Reply),
}

impl From<Reply> for ReplyMessage {
    fn from(reply: Reply) -> Self {
        ReplyMessage::Reply(reply)
    }
}

impl From<RespVersion> for ReplyMessage {
    fn from(version: RespVersion) -> Self {
        ReplyMessage::Protocol(version)
    }
}
