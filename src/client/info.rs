use crate::{ClientId, Command, Reply, ReplyMessage, Store, StringValue, client::Addr};
use bytes::BufMut;
use std::{
    io::Write,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};
use triomphe::Arc;
use web_time::Instant;

/// Clients are not owned by the store, but the store needs accurate data in several cases.
///
/// * Responding accurately to `CLIENT LIST` or `CLIENT INFO`
/// * Asking a client to quit.
///
/// For this reason, the store and the client share some data. This is only written to during
/// commands, so there is very little lock contention if any.
#[derive(Debug)]
pub struct ClientInfo {
    /// The client address
    pub addr: Option<Addr>,

    /// The currently selected database, shared with the client
    pub db: Arc<AtomicUsize>,

    /// The client id
    pub id: ClientId,

    /// A channel for asking the client to quit
    pub quit_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,

    /// A channel for sending replies
    pub reply_sender: mpsc::UnboundedSender<ReplyMessage>,

    /// Is this client currently blocking?
    pub blocking: Arc<AtomicBool>,

    /// The client name, shared with the client
    pub name: Option<StringValue>,

    /// The instant the client was created
    pub created_at: Instant,

    /// The current transaction status, shared with the client
    pub multi: Arc<AtomicIsize>,

    /// The number of subscribed channels, shared with the client
    pub subscribers: Arc<AtomicUsize>,

    /// The number of subscribed patterns, shared with the client
    pub psubscribers: Arc<AtomicUsize>,

    /// The last command run by the client, shared with the client
    pub last_command: Arc<AtomicPtr<Command>>,

    /// Current protocol version, shared with the client
    pub resp: Arc<AtomicU8>,

    /// Current monitor state, shared with the client
    pub monitor: Arc<AtomicBool>,
}

impl ClientInfo {
    /// The number of seconds since connection
    pub fn age(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Ask the client to quit
    pub fn quit(&mut self) {
        let Ok(mut quit) = self.quit_sender.lock() else {
            return;
        };
        let Some(quit) = quit.take() else {
            return;
        };
        _ = quit.send(());
        // No more replies after quitting.
        _ = self.reply_sender.send(ReplyMessage::Quit);
    }

    /// Send a reply to the client
    pub fn reply(&mut self, reply: impl Into<Reply>) {
        _ = self.reply_sender.send(reply.into().into());
    }

    /// Write client info to a buffer
    pub fn write_info(&self, store: &Store, buffer: &mut Vec<u8>) {
        let db = self.db.load(Ordering::Relaxed);
        let multi = self.multi.load(Ordering::Relaxed);
        let psubscribers = self.psubscribers.load(Ordering::Relaxed);
        let subscribers = self.subscribers.load(Ordering::Relaxed);
        let resp = self.resp.load(Ordering::Relaxed);
        let monitor = self.monitor.load(Ordering::Relaxed);

        _ = write!(buffer, "id={}", self.id);
        _ = write!(buffer, " db={db}");
        _ = write!(buffer, " age={}", self.age());
        _ = write!(buffer, " sub={subscribers}");
        _ = write!(buffer, " psub={psubscribers}");
        _ = write!(buffer, " resp={resp}");

        if let Some(addr) = self.addr {
            _ = write!(buffer, " addr={}", addr.peer);
            _ = write!(buffer, " laddr={}", addr.local);
        }

        buffer.extend_from_slice(b" cmd=");

        // SAFETY: `last_command` is always a `&'static Command` or null.
        let command = self.last_command.load(Ordering::Relaxed);
        if let Some(command) = unsafe { command.as_ref() } {
            buffer.extend_from_slice(command.name.as_bytes());
        }

        buffer.extend_from_slice(b" name=");
        if let Some(ref name) = self.name {
            _ = write!(buffer, "{}", name);
        }

        _ = write!(buffer, " multi={multi}");

        buffer.extend_from_slice(b" flags=");

        if self.blocking.load(Ordering::Relaxed) {
            buffer.put_u8(b'b');
        }

        if subscribers > 0 || psubscribers > 0 {
            buffer.put_u8(b'P');
        }

        if multi != -1 {
            buffer.put_u8(b'x');
        }

        if store.is_dirty(self.id) {
            buffer.put_u8(b'd');
        }

        if monitor {
            buffer.put_u8(b'O');
        }

        buffer.put_u8(b'\n');
    }
}
