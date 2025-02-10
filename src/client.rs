mod addr;
mod id;
mod info;
mod replier;
mod reply_message;

pub use addr::Addr;
pub use id::ClientId;
pub use info::ClientInfo;
pub use replier::Replier;
pub use reply_message::ReplyMessage;

use crate::{
    epoch, request::Request, BlockResult, BulkReply, Command, DBIndex, Reply, ReplyError, Store,
    StoreMessage, StringValue, TaskHandle,
};
use bytes::Bytes;
use respite::{RespConfig, RespReader, RespRequest, RespVersion};
use std::{
    collections::VecDeque,
    io::Write,
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
        Mutex,
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    select,
    sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    },
};
use triomphe::Arc;
use web_time::{Duration, Instant};

#[cfg(feature = "tokio-runtime")]
use tokio::task::JoinHandle;

pub enum Argument {
    Push(Bytes),
    End,
}

/// Should the client send replies or not?
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ReplyMode {
    /// Send all replies.
    On,

    /// Send no replies.
    Off,

    /// Skip the next reply, then turn them back on.
    Skip,
}

/// The current timeout task
#[derive(Debug)]
#[cfg(feature = "tokio-runtime")]
struct Timeout {
    /// Has this timeout been canceled?
    canceled: Arc<AtomicBool>,

    /// The task for sending a timeout message.
    task: JoinHandle<()>,
}

#[cfg(feature = "tokio-runtime")]
impl Timeout {
    /// Abort the task and mark this timeout as canceled to skip an existing message.
    fn cancel(&mut self) {
        self.canceled.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

/// The transaction state of a client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tx {
    /// Transaction error with the number of queued commands.
    Error(usize),

    /// No transaction.
    None,

    /// In a transaction with the number of queued commands.
    Some(usize),
}

/// The client! The place where everything intersects. Stores channels connecting the reader and
/// writer tasks. Stores atomics and locks for updating the store with information across threads.
/// Handles waiting for input, loading and running requests, shutting down related tasks, and
/// notifying the store of disconnection.
pub struct Client {
    /// The client's address.
    pub addr: Option<Addr>,

    /// A channel for receiving requests
    requests: mpsc::UnboundedReceiver<RespRequest>,

    /// The next request to process, already read from the channel.
    next_request: Option<RespRequest>,

    /// Is this client currently blocking? Shared with the store.
    blocking: Arc<AtomicBool>,

    /// The current database, shared with the store
    db: Arc<AtomicUsize>,

    /// Current monitor state, shared with the store
    monitor: Arc<AtomicBool>,

    /// The client id
    pub id: ClientId,

    /// A channel to listen for quit requests
    quit_receiver: oneshot::Receiver<()>,

    /// The client name, shared with the store
    pub name: Option<StringValue>,

    /// A channel for sending messages to the store
    store_sender: mpsc::UnboundedSender<StoreMessage>,

    /// A channel for sending replies
    pub reply_sender: mpsc::UnboundedSender<ReplyMessage>,

    /// Current transaction status
    tx: Tx,

    /// Are we currently running a multi transaction?
    pub in_exec: bool,

    /// The current request
    pub request: Request,

    /// A queue of commands to be executed with EXEC
    pub queue: VecDeque<Argument>,

    /// Are we currently running a script?
    scripting: bool,

    /// A buffer for storing script replies during a command
    pub scripting_reply: VecDeque<Reply>,

    /// Are we currently subscribed to any channels/patterns?
    pub pubsub: bool,

    /// The current RESP protocol version
    protocol: RespVersion,

    /// The current reply mode
    reply_mode: ReplyMode,

    /// Current multi state, shared with the store
    multi: Arc<AtomicIsize>,

    /// Current protocol version, shared with the store
    resp: Arc<AtomicU8>,

    /// The number of subscribed channels, shared with the store
    pub subscribers: Arc<AtomicUsize>,

    /// The number of subscribed patterns, shared with the store
    pub psubscribers: Arc<AtomicUsize>,

    /// The last command run by the client, shared with the store
    last_command: Arc<AtomicPtr<Command>>,

    /// The reader task
    reader_task: TaskHandle<()>,

    #[cfg(feature = "tokio-runtime")]
    /// The current timeout
    timeout: Option<Timeout>,
}

impl Client {
    /// Create a new client and wait for input
    pub fn spawn<S: AsyncRead + AsyncWrite + Send + 'static>(
        stream: S,
        store_sender: mpsc::UnboundedSender<StoreMessage>,
        config: RespConfig,
        addr: Option<Addr>,
    ) {
        // Set up various channels
        let (reader, writer) = tokio::io::split(stream);
        let (quit_sender, quit_receiver) = oneshot::channel();
        let (request_sender, request_receiver) = mpsc::unbounded_channel();
        let quit_sender = Arc::new(Mutex::new(Some(quit_sender)));

        // Spawn the reader
        let mut reader = RespReader::new(reader, config);
        let reader_task = crate::spawn_with_handle(async move {
            reader
                .requests(|request| {
                    _ = request_sender.send(request);
                })
                .await;
        });

        // Spawn the replier
        let reply_sender = Replier::spawn(writer, quit_sender.clone());

        // Create shared info state
        let db = Arc::new(AtomicUsize::new(0));
        let id = ClientId::next();
        let multi = Arc::new(AtomicIsize::new(-1));
        let subscribers = Arc::new(AtomicUsize::new(0));
        let psubscribers = Arc::new(AtomicUsize::new(0));
        let last_command = Arc::new(AtomicPtr::new(ptr::null_mut()));
        let protocol = RespVersion::V2;
        let resp = Arc::new(AtomicU8::new(protocol.into()));
        let monitor = Arc::new(AtomicBool::new(false));
        let blocking = Arc::new(AtomicBool::new(false));

        // Create an info instance
        let info = ClientInfo {
            addr,
            blocking: blocking.clone(),
            id,
            quit_sender,
            reply_sender: reply_sender.clone(),
            name: None,
            db: db.clone(),
            created_at: Instant::now(),
            multi: multi.clone(),
            subscribers: subscribers.clone(),
            psubscribers: psubscribers.clone(),
            last_command: last_command.clone(),
            resp: resp.clone(),
            monitor: monitor.clone(),
        };

        // Notify the store about the connection
        let message = StoreMessage::Connect(info);
        _ = store_sender.send(message);

        // Create the client
        let client = Client {
            addr,
            blocking,
            requests: request_receiver,
            next_request: None,
            db,
            id,
            quit_receiver,
            name: None,
            store_sender,
            reply_sender,
            tx: Tx::None,
            multi,
            in_exec: false,
            request: Request::default(),
            queue: VecDeque::new(),
            scripting: false,
            scripting_reply: VecDeque::new(),
            pubsub: false,
            protocol,
            reply_mode: ReplyMode::On,
            subscribers,
            psubscribers,
            last_command,
            resp,
            monitor,
            reader_task,
            #[cfg(feature = "tokio-runtime")]
            timeout: None,
        };

        // Wait for the first request
        client.wait();
    }

    /// Set a transaction error and clear any queued requests.
    fn error(&mut self) {
        if let Tx::Some(len) = self.tx {
            self.set_tx(Tx::Error(len));
        }
        self.queue.clear();
    }

    /// Discard the current multi transaction
    pub fn discard(&mut self, store: &mut Store) {
        self.set_tx(Tx::None);
        self.queue.clear();
        store.unwatch(self.id);
    }

    /// Get the currently selected database index.
    pub fn db(&self) -> DBIndex {
        DBIndex(self.db.load(Ordering::Relaxed))
    }

    /// Set the currently selected database index.
    /// Also updates the associated `ClientInfo`.
    pub fn set_db(&mut self, db: DBIndex) {
        self.db.store(db.0, Ordering::Relaxed);
    }

    /// Get the current monitor state
    pub fn monitor(&self) -> bool {
        self.monitor.load(Ordering::Relaxed)
    }

    /// Set the current monitor state
    pub fn set_monitor(&mut self, monitor: bool) {
        self.monitor.store(monitor, Ordering::Relaxed);
    }

    /// Set the current reply mode and notify the replier
    pub fn set_reply_mode(&mut self, reply_mode: ReplyMode) {
        if self.reply_mode != reply_mode {
            let message = ReplyMessage::On(reply_mode == ReplyMode::On);
            _ = self.reply_sender.send(message);
        }
        self.reply_mode = reply_mode;
    }

    /// Get the current transaction state.
    pub fn tx(&self) -> Tx {
        self.tx
    }

    /// Set the current transaction state.
    /// Also updates the associated `ClientInfo`.
    pub fn set_tx(&mut self, tx: Tx) -> Tx {
        let replaced = self.tx;
        self.tx = tx;
        let multi = match self.tx {
            Tx::Error(count) | Tx::Some(count) => isize::try_from(count).unwrap_or(isize::MAX),
            Tx::None => -1isize,
        };
        self.multi.store(multi, Ordering::Relaxed);
        replaced
    }

    /// Set the current protocol version, updating the replier and the store
    pub fn set_protocol(&mut self, version: RespVersion) {
        self.protocol = version;
        self.resp.store(version.into(), Ordering::Relaxed);
        _ = self.reply_sender.send(version.into());
    }

    /// Is the client currently using the Resp3 protocol?
    pub fn v3(&self) -> bool {
        self.protocol == RespVersion::V3
    }

    /// Is this client currently waiting on a blocking operation?
    pub fn is_blocked(&self) -> bool {
        self.blocking.load(Ordering::Relaxed)
    }

    /// Stop processing requests and drop.
    pub fn quit(&mut self) {
        if !self.is_quitting() {
            self.quit_receiver.close();
            // No more replies after quitting.
            _ = self.reply_sender.send(ReplyMessage::Quit);
        }
    }

    /// Is this client currently quitting?
    fn is_quitting(&mut self) -> bool {
        let result = self.quit_receiver.try_recv();
        !matches!(result, Err(TryRecvError::Empty))
    }

    /// Is this client currently in resp2 PUBSUB mode?
    pub fn pubsub_mode(&mut self) -> bool {
        self.pubsub && self.protocol == RespVersion::V2
    }

    /// Send a reply to the appropriate location, either the client or the scripting interpreter.
    pub fn reply(&mut self, reply: impl Into<Reply>) {
        if self.scripting {
            self.scripting_reply.push_back(reply.into());
        } else {
            _ = self.reply_sender.send(reply.into().into());
        }
    }

    /// Send an array reply for an iterator with an exact size.
    pub fn array<I, T>(&mut self, iter: I)
    where
        T: Into<Reply>,
        I: Iterator<Item = T> + ExactSizeIterator,
    {
        self.reply(Reply::Array(iter.len()));
        for reply in iter {
            self.reply(reply);
        }
    }

    /// Send an array reply for an iterator without an exact size.
    pub fn deferred_array<I, T>(&mut self, iter: I)
    where
        T: Into<Reply>,
        I: Iterator<Item = T>,
    {
        let (sender, receiver) = oneshot::channel();
        self.reply(Reply::DeferredArray(receiver));
        let count = iter.map(|reply| self.reply(reply)).count();
        _ = sender.send(count);
    }

    /// Send a map reply for an iterator without an exact size.
    pub fn deferred_map<I, K, V>(&mut self, iter: I)
    where
        K: Into<Reply>,
        V: Into<Reply>,
        I: Iterator<Item = (K, V)>,
    {
        let (sender, receiver) = oneshot::channel();
        self.reply(Reply::DeferredMap(receiver));
        let count = iter
            .map(|(k, v)| {
                self.reply(k);
                self.reply(v);
            })
            .count();
        _ = sender.send(count);
    }

    /// Send a bulk reply.
    pub fn bulk(&mut self, reply: impl Into<BulkReply>) {
        self.reply(Reply::Bulk(reply.into()));
    }

    /// Send a verbatim reply.
    pub fn verbatim(&mut self, format: impl Into<Bytes>, value: impl Into<BulkReply>) {
        self.reply(Reply::Verbatim(format.into(), value.into()));
    }

    /// Attempt to receive the next request if not blocked or quitting.
    pub fn try_request(&mut self) -> Option<RespRequest> {
        if self.is_blocked() {
            None
        } else if let Some(message) = self.next_request.take() {
            Some(message)
        } else {
            self.requests.try_recv().ok()
        }
    }

    /// Run the currently loaded request, and then clear it to free space in the request buffer.
    pub fn run(&mut self, store: &mut Store) -> Option<BlockResult> {
        // If the client is in SKIP mode when we begin, turn it off afterward.
        let skipped = self.reply_mode == ReplyMode::Skip;

        // Store the last command.
        let command = self.request.command as *const _ as *mut _;
        self.last_command.store(command, Ordering::Relaxed);

        let block = 'run: {
            if !self.request.is_valid() {
                self.error();
                self.reply(self.request.wrong_arguments());
                break 'run None;
            }

            if self.monitor() && !self.request.command.monitor_allowed() {
                self.reply(ReplyError::Replica);
                break 'run None;
            }

            // If the client is in resp 2 pubsub mode, make sure the command is allowed.
            if self.pubsub_mode() && !self.request.command.pubsub_allowed() {
                self.reply(ReplyError::Pubsub(self.request.command));
                break 'run None;
            }

            // If the command can be queued, check for an active transaction.
            if self.request.command.queueable() {
                match self.tx {
                    // The transaction already failed. Bump the count and bail.
                    Tx::Error(count) => {
                        self.set_tx(Tx::Error(count + 1));
                        self.reply("QUEUED");
                        break 'run None;
                    }

                    // Queue the request and tell the client about it.
                    Tx::Some(count) => {
                        self.set_tx(Tx::Some(count + 1));
                        for argument in self.request.drain() {
                            self.queue.push_back(Argument::Push(argument));
                        }
                        self.queue.push_back(Argument::End);
                        self.reply("QUEUED");
                        break 'run None;
                    }

                    Tx::None => {}
                }
            }

            let block = match (self.request.command.run)(self, store) {
                // The command has already replied.
                Ok(block) => block,

                // The command returned an actual error, so we should clear any queued requests and set
                // a transaction error before replying.
                Err(Reply::Error(reply)) => {
                    self.error();
                    self.reply(reply);
                    None
                }

                // The command returned early, but with a normal reply.
                Err(reply) => {
                    self.reply(reply);
                    None
                }
            };

            self.notify_monitors(store);

            store.numcommands += 1;

            block
        };

        if block.is_none() {
            self.request.clear();
        }

        if skipped {
            self.set_reply_mode(ReplyMode::On);
        }

        block
    }

    /// If quitting, drop. Otherwise, wait for the next actionable event. For example…
    ///
    /// * Receive an unblock message from the store.
    /// * Receive a quit message from the store (i.e. `CLIENT KILL`).
    /// * The timeout for a blocking operation expires.
    /// * Receive a request or error from the arguments task.
    pub fn wait(self) {
        crate::spawn(self.wait_inner());
    }

    #[doc(hidden)]
    async fn wait_inner(mut self) {
        loop {
            select! {
                _ = &mut self.quit_receiver => break,
                message = self.requests.recv() => {
                    match message {
                        Some(RespRequest::Argument(argument)) => {
                            // Push arguments until the request is complete.
                            self.request.push_back(argument);
                        }
                        Some(message) => {
                            // Buffer this message for the store.
                            self.next_request = Some(message);
                            let store_sender = self.store_sender.clone();
                            let message = StoreMessage::Ready(Box::new(self));
                            _ = store_sender.send(message);
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    }

    #[cfg(not(feature = "tokio-runtime"))]
    /// Mark this client as blocked and spawn a timeout if necessary.
    pub fn block(&mut self, _timeout: Duration) {
        self.blocking.store(true, Ordering::Relaxed);
    }

    #[cfg(feature = "tokio-runtime")]
    /// Mark this client as blocked and spawn a timeout if necessary.
    pub fn block(&mut self, timeout: Duration) {
        self.blocking.store(true, Ordering::Relaxed);

        if timeout.is_zero() {
            self.timeout = None;
            return;
        }

        let id = self.id;
        let sleep = tokio::time::sleep(timeout);
        let store_sender = self.store_sender.clone();

        // Use a shared value to ensure that a timeout message is from the most recent blocking
        // operation.
        let canceled = Arc::new(AtomicBool::new(false));

        self.timeout = Some(Timeout {
            canceled: canceled.clone(),
            task: tokio::spawn(async move {
                sleep.await;
                let message = StoreMessage::Timeout(id, canceled);
                _ = store_sender.send(message);
            }),
        });
    }

    // Mark this client unblocked and cancel the timeout.
    pub fn unblock(&mut self) {
        self.request.clear();
        self.blocking.store(false, Ordering::Relaxed);
        #[cfg(feature = "tokio-runtime")]
        if let Some(mut timeout) = self.timeout.take() {
            timeout.cancel();
        }
    }

    /// Process all requests from the queue and then wait.
    pub fn ready(mut self, store: &mut Store) {
        while let Some(message) = self.try_request() {
            if self.is_quitting() {
                return;
            }

            use RespRequest::*;
            match message {
                Argument(argument) => {
                    self.request.push_back(argument);
                }
                End => {
                    if let Some(block) = self.run(store) {
                        store.block(self, block);
                        store.unblock_ready();
                        return;
                    }
                    store.unblock_ready();
                }
                InvalidArgument => {
                    self.reply(ReplyError::InvalidArgument);
                }
                Error(error) => {
                    self.reply(error);
                    self.quit();
                    return;
                }
            }
        }

        self.wait();
    }

    /// Notify monitors of a command.
    pub fn notify_monitors(&mut self, store: &mut Store) {
        // Don't build the reply if the list is empty.
        if store.monitors.is_empty() {
            return;
        }

        // Administrative commands are too dangerous to send.
        if self.request.command.admin {
            return;
        }

        let mut buffer = Vec::new();
        _ = write!(buffer, "{:.6}", epoch().as_secs_f64());

        // TODO: Unix sockets…
        if self.scripting {
            _ = write!(buffer, " [{} lua]", self.db());
        } else if let Some(addr) = self.addr {
            _ = write!(buffer, " [{} {}]", self.db(), addr.peer);
        }

        _ = write!(buffer, " {}", self.request);

        let reply = StringValue::from(buffer);
        for monitor in store.monitors.iter() {
            monitor.reply(Reply::Bulk(reply.clone().into()));
        }
    }
}

impl Drop for Client {
    /// Send messages to stop the reader and clean up store resources.
    fn drop(&mut self) {
        self.reader_task.abort();
        _ = self.store_sender.send(StoreMessage::Disconnect(self.id));
    }
}
