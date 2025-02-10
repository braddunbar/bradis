mod blocking;
mod monitor;
mod watching;

use crate::{
    client::{Client, ClientId, ClientInfo},
    db::{DBIndex, KeyRef, StringValue, Value, DB},
    drop::{self, DropMessage},
    linked_hash_set::LinkedHashSet,
    pubsub::Pubsub,
    reply::{Reply, ReplyError},
    BlockResult,
};
use blocking::Blocking;
use bytes::Bytes;
use hashbrown::{hash_map::Entry, HashMap};
pub use monitor::Monitor;
use respite::RespConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use triomphe::Arc;
use watching::Watching;

pub const DATABASES: usize = 16;

/// Large values can be dropped on a separate thread to prevent long pauses.
const MAX_DROP_EFFORT: usize = 64;

/// A message to the store.
pub enum StoreMessage {
    /// A client is ready to execute some commands.
    Ready(Box<Client>),

    /// A client has connected.
    Connect(ClientInfo),

    /// A client has disconnected.
    Disconnect(ClientId),

    /// A blocking client has timed out.
    Timeout(ClientId, Arc<AtomicBool>),
}

/// Configuration for sets.
#[derive(Clone, Copy, Debug)]
pub struct SetConfig {
    /// The maxumum number of entries in an intset
    pub max_intset_entries: usize,

    /// The maxumum number of entries in a listpack encoded set
    pub max_listpack_entries: usize,

    /// The maxumum size of a value in a listpack encoded set
    pub max_listpack_value: usize,
}

/// The store holds all the data for a redis server. It is the
/// representation of the single threaded nature of the server. The
/// sequence of actions carried out by redis is happening wherever
/// store is.
pub struct Store {
    /// Info about all connected clients, keyed by client id.
    pub clients: HashMap<ClientId, ClientInfo>,

    /// All of the databases.
    pub dbs: Vec<DB>,

    /// A channel for dropping values on a separate thread.
    pub drop: mpsc::UnboundedSender<DropMessage>,

    /// The pubsub actions for this store.
    pub pubsub: Pubsub,

    /// The blocking actions for this store.
    pub blocking: Blocking,

    /// A set of monitors to send commands to.
    pub monitors: LinkedHashSet<Monitor>,

    /// The watching actions for this store.
    pub watching: Watching,

    // TODO: Finish implementing this…
    /// The number of changes since the last save.
    pub dirty: usize,

    /// Total commands executed since CONFIG RESETSTAT
    pub numcommands: usize,

    /// Total conncetions accepted since CONFIG RESETSTAT
    pub numconnections: usize,

    /// The maximum number of entries in a listpack hash
    pub hash_max_listpack_entries: usize,

    /// The maximum size of a listpack hash value
    pub hash_max_listpack_value: usize,

    /// The maximum number of entries in a listpack zset
    pub zset_max_listpack_entries: usize,

    /// The maximum size of a listpack zset value
    pub zset_max_listpack_value: usize,

    /// Set configuration
    pub set_config: SetConfig,

    /// Should keys be expired using UNLINK behavior?
    pub lazy_expire: bool,

    /// Should DEL calls use UNLINK behavior by default?
    pub lazy_user_del: bool,

    /// Should FLUSH calls be ASYNC by default?
    pub lazy_user_flush: bool,

    /// What's the maximum listpack size for a list value?
    pub list_max_listpack_size: i64,

    /// Resp reader config.
    pub reader_config: RespConfig,
}

impl Store {
    /// Spawn a store and return its config.
    pub fn spawn(mut store_receiver: mpsc::UnboundedReceiver<StoreMessage>) -> RespConfig {
        let config = RespConfig::default();

        let mut store = Store {
            clients: HashMap::new(),
            dbs: vec![DB::default(); DATABASES],
            drop: drop::spawn(),
            pubsub: Pubsub::default(),
            blocking: Blocking::default(),
            monitors: LinkedHashSet::new(),
            watching: Watching::default(),
            dirty: 0,
            numcommands: 0,
            numconnections: 0,
            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            set_config: SetConfig {
                max_intset_entries: 512,
                max_listpack_entries: 128,
                max_listpack_value: 64,
            },
            lazy_expire: false,
            lazy_user_del: false,
            lazy_user_flush: false,
            list_max_listpack_size: -2,
            reader_config: config.clone(),
        };

        crate::spawn(async move {
            while let Some(message) = store_receiver.recv().await {
                store.message(message);
            }
        });

        config
    }

    /// Get a reference to the database at a particular index.
    pub fn get_db(&self, index: DBIndex) -> Result<&DB, Reply> {
        self.dbs
            .get(index.0)
            .ok_or_else(|| ReplyError::DBIndex.into())
    }

    /// Get a mutable reference to the database at a particular index.
    pub fn mut_db(&mut self, index: DBIndex) -> Result<&mut DB, Reply> {
        self.dbs
            .get_mut(index.0)
            .ok_or_else(|| ReplyError::DBIndex.into())
    }

    /// Check to see if a particular client is dirty.
    pub fn is_dirty(&self, id: ClientId) -> bool {
        self.watching.dirty.contains(&id)
    }

    /// Remove all previously watched keys for a transaction.
    pub fn unwatch(&mut self, id: ClientId) {
        self.watching.remove(id);
        self.watching.dirty.remove(&id);
    }

    /// Mark a key as ready to fulfill blocking requests.
    pub fn mark_ready<Q>(&mut self, db: DBIndex, key: &Q)
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.blocking.mark_ready(db, key);
    }

    /// Mark all clients watching a key as dirty.
    pub fn touch<Q>(&mut self, db: DBIndex, key: &Q)
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.watching.touch(db, key);
    }

    // Handle a message from a client.
    pub fn message(&mut self, message: StoreMessage) {
        use StoreMessage::*;
        match message {
            Connect(info) => self.connect(info),
            Disconnect(id) => self.disconnect(id),
            Ready(client) => client.ready(self),
            Timeout(id, canceled) => {
                if !canceled.load(Ordering::Relaxed) {
                    self.blocking.unblock_with(id, Reply::Nil);
                }
            }
        }
    }

    /// A client has connected, so store some shared info about it.
    fn connect(&mut self, info: ClientInfo) {
        let id = info.id;
        self.numconnections += 1;
        self.clients.insert(id, info);
    }

    /// A client has disconnected, so remove all the tracking data for it.
    fn disconnect(&mut self, id: ClientId) {
        self.blocking.remove(id);
        self.monitors.remove(&id);
        self.pubsub.disconnect(id);
        self.unwatch(id);
        self.clients.remove(&id);
    }

    /// Block this client until the specified keys are ready.
    pub fn block(&mut self, mut client: Client, block: BlockResult) {
        client.block(block.timeout);
        self.blocking.add(client, block.keys);
    }

    /// Iterate over ready keys and serve blocking clients with as many results as possible.
    pub fn unblock_ready(&mut self) {
        // We loop as long as there are more empty keys, which can happen during the process of
        // serving blocked clients (e.g. BLMOVE with clients blocking on the destination).
        while let Some(ready) = self.blocking.ready() {
            // In order to run a command with an exclusive reference to both the client and the store,
            // we need to remove blocking clients from the store.
            let mut clients = self.blocking.take_clients();
            for (index, keys) in ready.iter() {
                for key in keys.iter() {
                    self.unblock_key(&mut clients, *index, key);
                }
            }
            self.blocking.restore_clients(clients);
        }
    }

    /// Serve blocked clients for a particular key with as many results as possible.
    pub fn unblock_key(
        &mut self,
        clients: &mut HashMap<ClientId, Client>,
        index: DBIndex,
        key: &StringValue,
    ) {
        while let Some(id) = self.blocking.front(index, key) {
            let Entry::Occupied(mut entry) = clients.entry(id) else {
                panic!("missing client");
            };

            let client = entry.get_mut();

            // Reset the request before running.
            client.request.reset(1);

            // If the client is still blocking then we're done.
            if client.run(self).is_some() {
                break;
            }

            // Remove the client and return it to the normal queue.
            self.blocking.remove(client.id);
            let mut client = entry.remove();
            client.unblock();
            client.ready(self);
        }
    }

    /// Drop a value, maybe asynchronously.
    pub fn drop_value(&mut self, value: Value, lazy: bool) {
        if lazy && value.drop_effort() > MAX_DROP_EFFORT {
            _ = self.drop.send(value.into());
        } else {
            drop(value);
        }
    }

    /// Set a client name.
    pub fn set_name(&mut self, client: &mut Client, name: Option<Bytes>) {
        let info = self.clients.get_mut(&client.id).unwrap();
        if let Some(name) = name {
            client.name = Some(name.into());
            info.name = client.name.clone();
        } else {
            client.name = None;
            info.name = None;
        }
    }
}
