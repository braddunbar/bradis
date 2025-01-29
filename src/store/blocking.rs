use crate::{
    client::{Client, ClientId},
    db::{DBIndex, KeyRef, StringValue},
    linked_hash_set::LinkedHashSet,
    reply::Reply,
    store::DATABASES,
};
use hashbrown::{
    hash_map::{Entry, EntryRef},
    HashMap, HashSet,
};
use std::{iter::StepBy, ops::Range};

/// Keep track of blocking clients, the db/key pairs they're waiting for, and keys that are ready.
pub struct Blocking {
    /// Blocked client instances.
    clients: Option<HashMap<ClientId, Client>>,

    /// The set of keys that a particular client is blocked on.
    keys: HashMap<ClientId, HashSet<(DBIndex, StringValue)>>,

    /// A list of queues by key for each database.
    dbs: Vec<HashMap<StringValue, LinkedHashSet<ClientId>>>,

    /// The set of keys that are ready, by database.
    ready: Option<HashMap<DBIndex, LinkedHashSet<StringValue>>>,
}

impl Default for Blocking {
    fn default() -> Self {
        Blocking {
            clients: Some(HashMap::new()),
            keys: HashMap::new(),
            dbs: vec![HashMap::new(); DATABASES],
            ready: None,
        }
    }
}

impl Blocking {
    /// Hold on to the client for re-running a command later.
    ///
    /// # Panics
    /// Panics if `clients` has been removed via `take_clients`.
    pub fn add(&mut self, client: Client, blocking_keys: StepBy<Range<usize>>) {
        // Get the queues for the current database.
        let queues = self.dbs.get_mut(client.db().0).unwrap();

        // Get or insert a set of keys for the client.
        let keys = self.keys.entry(client.id).or_default();

        // Add the client to the queue for each key it's blocked on.
        for index in blocking_keys {
            let key = client.request.get(index).unwrap();
            let mut entry = match queues.entry_ref(&key) {
                EntryRef::Occupied(entry) => entry,
                EntryRef::Vacant(entry) => entry.insert_entry(Default::default()),
            };

            // Add to the queue
            entry.get_mut().insert_back(client.id);

            // Add to the key set for fast removal.
            keys.insert((client.db(), entry.key().clone()));
        }

        self.clients.as_mut().unwrap().insert(client.id, client);
    }

    /// Remove a particular client from the list of blockers.
    pub fn remove(&mut self, id: ClientId) -> Option<Client> {
        // Remove from queues.
        if let Some(mut keys) = self.keys.remove(&id) {
            for (db, key) in keys.drain() {
                let Some(keys) = self.dbs.get_mut(db.0) else {
                    continue;
                };
                let Entry::Occupied(mut entry) = keys.entry(key) else {
                    continue;
                };
                entry.get_mut().remove(&id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }

        self.clients
            .as_mut()
            .and_then(|clients| clients.remove(&id))
    }

    /// Get the first client to be unblocked for a particular key.
    pub fn front<Q>(&mut self, db: DBIndex, key: &Q) -> Option<ClientId>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.dbs.get(db.0)?.get(key)?.front().copied()
    }

    /// Mark a particular key as ready to serve blockers, if there are any blockers for that key.
    pub fn mark_ready<Q>(&mut self, index: DBIndex, key: &Q)
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        let Some(db) = self.dbs.get(index.0) else {
            return;
        };
        let Some((key, _)) = db.get_key_value(key) else {
            return;
        };
        self.ready
            .get_or_insert_with(Default::default)
            .entry(index)
            .or_default()
            .insert_back(key.clone());
    }

    /// Return all keys that are ready, and replace the hash of ready keys.
    pub fn ready(&mut self) -> Option<HashMap<DBIndex, LinkedHashSet<StringValue>>> {
        self.ready.take()
    }

    /// Running a command requires an exclusive reference to client and a store. This presents a
    /// problem for blocked clients because they're owned by the store. To work around this issue
    /// we can remove the clients while we run commands on blocked clients. Attempting to add
    /// clients during this time will cause a panic.
    pub fn take_clients(&mut self) -> HashMap<ClientId, Client> {
        self.clients.take().unwrap()
    }

    /// Restore clients after running requests.
    pub fn restore_clients(&mut self, clients: HashMap<ClientId, Client>) {
        self.clients = Some(clients);
    }

    /// Attempt to unblock a client with a reply, then wait.
    pub fn unblock_with(&mut self, id: ClientId, reply: impl Into<Reply>) -> bool {
        if let Some(mut client) = self.remove(id) {
            client.reply(reply);
            client.unblock();
            client.wait();
            true
        } else {
            false
        }
    }
}
