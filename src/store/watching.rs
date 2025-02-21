use crate::{
    client::ClientId,
    db::{DBIndex, KeyRef, StringValue},
    linked_hash_set::LinkedHashSet,
    store::DATABASES,
};
use hashbrown::{
    HashMap, HashSet,
    hash_map::{Entry, EntryRef},
};

/// Keep track of which clients are watching which keys and which keys are dirty.
pub struct Watching {
    watchers: Vec<HashMap<StringValue, LinkedHashSet<ClientId>>>,
    clients: HashMap<ClientId, HashSet<(DBIndex, StringValue)>>,
    pub dirty: HashSet<ClientId>,
}

impl Default for Watching {
    fn default() -> Self {
        Watching {
            watchers: vec![HashMap::new(); DATABASES],
            clients: HashMap::new(),
            dirty: HashSet::new(),
        }
    }
}

impl Watching {
    /// Add an entry to find the list of watchers by key, and a reverse entry to find all keys
    /// watched by a particular client for easy removal.
    pub fn add(&mut self, db: DBIndex, key: impl AsRef<[u8]>, id: ClientId) {
        let Some(keys) = self.watchers.get_mut(db.0) else {
            return;
        };
        let entry = keys.entry_ref(key.as_ref());
        let key = if let EntryRef::Occupied(mut entry) = entry {
            entry.get_mut().insert_back(id);
            entry.key().clone()
        } else {
            let mut entry = entry.insert(Default::default());
            entry.get_mut().insert_back(id);
            entry.key().clone()
        };
        self.clients.entry(id).or_default().insert((db, key));
    }

    /// Remove all watched keys for a particular client.
    pub fn remove(&mut self, id: ClientId) {
        let Some(mut keys) = self.clients.remove(&id) else {
            return;
        };

        for (db, key) in keys.drain() {
            let Some(keys) = self.watchers.get_mut(db.0) else {
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

    /// Mark all watchers for a db/key pair as dirty.
    pub fn touch<Q>(&mut self, db: DBIndex, key: &Q)
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        let Some(keys) = self.watchers.get_mut(db.0) else {
            return;
        };
        let Some(ids) = keys.remove(key) else { return };

        for id in ids.iter() {
            self.remove(*id);
            self.dirty.insert(*id);
        }
    }
}
