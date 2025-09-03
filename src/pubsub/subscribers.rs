use crate::{
    client::Client,
    db::{KeyRef, StringValue},
    linked_hash_set::LinkedHashSet,
    pubsub::Subscriber,
};
use hashbrown::{HashMap, HashSet, hash_map::EntryRef};

macro_rules! remove {
    ($map:expr, $key:expr, $value:expr) => {{
        if let EntryRef::Occupied(mut entry) = $map.entry_ref($key) {
            let set = entry.get_mut();
            set.remove($value);
            let len = set.len();
            if set.is_empty() {
                entry.remove();
            }
            len
        } else {
            0
        }
    }};
}

pub struct Subscribers {
    channels: HashMap<StringValue, LinkedHashSet<Subscriber>>,
    subscribers: HashMap<Subscriber, HashSet<StringValue>>,
}

impl Subscribers {
    pub fn new() -> Subscribers {
        Subscribers {
            channels: HashMap::new(),
            subscribers: HashMap::new(),
        }
    }

    /// Add a subscription to a channel for a client
    pub fn add(&mut self, channel: impl AsRef<[u8]>, client: &mut Client) -> usize {
        let subscriber = Subscriber::new(client.id, client.reply_sender.clone());
        let key = self
            .channels
            .get_key_value(channel.as_ref())
            .map_or_else(|| channel.as_ref().into(), |(key, _)| key.clone());
        self.channels
            .entry(key.clone())
            .or_default()
            .insert_back(subscriber.clone());
        let subscribers = self.subscribers.entry(subscriber).or_default();
        subscribers.insert(key);
        subscribers.len()
    }

    /// Remove one channel from a subscriber
    pub fn remove<Q>(&mut self, channel: impl AsRef<[u8]>, subscriber: &Q) -> usize
    where
        Q: KeyRef<Subscriber>,
    {
        remove!(self.channels, channel.as_ref(), subscriber);
        remove!(self.subscribers, subscriber, channel.as_ref())
    }

    /// Remove all subscriptions for a particular subscriber
    pub fn remove_all<Q>(&mut self, subscriber: &Q) -> Option<HashSet<StringValue>>
    where
        Q: KeyRef<Subscriber>,
    {
        let keys = self.subscribers.remove(subscriber)?;

        for key in &keys {
            remove!(self.channels, key, subscriber);
        }

        Some(keys)
    }

    /// Get all subscribers for a channel.
    pub fn get(&self, channel: impl AsRef<[u8]>) -> Option<&LinkedHashSet<Subscriber>> {
        self.channels.get(channel.as_ref())
    }

    /// How many channels does a subscriber have?
    pub fn count<Q>(&self, value: &Q) -> usize
    where
        Q: KeyRef<Subscriber>,
    {
        self.subscribers.get(value).map_or(0, HashSet::len)
    }

    /// Return an iterator over channels and subscribers.
    pub fn iter(&self) -> impl Iterator<Item = (&StringValue, &LinkedHashSet<Subscriber>)> {
        self.channels.iter()
    }

    /// Return an iterator over all channels.
    pub fn channels(&self) -> impl Iterator<Item = &StringValue> {
        self.channels.keys()
    }
}
