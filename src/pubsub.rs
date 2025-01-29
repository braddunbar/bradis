mod subscriber;
mod subscribers;

pub use subscriber::Subscriber;
use subscribers::Subscribers;

use crate::{
    buffer::ArrayBuffer,
    client::{Client, ClientId},
    db::StringValue,
    glob,
    linked_hash_set::LinkedHashSet,
    reply::Reply,
};
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Keep track of pubsub subscribers and what channels they are subscribed to.
pub struct Pubsub {
    /// Clients subscribed to specific channels.
    subscribers: Subscribers,

    /// Clients subscribed to channel patterns.
    psubscribers: Subscribers,
}

impl Default for Pubsub {
    fn default() -> Self {
        Pubsub {
            subscribers: Subscribers::new(),
            psubscribers: Subscribers::new(),
        }
    }
}

impl Pubsub {
    /// The number of patterns subscribed to.
    pub fn numpat(&self) -> usize {
        self.psubscribers.iter().count()
    }

    /// The number of subscribers.
    pub fn numsub(&self, key: impl AsRef<[u8]>) -> usize {
        self.subscribers.get(key).map_or(0, LinkedHashSet::len)
    }

    /// The number of channels subscribed to.
    pub fn channels(&self) -> impl Iterator<Item = &StringValue> {
        self.subscribers.channels()
    }

    /// Disconnect a client, removing all bookkeeping.
    pub fn disconnect(&mut self, id: ClientId) {
        self.subscribers.remove_all(&id);
        self.psubscribers.remove_all(&id);
    }

    /// Reset a client, removing all subscribers.
    pub fn reset(&mut self, client: &mut Client) {
        self.subscribers.remove_all(&client.id);
        self.psubscribers.remove_all(&client.id);
        client.pubsub = false;
    }

    /// The number of subscribers to a specific channel.
    pub fn subscribers(&self, id: ClientId) -> usize {
        self.subscribers.count(&id)
    }

    /// The number of subscribers to a pattern.
    pub fn psubscribers(&self, id: ClientId) -> usize {
        self.psubscribers.count(&id)
    }

    /// Total subscriptions for a client.
    fn count(&self, id: ClientId) -> usize {
        self.subscribers(id) + self.psubscribers(id)
    }

    /// Subscribe a client to a channel.
    pub fn subscribe(&mut self, channel: Bytes, client: &mut Client) {
        let subscribers = self.subscribers.add(&channel, client);
        client.reply(Reply::Push(3));
        client.reply("subscribe");
        client.reply(channel);
        client.reply(self.count(client.id));
        client.pubsub = true;
        client.subscribers.store(subscribers, Ordering::Relaxed);
    }

    /// Subscribe a client to a pattern.
    pub fn psubscribe(&mut self, pattern: Bytes, client: &mut Client) {
        let psubscribers = self.psubscribers.add(&pattern, client);
        client.reply(Reply::Push(3));
        client.reply("psubscribe");
        client.reply(pattern);
        client.reply(self.count(client.id));
        client.pubsub = true;
        client.psubscribers.store(psubscribers, Ordering::Relaxed);
    }

    /// Unsubscribe a client from all channels.
    pub fn unsubscribe_all(&mut self, client: &mut Client) {
        let Some(channels) = self.subscribers.remove_all(&client.id) else {
            client.reply(Reply::Push(3));
            client.reply("unsubscribe");
            client.reply(Reply::Nil);
            client.reply(self.count(client.id));
            return;
        };

        let count = self.count(client.id);
        let len = channels.len();

        for (index, channel) in channels.iter().enumerate() {
            client.reply(Reply::Push(3));
            client.reply("unsubscribe");
            client.reply(channel);
            client.reply(count + len - index - 1);
        }

        if count == 0 {
            client.pubsub = false;
        }
        client.subscribers.store(0, Ordering::Relaxed);
    }

    /// Unsubscribe a client from all patterns.
    pub fn punsubscribe_all(&mut self, client: &mut Client) {
        let Some(patterns) = self.psubscribers.remove_all(&client.id) else {
            client.reply(Reply::Push(3));
            client.reply("punsubscribe");
            client.reply(Reply::Nil);
            client.reply(self.count(client.id));
            return;
        };

        let count = self.count(client.id);
        let len = patterns.len();

        for (index, pattern) in patterns.iter().enumerate() {
            client.reply(Reply::Push(3));
            client.reply("punsubscribe");
            client.reply(pattern);
            client.reply(count + len - index - 1);
        }

        if count == 0 {
            client.pubsub = false;
        }
        client.psubscribers.store(0, Ordering::Relaxed);
    }

    /// Unsubscribe a client from a channel.
    pub fn unsubscribe(&mut self, channel: Bytes, client: &mut Client) {
        let subscribers = self.subscribers.remove(&channel, &client.id);
        let count = self.count(client.id);
        client.reply(Reply::Push(3));
        client.reply("unsubscribe");
        client.reply(channel);
        client.reply(count);

        if count == 0 {
            client.pubsub = false;
        }
        client.subscribers.store(subscribers, Ordering::Relaxed);
    }

    /// Unsubscribe a client from a pattern.
    pub fn punsubscribe(&mut self, pattern: Bytes, client: &mut Client) {
        let psubscribers = self.psubscribers.remove(&pattern, &client.id);
        let count = self.count(client.id);
        client.reply(Reply::Push(3));
        client.reply("punsubscribe");
        client.reply(pattern);
        client.reply(count);

        if count == 0 {
            client.pubsub = false;
        }

        client.psubscribers.store(psubscribers, Ordering::Relaxed);
    }

    /// Publish a message to a channel.
    pub fn publish(&mut self, channel: &Bytes, message: &Bytes) -> usize {
        let mut count = 0;

        if let Some(subscribers) = self.subscribers.get(&channel[..]) {
            count += subscribers.len();

            for subscriber in subscribers.iter() {
                subscriber.reply(Reply::Push(3));
                subscriber.reply("message");
                subscriber.reply(channel);
                subscriber.reply(message);
            }
        }

        for (pattern, subscribers) in self.psubscribers.iter() {
            let mut buffer = ArrayBuffer::default();
            if glob::matches(&channel[..], pattern.as_bytes(&mut buffer)) {
                count += subscribers.len();
                for subscriber in subscribers.iter() {
                    subscriber.reply(Reply::Push(4));
                    subscriber.reply("pmessage");
                    subscriber.reply(pattern);
                    subscriber.reply(channel);
                    subscriber.reply(message);
                }
            }
        }

        count
    }
}
