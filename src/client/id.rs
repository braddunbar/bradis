use std::sync::atomic::{AtomicI64, Ordering::*};

/// The id to be assigned to the next client.
static NEXT_ID: AtomicI64 = AtomicI64::new(0);

/// An id for a [`Client`][`crate::Client`] for formatting and type safety.
/// Should be unique within the server process.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientId(pub i64);

impl ClientId {
    /// Get the next [`ClientId`].
    pub fn next() -> ClientId {
        let update = |x: i64| x.checked_add(1);
        let next = NEXT_ID.fetch_update(Relaxed, Relaxed, update);
        ClientId(next.expect("too many client ids"))
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
