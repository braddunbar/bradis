use crate::{
    client::{Addr, Client},
    store::{Store, StoreMessage},
};
use respite::RespConfig;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};

/// The main interface for starting a redis server. The `Default` implementation spawns a server to
/// go with it.
pub struct Server {
    /// The reader config, shared with each client.
    config: RespConfig,

    /// A channel for communicating with the store.
    store_sender: mpsc::UnboundedSender<StoreMessage>,
}

impl Default for Server {
    fn default() -> Self {
        let (store_sender, receiver) = mpsc::unbounded_channel();
        let config = Store::spawn(receiver);
        Server {
            config,
            store_sender,
        }
    }
}

impl Server {
    /// Connect a client to the server with a stream and a source address.
    pub fn connect<S: AsyncRead + AsyncWrite + Send + 'static>(
        &self,
        stream: S,
        addr: Option<Addr>,
    ) {
        let store_sender = self.store_sender.clone();
        Client::spawn(stream, store_sender, self.config.clone(), addr);
    }
}
