use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Addr {
    pub local: SocketAddr,
    pub peer: SocketAddr,
}
