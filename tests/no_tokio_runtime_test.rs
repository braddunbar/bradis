#![cfg(not(feature = "tokio-runtime"))]

use bradis::{run_until_stalled, Server};
use futures::executor::block_on;
use respite::{RespReader, RespWriter};
use tokio::io::{duplex, split};

#[test]
fn no_runtime() {
    let server = Server::default();
    let (local, remote) = duplex(100000);
    server.connect(remote, None);
    let (reader, writer) = split(local);
    let mut reader = RespReader::new(reader, Default::default());
    let mut writer = RespWriter::new(writer);
    block_on(writer.write_inline(b"get x")).unwrap();
    run_until_stalled();
    let value = block_on(reader.value());
    assert_eq!(value.unwrap(), Some(respite::RespValue::Nil));
}
