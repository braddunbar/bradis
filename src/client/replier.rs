use crate::{Reply, ReplyMessage};
use respite::{RespError, RespWriter};
use std::{io::Write as IoWrite, sync::Mutex};
use thiserror::Error;
use tokio::{
    io::{AsyncWrite, BufWriter},
    sync::{
        mpsc,
        oneshot::{self, error::RecvError},
    },
};
use triomphe::Arc;

/// An error during writing replies
#[derive(Debug, Error)]
pub enum ReplierError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Recv(#[from] RecvError),

    #[error(transparent)]
    Resp(#[from] RespError),
}

/// Serializes replies as they're produced, using the correct RESP version.
pub struct Replier<W: AsyncWrite + Unpin> {
    /// A buffer for writing output
    buffer: Vec<u8>,

    /// Are we currently sending requests, or ignoring them?
    on: bool,

    /// Is this client quitting?
    quitting: bool,

    /// A channel to receiver replies from
    reply_receiver: mpsc::UnboundedReceiver<ReplyMessage>,

    /// A writer for sending bytes to the client
    writer: RespWriter<W>,

    /// A oneshot sender to notify the client about errors.
    quit_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl<W: AsyncWrite + Unpin + Send + 'static> Replier<W> {
    /// Create a new Replier and wait for replies
    pub fn spawn(
        writer: W,
        quit_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    ) -> mpsc::UnboundedSender<ReplyMessage> {
        let (reply_sender, reply_receiver) = mpsc::unbounded_channel();
        let replier = Replier {
            buffer: Vec::new(),
            on: true,
            quitting: false,
            reply_receiver,
            writer: RespWriter::new(BufWriter::new(writer)),
            quit_sender,
        };
        crate::spawn(replier.listen());
        reply_sender
    }

    /// Listen for reply messages and handle them as quickly as possible.
    async fn listen(mut self) {
        if self.listen_inner().await.is_err() {
            let Ok(mut quit) = self.quit_sender.lock() else {
                return;
            };
            let Some(quit) = quit.take() else {
                return;
            };
            _ = quit.send(());
        }
    }

    #[doc(hidden)]
    async fn listen_inner(&mut self) -> Result<(), ReplierError> {
        while let Some(message) = self.reply_receiver.recv().await {
            self.message(message).await?;

            // Receive as many messages as possible before flushing the writer.
            while let Ok(message) = self.reply_receiver.try_recv() {
                self.message(message).await?;
            }

            self.writer.flush().await?;
        }
        Ok(())
    }

    /// Handle one reply message
    async fn message(&mut self, message: ReplyMessage) -> Result<(), ReplierError> {
        use ReplyMessage::*;

        match message {
            On(on) => {
                self.on = on;
            }
            Protocol(version) => {
                self.writer.version = version;
            }
            Quit => {
                self.quitting = true;
            }
            Reply(reply) => {
                self.write(reply).await?;
            }
        }
        Ok(())
    }

    /// Write a reply to send to the client
    async fn write(&mut self, reply: Reply) -> Result<(), ReplierError> {
        use Reply::*;

        if !self.on || self.quitting {
            return Ok(());
        }

        match reply {
            Boolean(value) => {
                self.writer.write_boolean(value).await?;
            }
            Nil => {
                self.writer.write_nil().await?;
            }
            Error(error) => {
                self.buffer.clear();
                write!(self.buffer, "{}", error).unwrap();
                self.writer.write_simple_error(&self.buffer[..]).await?;
            }
            Integer(value) => {
                self.writer.write_integer(value).await?;
            }
            Array(len) => {
                self.writer.write_array(len).await?;
            }
            DeferredArray(len) => {
                self.writer.write_array(len.await?).await?;
            }
            Set(len) => {
                self.writer.write_set(len).await?;
            }
            DeferredSet(len) => {
                self.writer.write_set(len.await?).await?;
            }
            Map(len) => {
                self.writer.write_map(len).await?;
            }
            DeferredMap(len) => {
                self.writer.write_map(len.await?).await?;
            }
            Bulk(bulk) => {
                self.buffer.clear();
                let value = bulk.as_bytes(&mut self.buffer);
                self.writer.write_blob_string(value).await?;
            }
            Double(value) => {
                self.writer.write_double(value).await?;
            }
            Verbatim(format, value) => {
                self.buffer.clear();
                let value = value.as_bytes(&mut self.buffer);
                self.writer.write_verbatim(&format, value).await?;
            }
            Bignum(value) => {
                self.writer.write_bignum(&value).await?;
            }
            Push(len) => {
                self.writer.write_push(len).await?;
            }
            Status(status) => {
                self.buffer.clear();
                let value = status.as_bytes(&mut self.buffer);
                self.writer.write_simple_string(value).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(miri))]
#[cfg(feature = "tokio-runtime")]
mod tests {
    use super::*;
    use crate::ReplyError;
    use bytes::Bytes;
    use respite::RespVersion;
    use std::{str::from_utf8, time::Duration};
    use tokio::{
        io::{duplex, AsyncReadExt},
        sync::oneshot,
        time::timeout,
    };

    #[tokio::test]
    async fn notify_client_of_errors() -> Result<(), ReplierError> {
        let (_, remote) = duplex(14);
        let (quit_sender, quit_receiver) = oneshot::channel();
        let (len_sender, len_receiver) = oneshot::channel();
        let quit_sender = Arc::new(Mutex::new(Some(quit_sender)));

        // Cause an error by dropping a deferred array reply.
        let sender = Replier::spawn(remote, quit_sender);
        _ = sender.send(ReplyMessage::Reply(Reply::DeferredArray(len_receiver)));
        drop(len_sender);

        let limit = Duration::from_millis(50);
        timeout(limit, quit_receiver).await.unwrap()?;
        Ok(())
    }

    macro_rules! assert_replies {
        ($reply:expr, $output:expr, $version:expr) => {{
            let (mut local, remote) = duplex(2usize.pow(8));
            let (quit_sender, _) = oneshot::channel();
            let quit_sender = Arc::new(Mutex::new(Some(quit_sender)));
            let sender = Replier::spawn(remote, quit_sender);

            _ = sender.send(ReplyMessage::Protocol($version));
            _ = sender.send(ReplyMessage::Reply($reply.into()));

            // Drop the sender so that the replier task exits
            drop(sender);

            // Read and compare the output
            let mut buffer = Vec::new();
            local.read_to_end(&mut buffer).await?;

            let output = $output;
            match (from_utf8(&buffer), from_utf8(output)) {
                (Ok(a), Ok(b)) => assert_eq!(a, b),
                _ => assert_eq!(buffer, output),
            }
        }};
    }

    macro_rules! assert_v2 {
        ($reply:expr, $output:expr) => {{
            assert_replies!($reply, $output, RespVersion::V2)
        }};
    }

    macro_rules! assert_v3 {
        ($reply:expr, $output:expr) => {{
            assert_replies!($reply, $output, RespVersion::V3)
        }};
    }

    #[tokio::test]
    async fn write_nil() -> Result<(), ReplierError> {
        assert_v2!(Reply::Nil, b"$-1\r\n");
        assert_v3!(Reply::Nil, b"_\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_integer() -> Result<(), ReplierError> {
        assert_v2!(Reply::Integer(-53), b":-53\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_set() -> Result<(), ReplierError> {
        assert_v2!(Reply::Set(3), b"*3\r\n");
        assert_v3!(Reply::Set(3), b"~3\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_syntax_error() -> Result<(), ReplierError> {
        assert_v2!(ReplyError::Syntax, b"-ERR syntax error\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_array() -> Result<(), ReplierError> {
        assert_v2!(Reply::Array(5), b"*5\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_map() -> Result<(), ReplierError> {
        assert_v2!(Reply::Map(5), b"*10\r\n");
        assert_v3!(Reply::Map(5), b"%5\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_push() -> Result<(), ReplierError> {
        assert_v2!(Reply::Push(5), b"*5\r\n");
        assert_v3!(Reply::Push(5), b">5\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_deferred_map() -> Result<(), ReplierError> {
        let (sender, receiver) = oneshot::channel();
        _ = sender.send(5);
        assert_v2!(Reply::DeferredMap(receiver), b"*10\r\n");
        let (sender, receiver) = oneshot::channel();
        _ = sender.send(5);
        assert_v3!(Reply::DeferredMap(receiver), b"%5\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_deferred_set() -> Result<(), ReplierError> {
        let (sender, receiver) = oneshot::channel();
        _ = sender.send(5);
        assert_v2!(Reply::DeferredSet(receiver), b"*5\r\n");
        let (sender, receiver) = oneshot::channel();
        _ = sender.send(5);
        assert_v3!(Reply::DeferredSet(receiver), b"~5\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_bulk() -> Result<(), ReplierError> {
        assert_v2!(Reply::Bulk(Bytes::from("abc").into()), b"$3\r\nabc\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_boolean() -> Result<(), ReplierError> {
        assert_v3!(Reply::Boolean(true), b"#t\r\n");
        assert_v3!(Reply::Boolean(false), b"#f\r\n");
        assert_v2!(Reply::Boolean(true), b":1\r\n");
        assert_v2!(Reply::Boolean(false), b":0\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_double() -> Result<(), ReplierError> {
        assert_v2!(Reply::Double(3.2f64), b"+3.2\r\n");
        assert_v2!(Reply::Double(f64::INFINITY), b"+inf\r\n");
        assert_v2!(Reply::Double(f64::NEG_INFINITY), b"+-inf\r\n");

        assert_v3!(Reply::Double(3.2f64), b",3.2\r\n");
        assert_v3!(Reply::Double(f64::INFINITY), b",inf\r\n");
        assert_v3!(Reply::Double(f64::NEG_INFINITY), b",-inf\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_verbatim() -> Result<(), ReplierError> {
        assert_v2!(
            Reply::Verbatim("txt".into(), "foo".into()),
            b"$3\r\nfoo\r\n"
        );
        assert_v2!(
            Reply::Verbatim("mkd".into(), "#foo".into()),
            b"$4\r\n#foo\r\n"
        );

        assert_v3!(
            Reply::Verbatim("txt".into(), "foo".into()),
            b"=7\r\ntxt:foo\r\n"
        );
        assert_v3!(
            Reply::Verbatim("mkd".into(), "#foo".into()),
            b"=8\r\nmkd:#foo\r\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_bignum() -> Result<(), ReplierError> {
        assert_v2!(Reply::Bignum("12345".into()), b"+12345\r\n");
        assert_v3!(Reply::Bignum("12345".into()), b"(12345\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn write_status() -> Result<(), ReplierError> {
        assert_v2!(Reply::Status("PONG".into()), b"+PONG\r\n");
        assert_v2!(Reply::Status(Bytes::from("PONG").into()), b"+PONG\r\n");
        Ok(())
    }
}
