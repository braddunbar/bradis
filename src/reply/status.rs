use crate::{buffer::Buffer, db::StringValue};
use bytes::Bytes;

#[derive(Debug)]
pub enum StatusReply {
    Bytes(Bytes),
    Str(&'static str),
    StringValue(StringValue),
}

impl StatusReply {
    pub fn as_bytes<'v>(&'v self, buffer: &'v mut impl Buffer) -> &'v [u8] {
        use StatusReply::*;
        match self {
            Bytes(value) => &value[..],
            Str(value) => value.as_bytes(),
            StringValue(value) => value.as_bytes(buffer),
        }
    }
}

impl From<&'static str> for StatusReply {
    fn from(value: &'static str) -> Self {
        StatusReply::Str(value)
    }
}

impl From<Bytes> for StatusReply {
    fn from(value: Bytes) -> Self {
        StatusReply::Bytes(value)
    }
}

impl From<Vec<u8>> for StatusReply {
    fn from(value: Vec<u8>) -> Self {
        StatusReply::StringValue(value.into())
    }
}
