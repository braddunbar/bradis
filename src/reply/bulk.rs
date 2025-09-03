use crate::{
    buffer::Buffer,
    db::{Raw, RawSlice, StringSlice, StringValue},
};
use bytes::Bytes;
use respite::RespVersion;

#[derive(Debug)]
pub enum BulkReply {
    Bytes(Bytes),
    RawSlice(RawSlice),
    StringSlice(StringSlice),
    StringValue(StringValue),
}

impl BulkReply {
    pub fn as_bytes<'v>(&'v self, buffer: &'v mut impl Buffer) -> &'v [u8] {
        use BulkReply::*;
        match self {
            Bytes(value) => &value[..],
            RawSlice(value) => &value[..],
            StringSlice(value) => value.as_bytes(buffer),
            StringValue(value) => value.as_bytes(buffer),
        }
    }
}

impl From<&'static str> for BulkReply {
    fn from(value: &'static str) -> Self {
        BulkReply::Bytes(value.into())
    }
}

impl From<Bytes> for BulkReply {
    fn from(value: Bytes) -> Self {
        BulkReply::Bytes(value)
    }
}

impl<const N: usize> From<&'static [u8; N]> for BulkReply {
    fn from(value: &'static [u8; N]) -> Self {
        BulkReply::Bytes(value[..].into())
    }
}

impl From<&[u8]> for BulkReply {
    fn from(value: &[u8]) -> Self {
        BulkReply::StringValue(value[..].into())
    }
}

impl From<Vec<u8>> for BulkReply {
    fn from(value: Vec<u8>) -> Self {
        BulkReply::StringValue(value.into())
    }
}

impl From<Raw> for BulkReply {
    fn from(value: Raw) -> Self {
        BulkReply::StringValue(value.into())
    }
}

impl From<&Raw> for BulkReply {
    fn from(value: &Raw) -> Self {
        BulkReply::StringValue(value.clone().into())
    }
}

impl From<&StringValue> for BulkReply {
    fn from(value: &StringValue) -> Self {
        BulkReply::StringValue(value.clone())
    }
}

impl From<StringValue> for BulkReply {
    fn from(value: StringValue) -> Self {
        BulkReply::StringValue(value)
    }
}

impl From<RawSlice> for BulkReply {
    fn from(value: RawSlice) -> Self {
        BulkReply::RawSlice(value)
    }
}

impl From<StringSlice> for BulkReply {
    fn from(value: StringSlice) -> Self {
        BulkReply::StringSlice(value)
    }
}

impl From<f64> for BulkReply {
    fn from(value: f64) -> Self {
        BulkReply::StringValue(value.into())
    }
}

impl From<i64> for BulkReply {
    fn from(value: i64) -> Self {
        BulkReply::StringValue(value.into())
    }
}

impl From<RespVersion> for BulkReply {
    fn from(value: RespVersion) -> Self {
        use RespVersion::*;
        let value = match value {
            V2 => 2i64,
            V3 => 3i64,
        };
        BulkReply::StringValue(value.into())
    }
}
