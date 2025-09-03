mod bulk;
mod error;
mod status;

pub use bulk::BulkReply;
pub use error::ReplyError;
pub use status::StatusReply;

use crate::{
    client::ClientId,
    command::Arity,
    config::YesNo,
    db::{
        HashKey, HashValue, Raw, RawSliceRef, SetRef, SetValue, SortedSetRef, SortedSetValue,
        StringSlice, StringValue, ValueError,
    },
    pack::{PackRef, PackValue},
};
use bytes::Bytes;
use ordered_float::NotNan;
use piccolo::FromMultiValue;
use respite::RespError;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Reply {
    Array(usize),
    Bignum(Bytes),
    Boolean(bool),
    Bulk(BulkReply),
    DeferredArray(oneshot::Receiver<usize>),
    DeferredMap(oneshot::Receiver<usize>),
    DeferredSet(oneshot::Receiver<usize>),
    Double(f64),
    Error(ReplyError),
    Integer(i64),
    Map(usize),
    Nil,
    Push(usize),
    Set(usize),
    Status(StatusReply),
    Verbatim(Bytes, BulkReply),
}

impl From<Raw> for Reply {
    fn from(value: Raw) -> Self {
        Reply::Bulk(value.into())
    }
}

impl From<&Raw> for Reply {
    fn from(value: &Raw) -> Self {
        Reply::Bulk(value.clone().into())
    }
}

impl From<i32> for Reply {
    fn from(value: i32) -> Self {
        Reply::Integer(value.into())
    }
}

impl From<i64> for Reply {
    fn from(value: i64) -> Self {
        Reply::Integer(value)
    }
}

impl From<usize> for Reply {
    fn from(value: usize) -> Self {
        match i64::try_from(value) {
            Ok(value) => Reply::Integer(value),
            Err(_) => ReplyError::InvalidUsize.into(),
        }
    }
}

impl From<ClientId> for Reply {
    fn from(value: ClientId) -> Self {
        Reply::Integer(value.0)
    }
}

impl<T: Into<Reply>> From<Option<T>> for Reply {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Reply::Nil,
        }
    }
}

impl From<RespError> for Reply {
    fn from(error: RespError) -> Self {
        Reply::Error(ReplyError::Resp(error))
    }
}

impl<const N: usize> From<&'static [u8; N]> for Reply {
    fn from(value: &'static [u8; N]) -> Self {
        Reply::Bulk(value.into())
    }
}

impl From<&'static str> for Reply {
    fn from(value: &'static str) -> Self {
        Reply::Status(value.into())
    }
}

impl From<Bytes> for Reply {
    fn from(value: Bytes) -> Self {
        Reply::Bulk(BulkReply::Bytes(value))
    }
}

impl From<&Bytes> for Reply {
    fn from(value: &Bytes) -> Self {
        Reply::Bulk(BulkReply::Bytes(value.clone()))
    }
}

impl From<ReplyError> for Reply {
    fn from(error: ReplyError) -> Self {
        Reply::Error(error)
    }
}

impl From<ValueError> for Reply {
    fn from(error: ValueError) -> Self {
        use ValueError::*;
        match error {
            WrongType => ReplyError::WrongType.into(),
        }
    }
}

impl From<bool> for Reply {
    fn from(value: bool) -> Self {
        Reply::Boolean(value)
    }
}

impl From<f64> for Reply {
    fn from(value: f64) -> Self {
        Reply::Double(value)
    }
}

impl From<&NotNan<f64>> for Reply {
    fn from(value: &NotNan<f64>) -> Self {
        Reply::Double(**value)
    }
}

impl From<&Arity> for Reply {
    fn from(arity: &Arity) -> Self {
        use Arity::*;
        Reply::Integer(match arity {
            Exact(arity) => (*arity).into(),
            Minimum(arity) => {
                let arity: i64 = (*arity).into();
                -arity
            }
        })
    }
}

impl From<&StringValue> for Reply {
    fn from(value: &StringValue) -> Self {
        Reply::Bulk(BulkReply::StringValue(value.clone()))
    }
}

impl From<StringValue> for Reply {
    fn from(value: StringValue) -> Self {
        Reply::Bulk(BulkReply::StringValue(value))
    }
}

impl From<StringSlice> for Reply {
    fn from(value: StringSlice) -> Self {
        Reply::Bulk(BulkReply::StringSlice(value))
    }
}

impl From<BulkReply> for Reply {
    fn from(bulk: BulkReply) -> Self {
        Reply::Bulk(bulk)
    }
}

impl<'a> From<SetRef<'a>> for Reply {
    fn from(value: SetRef<'a>) -> Self {
        match value {
            SetRef::Int(value) => Reply::Bulk(value.into()),
            SetRef::Pack(value) => value.into(),
            SetRef::String(value) => value.into(),
        }
    }
}

impl From<SetValue> for Reply {
    fn from(value: SetValue) -> Self {
        match value {
            SetValue::Int(value) => Reply::Bulk(value.into()),
            SetValue::Pack(value) => value.into(),
            SetValue::String(value) => value.into(),
        }
    }
}

impl<'a> From<SortedSetRef<'a>> for Reply {
    fn from(key: SortedSetRef<'a>) -> Self {
        match key {
            SortedSetRef::Pack(value) => value.into(),
            SortedSetRef::String(value) => value.into(),
        }
    }
}

impl From<SortedSetValue> for Reply {
    fn from(key: SortedSetValue) -> Self {
        match key {
            SortedSetValue::Pack(value) => value.into(),
            SortedSetValue::String(value) => value.into(),
        }
    }
}

impl<'a> From<HashKey<'a>> for Reply {
    fn from(key: HashKey<'a>) -> Self {
        match key {
            HashKey::String(value) => value.into(),
            HashKey::Pack(value) => value.into(),
        }
    }
}

impl<'a> From<HashValue<'a>> for Reply {
    fn from(key: HashValue<'a>) -> Self {
        match key {
            HashValue::Pack(value) => value.into(),
            HashValue::String(value) => value.into(),
        }
    }
}

impl<'a> From<RawSliceRef<'a>> for Reply {
    fn from(value: RawSliceRef<'a>) -> Self {
        Reply::Bulk(value.to_owned().into())
    }
}

impl From<PackRef<'_>> for Reply {
    fn from(value: PackRef) -> Self {
        use PackRef::*;
        match value {
            Float(f) => Reply::Bulk(f.into()),
            Integer(i) => Reply::Bulk(i.into()),
            Slice(s) => Reply::Bulk(s.to_owned().into()),
        }
    }
}

impl From<&PackRef<'_>> for Reply {
    fn from(value: &PackRef) -> Self {
        use PackRef::*;
        match value {
            Float(f) => Reply::Bulk((*f).into()),
            Integer(i) => Reply::Bulk((*i).into()),
            Slice(s) => Reply::Bulk(s.to_owned().into()),
        }
    }
}

impl From<PackValue> for Reply {
    fn from(value: PackValue) -> Self {
        use PackValue::*;
        match value {
            Float(f) => Reply::Bulk(f.into()),
            Integer(i) => Reply::Bulk(i.into()),
            Raw(s) => Reply::Bulk(s.into()),
        }
    }
}

impl From<YesNo> for Reply {
    fn from(value: YesNo) -> Self {
        Reply::Bulk(if value.0 { "yes" } else { "no" }.into())
    }
}

impl<'gc> FromMultiValue<'gc> for Reply {
    fn from_multi_value(
        _context: piccolo::Context<'gc>,
        mut values: impl Iterator<Item = piccolo::Value<'gc>>,
    ) -> Result<Self, piccolo::TypeError> {
        let first = values.next();
        match first {
            Some(piccolo::Value::Nil) => Ok(Reply::Nil),
            Some(piccolo::Value::Integer(i)) => Ok(Reply::Integer(i)),
            _ => todo!(),
        }
    }
}
