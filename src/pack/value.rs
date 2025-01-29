use crate::{buffer::Buffer, db::RawSlice};

/// A packed value, optionally with a shared reference to the pack it comes from.
#[derive(Clone, Debug)]
pub enum PackValue {
    Float(f64),
    Integer(i64),
    Raw(RawSlice),
}

impl PackValue {
    /// Return this value as a slice of bytes, optionally in the supplied [`Buffer`].
    pub fn as_bytes<'a>(&'a self, buffer: &'a mut impl Buffer) -> &'a [u8] {
        use PackValue::*;
        match self {
            Float(f) => buffer.write_f64(*f),
            Integer(i) => buffer.write_i64(*i),
            Raw(s) => &s[..],
        }
    }
}
