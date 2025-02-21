use crate::{
    buffer::Buffer,
    bytes::{Output, parse, parse_i64_exact},
    db::{Raw, RawSliceRef},
    pack::{PackValue, Packable},
};
use std::io::Write;

/// A reference to a value inside an existing [`Pack`][`crate::Pack`].
pub enum PackRef<'a> {
    /// A reference to an `f64` value.
    Float(f64),

    /// A reference to an `i64` value.
    Integer(i64),

    /// A reference to a raw slice in a [`Pack`][`crate::Pack`].
    Slice(RawSliceRef<'a>),
}

impl std::fmt::Debug for PackRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use PackRef::*;

        match self {
            Float(value) => write!(f, "{value:?}"),
            Integer(value) => write!(f, "{value:?}"),
            Slice(value) => write!(f, "\"{:?}\"", Output(&value[..])),
        }
    }
}

impl PartialEq for PackRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        use PackRef::*;
        match self {
            Float(f) => f.pack_eq(other),
            Integer(i) => i.pack_eq(other),
            Slice(s) => (&s[..]).pack_eq(other),
        }
    }
}

impl<'a> PackRef<'a> {
    /// The packed size of this value.
    pub fn size(&self) -> usize {
        use PackRef::*;
        match self {
            Float(f) => f.pack_size(),
            Integer(i) => i.pack_size(),
            Slice(s) => (&s[..]).pack_size(),
        }
    }

    /// Convert this value to an `f64`.
    pub fn float(&self) -> Option<f64> {
        use PackRef::*;
        match self {
            Float(f) => Some(*f),
            #[allow(clippy::cast_precision_loss)]
            Integer(i) => Some(*i as f64),
            Slice(s) => parse(&s[..]),
        }
    }

    /// Convert this value to an `i64`.
    pub fn integer(&self) -> Option<i64> {
        use PackRef::*;
        match self {
            Float(f) => {
                if f.fract() == 0f64 {
                    #[allow(clippy::cast_possible_truncation)]
                    Some(*f as i64)
                } else {
                    None
                }
            }
            Integer(i) => Some(*i),
            Slice(s) => parse_i64_exact(&s[..]),
        }
    }

    /// Convert this value to [`Raw`] bytes.
    pub fn raw(&self) -> Raw {
        use PackRef::*;
        match self {
            Float(f) => {
                let mut v = Vec::new();
                _ = write!(v, "{f}");
                v.into()
            }
            Integer(i) => {
                let mut v = Vec::new();
                _ = write!(v, "{i}");
                v.into()
            }
            Slice(s) => s[..].into(),
        }
    }

    /// Return this value as a slice of bytes, optionally in the supplied [`Buffer`].
    pub fn as_bytes(&'a self, buffer: &'a mut impl Buffer) -> &'a [u8] {
        use PackRef::*;
        match self {
            Float(f) => buffer.write_f64(*f),
            Integer(i) => buffer.write_i64(*i),
            Slice(s) => &s[..],
        }
    }

    /// Convert this value to an owned value with a new [`Raw`] value.
    pub fn to_owned(&self) -> PackValue {
        use PackRef::*;
        match self {
            Float(f) => PackValue::Float(*f),
            Integer(i) => PackValue::Integer(*i),
            Slice(s) => PackValue::Raw(s.to_owned()),
        }
    }
}

impl From<f64> for PackRef<'_> {
    fn from(value: f64) -> Self {
        PackRef::Float(value)
    }
}

impl From<i64> for PackRef<'_> {
    fn from(value: i64) -> Self {
        PackRef::Integer(value)
    }
}

impl<'a> From<RawSliceRef<'a>> for PackRef<'a> {
    fn from(value: RawSliceRef<'a>) -> Self {
        PackRef::Slice(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq() {
        let raw: Raw = "12".as_bytes().into();
        assert_eq!(PackRef::Slice(raw.slice(0..2)), PackRef::Float(12f64));
        assert_eq!(PackRef::Slice(raw.slice(0..2)), PackRef::Integer(12i64));
        assert_eq!(PackRef::Float(12f64), PackRef::Slice(raw.slice(0..2)));
        assert_eq!(PackRef::Integer(12i64), PackRef::Slice(raw.slice(0..2)));
        assert_eq!(PackRef::Integer(12i64), PackRef::Float(12f64));
        assert_eq!(PackRef::Float(12f64), PackRef::Integer(12i64));
    }
}
