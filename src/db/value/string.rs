use crate::{
    buffer::{ArrayBuffer, Buffer},
    bytes::{Output, i64_len, parse, parse_i64_exact},
    db::{ArrayString, Raw, StringSlice},
    pack::PackRef,
};
use bytes::Bytes;
use hashbrown::Equivalent;
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    io::Write,
    ops::Range,
};

/// A redis string value, represented in various ways to save memory or
/// facilitate specific operations.
#[derive(Clone, Debug, PartialEq)]
pub enum StringValue {
    Array(ArrayString),
    Float(f64),
    Integer(i64),
    Raw(Raw),
}

impl std::fmt::Display for StringValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use StringValue::*;
        match self {
            Array(value) => write!(f, "{}", Output(&value[..])),
            Float(value) => write!(f, "{}", *value),
            Integer(value) => write!(f, "{}", *value),
            Raw(value) => write!(f, "{}", Output(&value[..])),
        }
    }
}

impl Default for StringValue {
    fn default() -> Self {
        into_string(&[])
    }
}

impl Eq for StringValue {}

impl PartialOrd for StringValue {
    fn partial_cmp(&self, other: &StringValue) -> Option<Ordering> {
        let mut a = ArrayBuffer::default();
        let mut b = ArrayBuffer::default();
        self.as_bytes(&mut a).partial_cmp(other.as_bytes(&mut b))
    }
}

impl Equivalent<StringValue> for Bytes {
    fn equivalent(&self, key: &StringValue) -> bool {
        let mut buffer = ArrayBuffer::default();
        &self[..] == key.as_bytes(&mut buffer)
    }
}

impl Equivalent<StringValue> for Raw {
    fn equivalent(&self, key: &StringValue) -> bool {
        let mut buffer = ArrayBuffer::default();
        &self[..] == key.as_bytes(&mut buffer)
    }
}

impl Equivalent<StringValue> for [u8] {
    fn equivalent(&self, key: &StringValue) -> bool {
        let mut buffer = ArrayBuffer::default();
        self == key.as_bytes(&mut buffer)
    }
}

impl<const C: usize> Equivalent<StringValue> for [u8; C] {
    fn equivalent(&self, key: &StringValue) -> bool {
        let mut buffer = ArrayBuffer::default();
        &self[..] == key.as_bytes(&mut buffer)
    }
}

impl Hash for StringValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut buffer = ArrayBuffer::default();
        self.as_bytes(&mut buffer).hash(state);
    }
}

/// Convert a value into a [`StringValue`]. This ensures that values
/// are put into an array or integer if possible.
fn into_string<V>(value: V) -> StringValue
where
    V: Into<Raw> + AsRef<[u8]>,
{
    if let Some(i) = parse_i64_exact(value.as_ref()) {
        return i.into();
    }

    if let Ok(value) = value.as_ref().try_into() {
        return StringValue::Array(value);
    }

    StringValue::Raw(value.into())
}

impl From<f64> for StringValue {
    fn from(value: f64) -> Self {
        StringValue::Float(value)
    }
}

impl From<i64> for StringValue {
    fn from(value: i64) -> Self {
        StringValue::Integer(value)
    }
}

impl From<&Raw> for StringValue {
    fn from(value: &Raw) -> Self {
        into_string(value)
    }
}

impl From<Raw> for StringValue {
    fn from(value: Raw) -> Self {
        into_string(value)
    }
}

impl From<Bytes> for StringValue {
    fn from(value: Bytes) -> Self {
        into_string(value)
    }
}

impl From<&Bytes> for StringValue {
    fn from(value: &Bytes) -> Self {
        into_string(value)
    }
}

impl From<&[u8]> for StringValue {
    fn from(value: &[u8]) -> Self {
        into_string(value)
    }
}

impl From<Vec<u8>> for StringValue {
    fn from(value: Vec<u8>) -> Self {
        into_string(value)
    }
}

impl From<&str> for StringValue {
    fn from(value: &str) -> Self {
        into_string(value.as_bytes())
    }
}

impl<const N: usize> From<&'static [u8; N]> for StringValue {
    fn from(value: &'static [u8; N]) -> Self {
        into_string(&value[..])
    }
}

impl From<PackRef<'_>> for StringValue {
    fn from(value: PackRef) -> Self {
        use PackRef::*;
        match value {
            Float(f) => StringValue::Float(f),
            Integer(i) => StringValue::Integer(i),
            Slice(s) => into_string(&s[..]),
        }
    }
}

impl From<ArrayString> for StringValue {
    fn from(value: ArrayString) -> Self {
        StringValue::Array(value)
    }
}

impl From<&StringValue> for StringValue {
    fn from(value: &StringValue) -> Self {
        value.clone()
    }
}

impl StringValue {
    /// Return the length of the string.
    pub fn len(&self) -> usize {
        let mut buffer = ArrayBuffer::default();
        use StringValue::*;
        match self {
            Array(value) => value.len(),
            Float(value) => buffer.write_f64(*value).len(),
            Integer(value) => i64_len(*value),
            Raw(value) => value.len(),
        }
    }

    /// Return a reference to this value as bytes, optionally in `buffer`.
    pub fn as_bytes<'v>(&'v self, buffer: &'v mut impl Buffer) -> &'v [u8] {
        use StringValue::*;
        match self {
            Array(value) => &value[..],
            Float(value) => buffer.write_f64(*value),
            Integer(value) => buffer.write_i64(*value),
            Raw(value) => &value[..],
        }
    }

    /// Convert this string into a float.
    pub fn float(&mut self) -> Option<&mut f64> {
        use StringValue::*;

        match self {
            Array(value) => {
                let value = parse::<f64>(&value[..])?;
                *self = Float(value);
            }
            Float(_) => {}
            Integer(value) => {
                #[allow(clippy::cast_precision_loss)]
                let value = *value as f64;
                *self = Float(value);
            }
            Raw(raw) => {
                // TODO: Use more exact parsing?
                let value = parse::<f64>(raw)?;
                *self = Float(value);
            }
        }

        match self {
            Float(value) => Some(value),
            _ => unreachable!(),
        }
    }

    /// Convert this string into an integer.
    pub fn integer(&mut self) -> Option<&mut i64> {
        use StringValue::*;

        match self {
            Array(value) => {
                let value = parse_i64_exact(&value[..])?;
                *self = Integer(value);
            }
            Float(value) => {
                if value.fract() != 0f64 {
                    return None;
                }
                #[allow(clippy::cast_possible_truncation)]
                let value = *value as i64;
                *self = Integer(value);
            }
            Integer(_) => {}
            Raw(raw) => {
                let value = parse_i64_exact(raw)?;
                *self = Integer(value);
            }
        }

        match self {
            Integer(value) => Some(value),
            _ => unreachable!(),
        }
    }

    /// Convert this string into a raw value.
    pub fn raw(&mut self) -> &mut Raw {
        use StringValue::*;

        match self {
            Array(value) => {
                let slice = &value[..];
                *self = Raw(slice.into());
            }
            Float(value) => {
                let mut raw = Vec::new();
                _ = write!(raw, "{value}");
                *self = Raw(raw.into());
            }
            Integer(value) => {
                let mut raw = Vec::new();
                _ = write!(raw, "{value}");
                *self = Raw(raw.into());
            }
            Raw(_) => {}
        }

        match self {
            Raw(value) => value,
            _ => unreachable!(),
        }
    }

    /// Append `bytes` to the string.
    pub fn append(&mut self, bytes: &[u8]) {
        fn append(a: &[u8], b: &[u8]) -> StringValue {
            let mut vec = Vec::with_capacity(a.len() + b.len());
            vec.extend_from_slice(a);
            vec.extend_from_slice(b);
            into_string(vec)
        }

        let mut buffer = ArrayBuffer::default();
        use StringValue::*;
        match self {
            Array(value) => {
                if value.append(bytes).is_err() {
                    *self = append(value, bytes);
                }
            }
            Float(value) => {
                *self = append(buffer.write_f64(*value), bytes);
            }
            Integer(value) => {
                *self = append(buffer.write_i64(*value), bytes);
            }
            Raw(value) => {
                value.make_mut().extend_from_slice(bytes);
                *self = into_string(std::mem::take(value));
            }
        }
    }

    /// Set a range of bytes in the string.
    pub fn set_range(&mut self, bytes: &[u8], start: usize) {
        match self {
            StringValue::Array(value) => {
                if value.set_range(bytes, start).is_err() {
                    let mut raw = Raw::from(&value[..]);
                    raw.set_range(bytes, start);
                    *self = into_string(raw);
                }
            }
            StringValue::Float(f) => {
                let mut raw = Raw::default();
                raw.make_mut().write_f64(*f);
                raw.set_range(bytes, start);
                *self = into_string(raw);
            }
            StringValue::Integer(i) => {
                let mut raw = Raw::default();
                raw.make_mut().write_i64(*i);
                raw.set_range(bytes, start);
                *self = into_string(raw);
            }
            StringValue::Raw(raw) => {
                raw.set_range(bytes, start);
                *self = into_string(std::mem::take(raw));
            }
        }
    }

    /// Return a slice of the string.
    pub fn slice(&self, range: Range<usize>) -> StringSlice {
        StringSlice::new(self.clone(), range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn size() {
        assert_eq!(40, std::mem::size_of::<StringValue>());
    }

    #[test]
    fn len() {
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Integer(2).as_bytes(&mut buffer).len(), 1);
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Integer(22).as_bytes(&mut buffer).len(), 2);
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Integer(25).as_bytes(&mut buffer).len(), 2);
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Integer(-25).as_bytes(&mut buffer).len(), 3);
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Raw("2".into()).as_bytes(&mut buffer).len(), 1);
        let mut buffer = ArrayBuffer::default();
        assert_eq!(StringValue::Float(-5.6f64).as_bytes(&mut buffer).len(), 4);
    }

    #[test]
    fn integer() {
        let mut value = StringValue::Integer(2);
        let i = value.integer();
        assert_eq!(i, Some(&mut 2i64));
        assert_eq!(value, StringValue::Integer(2i64));

        let mut value = StringValue::Raw("2".into());
        let i = value.integer();
        assert_eq!(i, Some(&mut 2i64));
        assert_eq!(value, StringValue::Integer(2i64));

        let mut value = StringValue::Raw("invalid".into());
        let i = value.integer();
        assert_eq!(i, None);
        assert_eq!(value, StringValue::Raw("invalid".into()));

        let mut value = StringValue::Float(5.3f64);
        let i = value.integer();
        assert_eq!(i, None);
        assert_eq!(value, StringValue::Float(5.3f64));

        let mut value = StringValue::Float(5f64);
        let i = value.integer();
        assert_eq!(i, Some(&mut 5i64));
        assert_eq!(value, StringValue::Integer(5i64));

        let mut value = StringValue::Float(-5f64);
        let i = value.integer();
        assert_eq!(i, Some(&mut -5i64));
        assert_eq!(value, StringValue::Integer(-5i64));
    }

    #[test]
    fn raw() {
        let mut value = StringValue::Float(-5.6f64);
        let raw = value.raw();
        assert_eq!(&raw[..], &b"-5.6"[..]);
        assert_eq!(value, StringValue::Raw("-5.6".into()));

        let mut value = StringValue::Integer(2);
        let raw = value.raw();
        assert_eq!(&raw[..], &b"2"[..]);
        assert_eq!(value, StringValue::Raw("2".into()));

        let mut value = StringValue::Raw("2".into());
        let raw = value.raw();
        assert_eq!(&raw[..], &b"2"[..]);
        assert_eq!(value, StringValue::Raw("2".into()));
    }

    #[test]
    fn float() {
        let mut value = StringValue::Float(-5.6f64);
        let f = value.float();
        assert_eq!(f, Some(&mut -5.6f64));
        assert_eq!(value, StringValue::Float(-5.6f64));

        let mut value = StringValue::Integer(2);
        let f = value.float();
        assert_eq!(f, Some(&mut 2f64));
        assert_eq!(value, StringValue::Float(2f64));

        let mut value = StringValue::Raw("234".into());
        let f = value.float();
        assert_eq!(f, Some(&mut 234f64));
        assert_eq!(value, StringValue::Float(234f64));

        let mut value = StringValue::Raw("invalid".into());
        let f = value.float();
        assert_eq!(f, None);
        assert_eq!(value, StringValue::Raw("invalid".into()));
    }
}
