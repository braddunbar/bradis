mod slice;
mod slice_ref;

pub use slice::RawSlice;
pub use slice_ref::RawSliceRef;

use crate::bytes::Output;
use bytes::Bytes;
use hashbrown::Equivalent;
use std::{
    cmp::{Ord, PartialOrd},
    ops::{Deref, Range},
    str,
};
use triomphe::Arc;

/// A heap allocated, shared slice of bytes.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Raw(pub Arc<Vec<u8>>);

impl std::fmt::Debug for Raw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raw(\"{:?}\")", Output(&self.0[..]))
    }
}

impl Raw {
    /// Return a mutable reference to the underlying bytes, or clone them first if this value is
    /// shared.
    pub fn make_mut(&mut self) -> &mut Vec<u8> {
        Arc::make_mut(&mut self.0)
    }

    /// Return a reference to a slice of this value.
    pub fn slice(&self, range: Range<usize>) -> RawSliceRef {
        RawSliceRef::new(self, range)
    }

    /// Set the bytes for a particular range of this value.
    pub fn set_range(&mut self, bytes: &[u8], start: usize) {
        let end = start + bytes.len();
        let value = self.make_mut();
        if end > value.len() {
            value.reserve_exact(end - value.len());
            if start > value.len() {
                value.resize(start, 0);
            }
        }
        let end = std::cmp::min(end, value.len());
        value.splice(start..end, bytes.iter().copied());
    }
}

impl AsRef<[u8]> for Raw {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for Raw {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl Equivalent<Raw> for [u8] {
    fn equivalent(&self, key: &Raw) -> bool {
        self == &key[..]
    }
}

impl From<&str> for Raw {
    fn from(value: &str) -> Self {
        Raw(Arc::new(value.as_bytes().to_vec()))
    }
}

impl From<Vec<u8>> for Raw {
    fn from(value: Vec<u8>) -> Self {
        Raw(Arc::new(value))
    }
}

impl From<&Bytes> for Raw {
    fn from(value: &Bytes) -> Self {
        Raw(Arc::new(value.to_vec()))
    }
}

impl From<Bytes> for Raw {
    fn from(value: Bytes) -> Self {
        Raw(Arc::new(value.to_vec()))
    }
}

impl From<&[u8]> for Raw {
    fn from(value: &[u8]) -> Self {
        Raw(Arc::new(value.to_vec()))
    }
}

impl From<&Raw> for Raw {
    fn from(value: &Raw) -> Self {
        value.clone()
    }
}

impl<const N: usize> From<&'static [u8; N]> for Raw {
    fn from(value: &'static [u8; N]) -> Self {
        Raw(Arc::new(value.to_vec()))
    }
}
