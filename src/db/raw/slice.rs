use crate::{bytes::Output, db::Raw};
use std::ops::{Deref, Range};

/// A shared slice of [`Raw`] bytes.
#[derive(Clone)]
pub struct RawSlice {
    /// The shared [`Raw`] bytes.
    pub data: Raw,

    /// The range of bytes to share.
    pub range: Range<usize>,
}

impl RawSlice {
    /// Create a new slice for a particular range.
    pub fn new(data: Raw, range: Range<usize>) -> Self {
        RawSlice { data, range }
    }
}

impl Deref for RawSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[self.range.clone()]
    }
}

impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        self[..] == other[..]
    }
}

impl std::fmt::Debug for RawSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawSlice(\"{:?}\")", Output(&self[..]))
    }
}
