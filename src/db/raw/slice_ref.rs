use crate::{
    bytes::Output,
    db::{Raw, RawSlice},
};
use std::ops::{Deref, Range};

/// A reference to a range of [`Raw`] bytes.
pub struct RawSliceRef<'a> {
    /// The shared bytes.
    pub data: &'a Raw,

    /// The range of bytes.
    pub range: Range<usize>,
}

impl<'a> RawSliceRef<'a> {
    /// Create a new reference to a particular range of bytes.
    pub fn new(data: &'a Raw, range: Range<usize>) -> Self {
        RawSliceRef { data, range }
    }

    /// Clone the underlying bytes in order to share them elsewhere.
    pub fn to_owned(&self) -> RawSlice {
        RawSlice::new(self.data.clone(), self.range.clone())
    }
}

impl Deref for RawSliceRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[self.range.clone()]
    }
}

impl PartialEq for RawSliceRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self[..] == other[..]
    }
}

impl std::fmt::Debug for RawSliceRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawSliceRef(\"{:?}\")", Output(&self[..]))
    }
}
