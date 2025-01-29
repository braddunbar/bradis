use crate::{buffer::Buffer, db::StringValue};
use std::ops::Range;

/// An owned slice of a [`StringValue`].
#[derive(Clone, Debug, PartialEq)]
pub struct StringSlice {
    pub value: StringValue,
    pub range: Range<usize>,
}

impl StringSlice {
    /// Return a new string slice.
    pub fn new(value: StringValue, range: Range<usize>) -> Self {
        Self { value, range }
    }

    /// Return a reference to the bytes of this string.
    pub fn as_bytes<'v>(&'v self, buffer: &'v mut impl Buffer) -> &'v [u8] {
        &self.value.as_bytes(buffer)[self.range.clone()]
    }
}
