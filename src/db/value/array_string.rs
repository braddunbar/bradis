use crate::bytes::Output;
use bytes::BufMut;
use std::{mem::MaybeUninit, ops::Deref, slice::from_raw_parts};

/// The maximum length of an [`ArrayString`].
const MAX_LEN: usize = 38;

/// An array of bytes that can be embedded in a struct when small enough. When `ArrayVec` supports
/// const generics (and therefore a `u8` length) we can just swap to using that.
#[derive(Clone)]
pub struct ArrayString {
    /// The bytes array.
    data: [MaybeUninit<u8>; MAX_LEN],

    /// The length of written bytes.
    len: u8,
}

impl std::fmt::Debug for ArrayString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArrayString(\"{:?}\")", Output(&self[..]))
    }
}

impl ArrayString {
    /// The number of bytes in this container.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Append a slice of bytes to this value.
    pub fn append(&mut self, value: &[u8]) -> Result<(), ()> {
        let start = self.len();
        let end = start + value.len();
        let mut slice = self.data.get_mut(start..end).ok_or(())?;
        slice.put_slice(value);
        self.len = u8::try_from(end).unwrap();
        Ok(())
    }

    /// Set a range of bytes.
    pub fn set_range(&mut self, value: &[u8], start: usize) -> Result<(), ()> {
        let len = self.len();
        let end = start + value.len();
        let mut slice = self.data.get_mut(start..end).ok_or(())?;
        slice.put_slice(value);
        if end > len {
            self.len = u8::try_from(end).unwrap();
            if start > len {
                for byte in &mut self.data[len..start] {
                    byte.write(0);
                }
            }
        }
        Ok(())
    }
}

impl PartialEq for ArrayString {
    fn eq(&self, other: &Self) -> bool {
        self[..] == other[..]
    }
}

impl Deref for ArrayString {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let len = self.len();
        let pointer = self.data.as_ptr().cast::<u8>();
        unsafe { from_raw_parts(pointer, len) }
    }
}

impl TryFrom<&[u8]> for ArrayString {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let len = value.len();
        let mut data = [MaybeUninit::uninit(); MAX_LEN];
        let mut slice = data.get_mut(..len).ok_or(())?;
        slice.put_slice(value);
        Ok(Self {
            data,
            len: u8::try_from(len).unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_range_capacity_error() {
        let mut value: ArrayString = (&[][..]).try_into().unwrap();
        let bytes = [1; MAX_LEN + 5];
        assert!(value.set_range(&bytes[..], 0).is_err());

        let bytes = [1; MAX_LEN - 2];
        assert!(value.set_range(&bytes[..], 3).is_err());
    }

    #[test]
    fn set_range_within_len() {
        let mut value: ArrayString = "xxxxxx".as_bytes().try_into().unwrap();
        let bytes = "yyy".as_bytes();
        assert!(value.set_range(bytes, 2).is_ok());
        assert_eq!("xxyyyx".as_bytes(), &value[..]);

        let bytes = "yyyy".as_bytes();
        assert!(value.set_range(bytes, 2).is_ok());
        assert_eq!("xxyyyy".as_bytes(), &value[..]);
    }

    #[test]
    fn set_range_past_len() {
        let mut value: ArrayString = "xxx".as_bytes().try_into().unwrap();
        let bytes = "yyy".as_bytes();
        assert!(value.set_range(bytes, 2).is_ok());
        assert_eq!("xxyyy".as_bytes(), &value[..]);

        let mut value: ArrayString = "xxx".as_bytes().try_into().unwrap();
        assert!(value.set_range(bytes, 4).is_ok());
        assert_eq!("xxx\0yyy".as_bytes(), &value[..]);
    }
}
