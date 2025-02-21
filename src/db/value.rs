mod array_string;
mod hash;
mod list;
mod set;
mod sorted_set;
mod string;
mod string_slice;

pub use array_string::ArrayString;
pub use hash::{Hash, HashKey, HashValue};
pub use list::{List, list_is_valid};
pub use set::{Set, SetRef, SetValue};
pub use sorted_set::{Insertion, SortedSet, SortedSetRef, SortedSetValue};
pub use string::StringValue;
pub use string_slice::StringSlice;

use crate::db::Raw;
use bytes::Bytes;

/// The minimum or maximum extreme of a sorted set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Extreme {
    /// The minimum extreme.
    Min,

    // The maximum extreme.
    Max,
}

/// The left or right edge of a list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Edge {
    /// The left edge.
    Left,

    /// The right edge.
    Right,
}

/// An error from an operation on a `Value`.
#[derive(Debug)]
pub enum ValueError {
    /// An error due to having the wrong type of value.
    WrongType,
}

/// A value in a database, representing one of several types.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A hash value.
    Hash(Box<Hash>),

    /// A list value.
    List(Box<List>),

    /// A set value
    Set(Box<Set>),

    /// A sorted set value.
    SortedSet(Box<SortedSet>),

    /// A string value.
    String(StringValue),
}

impl Value {
    /// Create a new hash value.
    pub fn hash() -> Self {
        Value::Hash(Box::default())
    }

    /// Create a new list value.
    pub fn list() -> Self {
        Value::List(Box::default())
    }

    /// Create a new set value.
    pub fn set() -> Self {
        Value::Set(Box::default())
    }

    /// Create a new sorted set value.
    pub fn sorted_set() -> Self {
        Value::SortedSet(Box::default())
    }

    /// Create a new string value.
    pub fn string() -> Self {
        Value::String(StringValue::default())
    }

    /// Return a reference to the inner hash value or an error.
    pub fn as_hash(&self) -> Result<&Hash, ValueError> {
        match self {
            Value::Hash(h) => Ok(h),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a mutable reference to the inner hash value or an error.
    pub fn mut_hash(&mut self) -> Result<&mut Hash, ValueError> {
        match self {
            Value::Hash(h) => Ok(h),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a reference to the inner set value or an error.
    pub fn as_set(&self) -> Result<&Set, ValueError> {
        match self {
            Value::Set(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a mutable reference to the inner set value or an error.
    pub fn mut_set(&mut self) -> Result<&mut Set, ValueError> {
        match self {
            Value::Set(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a reference to the inner sorted set value or an error.
    pub fn as_sorted_set(&self) -> Result<&SortedSet, ValueError> {
        match self {
            Value::SortedSet(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a mutable reference to the inner sorted set value or an error.
    pub fn mut_sorted_set(&mut self) -> Result<&mut SortedSet, ValueError> {
        match self {
            Value::SortedSet(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a reference to the inner string value or an error.
    pub fn as_string(&self) -> Result<&StringValue, ValueError> {
        match self {
            Value::String(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a mutable reference to the inner string value or an error.
    pub fn mut_string(&mut self) -> Result<&mut StringValue, ValueError> {
        match self {
            Value::String(s) => Ok(s),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a reference to the inner list value or an error.
    pub fn as_list(&self) -> Result<&List, ValueError> {
        match self {
            Value::List(l) => Ok(l),
            _ => Err(ValueError::WrongType),
        }
    }

    /// Return a mutable reference to the inner list value or an error.
    pub fn mut_list(&mut self) -> Result<&mut List, ValueError> {
        match self {
            Value::List(l) => Ok(l),
            _ => Err(ValueError::WrongType),
        }
    }

    /// How much effort is required to drop this value?
    pub fn drop_effort(&self) -> usize {
        match self {
            Value::Hash(hash) => hash.drop_effort(),
            Value::List(list) => list.drop_effort(),
            Value::Set(set) => set.drop_effort(),
            Value::SortedSet(set) => set.drop_effort(),
            Value::String(_) => 1,
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::String(value[..].into())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Value::String(value.into())
    }
}

impl From<Raw> for Value {
    fn from(value: Raw) -> Self {
        Value::String(value.into())
    }
}

impl From<Hash> for Value {
    fn from(hash: Hash) -> Self {
        Value::Hash(Box::new(hash))
    }
}

impl From<List> for Value {
    fn from(list: List) -> Self {
        Value::List(Box::new(list))
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Value::String(value.into())
    }
}

impl From<&Bytes> for Value {
    fn from(value: &Bytes) -> Self {
        Value::String(value.into())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::String(value.into())
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::String(value.into())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.as_bytes().into())
    }
}

impl<const N: usize> From<&'static [u8; N]> for Value {
    fn from(value: &'static [u8; N]) -> Self {
        Value::String(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn size() {
        assert_eq!(40, std::mem::size_of::<Value>());
    }
}
