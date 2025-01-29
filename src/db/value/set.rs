use crate::{
    bytes::parse_i64_exact,
    db::{KeyRef, StringValue},
    int_set::{IntSet, Iter as IntSetIter},
    pack::{PackRef, PackSet, PackValue, Packable},
    store::SetConfig,
    PackIter,
};
use hashbrown::{hash_set::Iter as HashSetIter, HashSet};

/// A reference to a [`Set`] value.
pub enum SetRef<'a> {
    Int(i64),
    Pack(PackRef<'a>),
    String(&'a StringValue),
}

impl From<i64> for SetRef<'_> {
    fn from(value: i64) -> Self {
        SetRef::Int(value)
    }
}

impl<'a> From<&'a StringValue> for SetRef<'a> {
    fn from(value: &'a StringValue) -> Self {
        SetRef::String(value)
    }
}

impl<'a> From<PackRef<'a>> for SetRef<'a> {
    fn from(value: PackRef<'a>) -> Self {
        SetRef::Pack(value)
    }
}

/// An owned value from a [`Set`].
pub enum SetValue {
    Int(i64),
    Pack(PackValue),
    String(StringValue),
}

impl From<i64> for SetValue {
    fn from(value: i64) -> Self {
        SetValue::Int(value)
    }
}

impl From<StringValue> for SetValue {
    fn from(value: StringValue) -> Self {
        SetValue::String(value)
    }
}

impl From<PackValue> for SetValue {
    fn from(value: PackValue) -> Self {
        SetValue::Pack(value)
    }
}

/// A set of unique string values, stored as a [`HashSet`] or an [`IntSet`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Set {
    /// Stored as an [`IntSet`].
    Int(IntSet),

    /// Stores as a [`PackSet`].
    Pack(PackSet),

    /// Stored as a [`HashSet`].
    Hash(HashSet<StringValue>),
}

impl Default for Set {
    fn default() -> Self {
        Set::Int(IntSet::default())
    }
}

impl Set {
    /// The number of values in this set.
    pub fn len(&self) -> usize {
        match self {
            Set::Int(set) => set.len(),
            Set::Pack(set) => set.len(),
            Set::Hash(set) => set.len(),
        }
    }

    /// Is this set empty?
    pub fn is_empty(&self) -> bool {
        match self {
            Set::Int(set) => set.is_empty(),
            Set::Pack(set) => set.is_empty(),
            Set::Hash(set) => set.is_empty(),
        }
    }

    /// How much effort is required to drop this value?
    pub fn drop_effort(&self) -> usize {
        match self {
            Set::Int(_) => 1,
            Set::Pack(_) => 1,
            Set::Hash(set) => set.len(),
        }
    }

    /// Does this set contain `value`?
    pub fn contains<'a, Q>(&self, value: &'a Q) -> bool
    where
        Q: AsRef<[u8]> + KeyRef<StringValue> + ?Sized,
        &'a Q: Packable,
    {
        match self {
            Set::Int(set) => match parse_i64_exact(value.as_ref()) {
                Some(value) => set.contains(value),
                None => false,
            },
            Set::Pack(set) => set.contains(&value),
            Set::Hash(set) => set.contains(value),
        }
    }

    /// Insert `value` into this set. Return `false` if it doesn't fit.
    pub fn insert<'a, Q>(&mut self, value: &'a Q, config: &SetConfig) -> bool
    where
        Q: AsRef<[u8]> + KeyRef<StringValue> + ?Sized,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Set::Int(set) => {
                let Some(n) = parse_i64_exact(value.as_ref()) else {
                    // The value is not a number, convert the set!
                    self.convert(config, value);
                    return true;
                };

                // If there's room, just insert the value.
                if set.len() < config.max_intset_entries {
                    return set.insert(n);
                }

                // If the value is already in there, just return false.
                if set.contains(n) {
                    return false;
                }

                // The value isn't present. Gotta convert.
                self.convert(config, value);
                true
            }
            Set::Pack(set) => {
                let max_entries = config.max_listpack_entries;
                let max_value = config.max_listpack_value;
                let invalid = set.len() >= max_entries || value.as_ref().len() > max_value;

                if invalid && !set.contains(&value) {
                    self.convert(config, value);
                    true
                } else {
                    set.insert(&value)
                }
            }
            Set::Hash(set) => set.insert(value.into()),
        }
    }

    /// Pop a random value from this set.
    pub fn pop(&mut self) -> Option<SetValue> {
        match self {
            Set::Int(set) => Some(set.pop()?.into()),
            Set::Pack(set) => Some(set.pop()?.into()),
            Set::Hash(set) => {
                // TODO: Make it random.
                let member = set.iter().next()?.clone();
                set.remove(&member);
                Some(member.into())
            }
        }
    }

    /// Remove `value` from this set.
    pub fn remove<'a, Q>(&mut self, value: &'a Q) -> bool
    where
        Q: AsRef<[u8]> + KeyRef<StringValue> + ?Sized,
        &'a Q: Packable,
    {
        match self {
            Set::Int(set) => match parse_i64_exact(value.as_ref()) {
                Some(value) => set.remove(value),
                None => false,
            },
            Set::Pack(set) => set.remove(&value),
            Set::Hash(set) => set.remove(value),
        }
    }

    /// Return an iterator of the values in this set.
    pub fn iter(&self) -> Iter {
        match self {
            Set::Int(set) => Iter::Int(set.iter()),
            Set::Pack(set) => Iter::Pack(set.iter()),
            Set::Hash(set) => Iter::String(set.iter()),
        }
    }

    /// Convert from an [`IntSet`] or [`PackSet`] to a [`HashSet`] and insert a new value.
    fn convert<'a, Q>(&mut self, config: &SetConfig, value: &'a Q)
    where
        Q: AsRef<[u8]> + KeyRef<StringValue> + ?Sized,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Set::Int(set) => {
                let max_entries = config.max_listpack_entries;
                let max_value = config.max_listpack_value;

                if set.len() == max_entries || set.longest() > max_value {
                    let mut hashset = HashSet::with_capacity(set.len() + 1);
                    for x in set.iter() {
                        hashset.insert(x.into());
                    }
                    hashset.insert(value.into());
                    *self = Set::Hash(hashset);
                } else {
                    *self = Set::Pack((set.iter(), value).into());
                }
            }
            Set::Pack(set) => {
                let mut hashset = HashSet::with_capacity(set.len() + 1);
                for x in set.iter() {
                    hashset.insert(x.into());
                }
                hashset.insert(value.into());
                *self = Set::Hash(hashset);
            }
            Set::Hash(_) => {}
        }
    }
}

/// An iterator over the values in a [`Set`].
pub enum Iter<'a> {
    Int(IntSetIter<'a>),
    Pack(PackIter<'a>),
    String(HashSetIter<'a, StringValue>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = SetRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Int(iter) => iter.next().map(|value| value.into()),
            Iter::Pack(iter) => iter.next().map(|value| value.into()),
            Iter::String(iter) => iter.next().map(|value| value.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size() {
        assert_eq!(48, std::mem::size_of::<Set>());
    }
}
