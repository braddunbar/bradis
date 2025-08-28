use crate::{
    buffer::Buffer,
    db::{KeyRef, StringValue},
    pack::{PackMap, PackRef, Packable},
    reply::ReplyError,
};
use hashbrown::{HashMap, hash_map::EntryRef};

/// A reference to a hash key.
#[derive(Debug)]
pub enum HashKey<'a> {
    Pack(PackRef<'a>),
    String(&'a StringValue),
}

impl<'a> From<PackRef<'a>> for HashKey<'a> {
    fn from(value: PackRef<'a>) -> Self {
        HashKey::Pack(value)
    }
}

impl<'a> From<&'a StringValue> for HashKey<'a> {
    fn from(value: &'a StringValue) -> Self {
        HashKey::String(value)
    }
}

/// A reference to a hash value.
#[derive(Debug, PartialEq)]
pub enum HashValue<'a> {
    Pack(PackRef<'a>),
    String(&'a StringValue),
}

impl HashValue<'_> {
    pub fn as_bytes<'v>(&'v self, buffer: &'v mut impl Buffer) -> &'v [u8] {
        use HashValue::*;
        match self {
            Pack(value) => value.as_bytes(buffer),
            String(value) => value.as_bytes(buffer),
        }
    }
}

impl<'a> From<PackRef<'a>> for HashValue<'a> {
    fn from(value: PackRef<'a>) -> Self {
        HashValue::Pack(value)
    }
}

impl<'a> From<&'a StringValue> for HashValue<'a> {
    fn from(value: &'a StringValue) -> Self {
        HashValue::String(value)
    }
}

/// A hash, stored as a [`HashMap`] or a [`PackMap`].
#[derive(Clone, Debug, PartialEq)]
pub enum Hash {
    HashMap(HashMap<StringValue, StringValue>),
    PackMap(PackMap),
}

impl Default for Hash {
    fn default() -> Self {
        Hash::PackMap(PackMap::default())
    }
}

impl Hash {
    /// Does the hash contain `key`?
    pub fn contains_key<'a, Q>(&self, key: &'a Q) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Hash::HashMap(map) => map.contains_key(key),
            Hash::PackMap(map) => map.contains_key(&key),
        }
    }

    /// Get the value for `key`.
    pub fn get<'a, Q>(&'a self, key: &'a Q) -> Option<HashValue<'a>>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Hash::HashMap(map) => map.get(key).map(|value| value.into()),
            Hash::PackMap(map) => map.get(&key).map(|value| value.into()),
        }
    }

    /// Increment the value for `key` as an integer.
    pub fn incrby<'a, Q>(
        &mut self,
        key: &'a Q,
        by: i64,
        max_len: usize,
        max_size: usize,
    ) -> Result<i64, ReplyError>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Hash::HashMap(map) => match map.entry_ref(key) {
                EntryRef::Occupied(mut entry) => {
                    let i = entry.get_mut().integer().ok_or(ReplyError::Integer)?;
                    let sum = i.checked_add(by).ok_or(ReplyError::IncrOverflow)?;
                    *i = sum;
                    Ok(sum)
                }
                EntryRef::Vacant(entry) => {
                    entry.insert(by.into());
                    Ok(by)
                }
            },
            Hash::PackMap(map) => {
                if let Some(value) = map.get(&key) {
                    let value = value.integer().ok_or(ReplyError::Integer)?;
                    let sum = value.checked_add(by).ok_or(ReplyError::IncrOverflow)?;
                    self.insert(key, sum, max_len, max_size);
                    Ok(sum)
                } else {
                    self.insert(key, by, max_len, max_size);
                    Ok(by)
                }
            }
        }
    }

    /// Increment the value for `key` as a float.
    pub fn incrbyfloat<'a, Q>(
        &mut self,
        key: &'a Q,
        by: f64,
        max_len: usize,
        max_size: usize,
    ) -> Result<f64, ReplyError>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Hash::HashMap(map) => match map.entry_ref(key) {
                EntryRef::Occupied(mut entry) => {
                    let f = entry.get_mut().float().ok_or(ReplyError::Float)?;
                    let sum = *f + by;
                    if !sum.is_finite() {
                        return Err(ReplyError::NanOrInfinity);
                    }
                    *f = sum;
                    Ok(sum)
                }
                EntryRef::Vacant(entry) => {
                    if !by.is_finite() {
                        return Err(ReplyError::NanOrInfinity);
                    }
                    entry.insert(by.into());
                    Ok(by)
                }
            },
            Hash::PackMap(map) => {
                if let Some(value) = map.get(&key) {
                    let f = value.float().ok_or(ReplyError::Float)?;
                    let sum = f + by;
                    if !sum.is_finite() {
                        return Err(ReplyError::NanOrInfinity);
                    }
                    self.insert(key, sum, max_len, max_size);
                    Ok(sum)
                } else {
                    if !by.is_finite() {
                        return Err(ReplyError::NanOrInfinity);
                    }
                    self.insert(key, by, max_len, max_size);
                    Ok(by)
                }
            }
        }
    }

    /// Insert a `key` `value` pair.
    pub fn insert<'a, Q, V>(
        &mut self,
        key: &'a Q,
        value: V,
        max_len: usize,
        max_size: usize,
    ) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
        V: Into<StringValue> + Packable,
    {
        if let Hash::PackMap(_) = self {
            if key.pack_size() > max_size || value.pack_size() > max_size {
                self.convert();
            }
        }

        match self {
            Hash::HashMap(map) => match map.entry_ref(key) {
                EntryRef::Occupied(mut entry) => {
                    entry.insert(value.into());
                    false
                }
                EntryRef::Vacant(entry) => {
                    entry.insert(value.into());
                    true
                }
            },
            Hash::PackMap(map) => {
                let result = map.insert(&key, &value);
                if map.len() > max_len {
                    self.convert();
                }
                result
            }
        }
    }

    /// Remove the value for `key`.
    pub fn remove<'a, Q>(&mut self, key: &'a Q) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        &'a Q: Packable,
        StringValue: From<&'a Q>,
    {
        match self {
            Hash::HashMap(map) => map.remove(key).is_some(),
            Hash::PackMap(map) => map.remove(&key),
        }
    }

    /// Is this hash empty?
    pub fn is_empty(&self) -> bool {
        match self {
            Hash::HashMap(map) => map.is_empty(),
            Hash::PackMap(map) => map.is_empty(),
        }
    }

    /// The number of values in this hash.
    pub fn len(&self) -> usize {
        match self {
            Hash::HashMap(map) => map.len(),
            Hash::PackMap(map) => map.len(),
        }
    }

    /// Return an iterator over the key value pairs.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (HashKey<'a>, HashValue<'a>)> {
        match self {
            Hash::HashMap(map) => Iter::HashMap(map.iter()),
            Hash::PackMap(map) => Iter::PackMap(map.iter()),
        }
    }

    /// Return an iterator over the keys.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = HashKey<'a>> {
        match self {
            Hash::HashMap(map) => Keys::HashMap(map.keys()),
            Hash::PackMap(map) => Keys::PackMap(map.keys()),
        }
    }

    /// Return an iterator over the values.
    pub fn values<'a>(&'a self) -> impl Iterator<Item = HashValue<'a>> {
        match self {
            Hash::HashMap(map) => Values::HashMap(map.values()),
            Hash::PackMap(map) => Values::PackMap(map.values()),
        }
    }

    /// Convert from a `PackMap` to a `HashMap`.
    pub fn convert(&mut self) {
        match self {
            Hash::HashMap(_) => {}
            Hash::PackMap(packmap) => {
                let mut hashmap = HashMap::with_capacity(packmap.len());
                for (key, value) in packmap.iter() {
                    hashmap.insert(key.into(), value.into());
                }
                *self = Hash::HashMap(hashmap);
            }
        }
    }

    /// How much effort is required to drop this value?
    pub fn drop_effort(&self) -> usize {
        match self {
            Hash::HashMap(map) => map.len(),
            Hash::PackMap(_) => 1,
        }
    }
}

/// An iterator over the keys of a [`enum@Hash`].
pub enum Keys<H, P> {
    HashMap(H),
    PackMap(P),
}

impl<'a, H, P> Iterator for Keys<H, P>
where
    H: Iterator<Item = &'a StringValue>,
    P: Iterator<Item = PackRef<'a>>,
{
    type Item = HashKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Keys::HashMap(iter) => iter.next().map(|value| value.into()),
            Keys::PackMap(iter) => iter.next().map(|value| value.into()),
        }
    }
}

/// An iterator over the values of a [`enum@Hash`].
pub enum Values<H, P> {
    HashMap(H),
    PackMap(P),
}

impl<'a, H, P> Iterator for Values<H, P>
where
    H: Iterator<Item = &'a StringValue>,
    P: Iterator<Item = PackRef<'a>>,
{
    type Item = HashValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Values::HashMap(iter) => iter.next().map(|value| value.into()),
            Values::PackMap(iter) => iter.next().map(|value| value.into()),
        }
    }
}

/// An iterator over the key value pairs in a [`enum@Hash`].
pub enum Iter<H, P> {
    HashMap(H),
    PackMap(P),
}

impl<'a, H, P> Iterator for Iter<H, P>
where
    H: Iterator<Item = (&'a StringValue, &'a StringValue)>,
    P: Iterator<Item = (PackRef<'a>, PackRef<'a>)>,
{
    type Item = (HashKey<'a>, HashValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::HashMap(iter) => iter.next().map(|(key, value)| (key.into(), value.into())),
            Iter::PackMap(iter) => iter.next().map(|(key, value)| (key.into(), value.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert() {
        let mut hash = Hash::default();

        hash.insert(&b"key"[..], "value", 1, 50);
        assert!(matches!(hash, Hash::PackMap(_)));

        hash.insert(&b"1"[..], "2", 1, 50);
        assert!(matches!(hash, Hash::HashMap(_)));

        assert_eq!(
            hash.get(&b"key"[..]),
            Some(HashValue::String(&"value".into()))
        );
        assert_eq!(hash.get(&b"1"[..]), Some(HashValue::String(&2.into())));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn size() {
        assert_eq!(40, std::mem::size_of::<Hash>());
    }
}
