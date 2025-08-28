use crate::{Pack, PackIter, PackRef, Packable, db::Edge};

/// A Redis map, stored in a [`Pack`] to improve memory usage and locality. Keys and values are
/// stored in an alternating pattern, key first.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PackMap {
    /// The [`Pack`] where the values are stored.
    pack: Pack,
}

impl std::fmt::Debug for PackMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl PackMap {
    /// The number of key value pairs in this map.
    pub fn len(&self) -> usize {
        self.pack.len() / 2
    }

    /// Is this map empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Does this map contain `key`?
    pub fn contains_key<K>(&self, key: &K) -> bool
    where
        K: Packable,
    {
        self.iter().any(|(other, _)| key.pack_eq(&other))
    }

    /// Get the value for a `key`.
    pub fn get<'a, K>(&'a self, key: &K) -> Option<PackRef<'a>>
    where
        K: Packable,
    {
        self.iter()
            .find(|(other, _)| key.pack_eq(other))
            .map(|(_, value)| value)
    }

    /// Remove the value for a `key`. Return `true` if it was removed.
    pub fn remove<K>(&mut self, key: &K) -> bool
    where
        K: Packable,
    {
        let mut cursor = self.pack.cursor(Edge::Left);
        while let Some(element) = cursor.peek() {
            if key.pack_eq(&element) {
                cursor.remove(2);
                return true;
            }
            cursor.skip(2);
        }
        false
    }

    /// Insert a `key` `value` pair into the map. Return `true` if it didn't already exist.
    pub fn insert<K, V>(&mut self, key: &K, value: &V) -> bool
    where
        K: Packable,
        V: Packable,
    {
        let mut cursor = self.pack.cursor(Edge::Left);

        while let Some(other) = cursor.peek() {
            if key.pack_eq(&other) {
                cursor.next();
                cursor.replace(value);
                return false;
            }
            cursor.skip(2);
        }

        self.pack.append2(key, value);
        true
    }

    /// Return an iterator over each key value pair in this map.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter(self.pack.iter())
    }

    /// Return an iterator over the keys in this map.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = PackRef<'a>> {
        self.pack.iter().step_by(2)
    }

    /// Return an iterator over the values in this map.
    pub fn values<'a>(&'a self) -> impl Iterator<Item = PackRef<'a>> {
        self.pack.iter().skip(1).step_by(2)
    }
}

/// An iterator over key value pairs in this map.
pub struct Iter<'a>(PackIter<'a>);

impl<'a> Iterator for Iter<'a> {
    type Item = (PackRef<'a>, PackRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.0.next()?;
        let value = self.0.next()?;
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Raw;

    #[test]
    fn test_insert() {
        let mut map = PackMap::default();

        assert!(!map.contains_key(&"foo"));
        assert_eq!(map.get(&"foo"), None);

        let bar: Raw = "bar".into();
        map.insert(&"foo", &"bar");

        assert!(map.contains_key(&"foo"));
        assert_eq!(map.get(&"foo"), Some(bar.slice(0..3).into()));

        map.remove(&"foo");

        assert!(!map.contains_key(&"foo"));
        assert_eq!(map.get(&"foo"), None);
    }

    #[test]
    fn debug() {
        let mut map = PackMap::default();
        map.insert(&"foo", &"bar");
        map.insert(&2, &5);
        let s = format!("{map:?}");
        assert_eq!(s, "{\"foo\": \"bar\", 2: 5}");
    }
}
