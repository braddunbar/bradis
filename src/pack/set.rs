use crate::{Pack, PackIter, PackValue, Packable, db::Edge};
use rand::Rng;

/// A Redis set, stored in a [`Pack`] to improve memory usage and locality.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PackSet {
    /// The [`Pack`] where the values are stored.
    pack: Pack,
}

impl std::fmt::Debug for PackSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl PackSet {
    /// The number of key value pairs in this set.
    pub fn len(&self) -> usize {
        self.pack.len()
    }

    /// Is this set empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Does this set contain `value`?
    pub fn contains<V>(&self, value: &V) -> bool
    where
        V: Packable,
    {
        self.iter().any(|other| value.pack_eq(&other))
    }

    /// Remove a `value`. Return `true` if it was removed.
    pub fn remove<V>(&mut self, value: &V) -> bool
    where
        V: Packable,
    {
        let mut cursor = self.pack.cursor(Edge::Left);
        while let Some(element) = cursor.peek() {
            if value.pack_eq(&element) {
                cursor.remove(1);
                return true;
            }
            cursor.skip(1);
        }
        false
    }

    /// Insert a `value` into the set. Return `true` if it didn't already exist.
    pub fn insert<V>(&mut self, value: &V) -> bool
    where
        V: Packable,
    {
        if self.contains(value) {
            return false;
        }

        self.pack.append(value);
        true
    }

    /// Pop a random value.
    pub fn pop(&mut self) -> Option<PackValue> {
        if self.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.len());
        let mut cursor = self.pack.cursor(Edge::Left);
        cursor.skip(index);
        let result = cursor.peek().map(|element| element.to_owned());
        cursor.remove(1);
        result
    }

    /// Return an iterator over each value in this set.
    pub fn iter(&self) -> PackIter {
        self.pack.iter()
    }
}

impl<I, TI, TV> From<(I, TV)> for PackSet
where
    TI: Packable,
    TV: Packable,
    I: Iterator<Item = TI> + Clone,
{
    fn from(value: (I, TV)) -> Self {
        PackSet { pack: value.into() }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::ArrayBuffer;

    use super::*;

    #[test]
    fn test_insert() {
        let mut set = PackSet::default();
        assert!(!set.contains(&"foo"));
        set.insert(&"foo");
        assert!(set.contains(&"foo"));
        set.remove(&"foo");
        assert!(!set.contains(&"foo"));
    }

    #[test]
    fn test_pop() {
        let mut buffer = ArrayBuffer::default();
        let mut set = PackSet::default();
        set.insert(&"foo");
        let value = set.pop().unwrap();
        assert_eq!(b"foo", value.as_bytes(&mut buffer));
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut set = PackSet::default();
        set.insert(&"foo");
        set.insert(&"bar");
        assert!(set.contains(&"foo"));
        assert!(set.contains(&"bar"));
        assert!(set.remove(&"foo"));
        assert!(!set.remove(&"foo"));
        assert!(!set.contains(&"foo"));
        assert!(set.contains(&"bar"));
    }

    #[test]
    fn debug() {
        let mut set = PackSet::default();
        set.insert(&"foo");
        set.insert(&2);
        let s = format!("{set:?}");
        assert_eq!(s, "[\"foo\", 2]");
    }
}
