use crate::{
    db::{list_is_valid, Edge},
    Pack, PackIter, PackRef, Packable, Reversible,
};

/// A redis list, stored as a [`Pack`] of values to improve memory usage and locality.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PackList {
    /// The [`Pack`] where the values are stored.
    pack: Pack,
}

impl std::fmt::Debug for PackList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl<'a, V> From<&'a V> for PackList
where
    V: Packable,
{
    fn from(value: &'a V) -> Self {
        let mut pack = Pack::default();
        pack.append(value);
        Self { pack }
    }
}

/// The result of attempting to insert a value into a [`PackList`]. The value may actually be
/// inserted, or the result may indicate the position at which the value should be inserted.
#[must_use]
#[derive(Debug, PartialEq)]
pub enum PackListInsert {
    /// The value should be inserted after this [`PackList`].
    After,

    /// The value should be inserted before this [`PackList`].
    Before,

    /// The value was inserted into this [`PackList`].
    Inserted,

    /// The pivot value was not found.
    NotFound,

    /// This [`PackList`] was split in order to insert the value. The resultant [`PackList`] should
    /// follow this one.
    Split(PackList),
}

impl PackList {
    /// The number of values in this list.
    pub fn len(&self) -> usize {
        self.pack.len()
    }

    /// Is this list empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of bytes used to store this list.
    pub fn size(&self) -> usize {
        self.pack.size()
    }

    /// Take a peek at the value on the `edge` without removing it.
    pub fn peek(&self, edge: Edge) -> Option<PackRef> {
        let mut iter = self.pack.iter();
        match edge {
            Edge::Left => iter.next(),
            Edge::Right => iter.next_back(),
        }
    }

    /// If the value fits in this list, push it onto the appropriate `edge` and return `true`.
    /// Otherwise, return `false`.
    pub fn push<V>(&mut self, value: &V, edge: Edge, max: i64) -> bool
    where
        V: Packable,
    {
        let len = self.len() + 1;
        let size = self.size() + value.pack_size();
        if !list_is_valid(len, size, max) {
            return false;
        }

        match edge {
            Edge::Left => self.pack.prepend(value),
            Edge::Right => self.pack.append(value),
        }

        true
    }

    /// If `index` exists, set the value and return `true`. Otherwise return `false`.
    pub fn set<V>(&mut self, value: &V, index: usize) -> bool
    where
        V: Packable,
    {
        if index >= self.len() {
            return false;
        }

        if self.len() - index >= index {
            let mut cursor = self.pack.cursor(Edge::Left);
            cursor.skip(index);
            cursor.replace(value);
        } else {
            let len = self.len();
            let mut cursor = self.pack.cursor(Edge::Right);
            cursor.skip(len - index - 1);
            cursor.replace(value);
        }
        true
    }

    /// Insert `value` adjacent to `pivot`, according to `before`. Return the appropriate
    /// [`PackListInsert`] result.
    pub fn insert<P, V>(&mut self, value: &V, pivot: P, before: bool, max: i64) -> PackListInsert
    where
        P: AsRef<[u8]>,
        V: Packable,
    {
        let len = self.len() + 1;
        let size = self.size() + value.pack_size();
        let valid = list_is_valid(len, size, max);

        let mut cursor = self.pack.cursor(Edge::Left);
        while let Some(element) = cursor.next() {
            if !pivot.as_ref().pack_eq(&element) {
                continue;
            }

            if valid {
                if before {
                    cursor.prev();
                }
                cursor.insert(value);
                return PackListInsert::Inserted;
            }

            if before && cursor.index() == 1 {
                return PackListInsert::Before;
            }

            if !before && cursor.index() == cursor.len() {
                return PackListInsert::After;
            }

            if before {
                cursor.prev();
            }

            let new = PackList {
                pack: cursor.split(),
            };
            cursor.insert(value);
            return PackListInsert::Split(new);
        }

        PackListInsert::NotFound
    }

    /// Remove `count` values from the list that match `element` from `edge`. Return the number of
    /// values removed.
    pub fn remove<E>(&mut self, element: &E, count: usize, edge: Edge) -> usize
    where
        E: AsRef<[u8]>,
    {
        let mut result = 0;
        let mut cursor = self.pack.cursor(edge);

        while let Some(value) = cursor.peek() {
            if element.as_ref().pack_eq(&value) {
                result += 1;
                cursor.remove(1);
                if count != 0 && result == count {
                    break;
                }
            } else {
                cursor.next();
            }
        }

        result
    }

    /// An iterator over the values in this list.
    pub fn iter(&self) -> PackIter {
        self.pack.iter()
    }

    /// A reversible iterator over the values in this list.
    pub fn iter_from(&self, edge: Edge) -> Reversible<PackIter> {
        match edge {
            Edge::Left => Reversible::Forward(self.iter()),
            Edge::Right => Reversible::Reverse(self.iter().rev()),
        }
    }

    /// Trim `count` values from the `edge` of the list.
    pub fn trim(&mut self, edge: Edge, count: usize) {
        self.pack.cursor(edge).remove(count);
    }

    /// Move an element from one edge to the other.
    pub fn mv(&mut self, from: Edge) {
        self.pack.mv(from);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push() {
        let mut list = PackList::default();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.push(&2, Edge::Left, -2);
        list.push(&3, Edge::Right, -2);
        list.push(&1, Edge::Left, -2);

        let mut expected = Pack::default();
        expected.append(&1);
        expected.append(&2);
        expected.append(&3);

        assert_eq!(expected, list.pack);
    }

    #[test]
    fn test_peek() {
        let mut list = PackList::default();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.push(&2, Edge::Left, -2);
        list.push(&3, Edge::Right, -2);
        list.push(&1, Edge::Left, -2);

        assert!(1.pack_eq(&list.peek(Edge::Left).unwrap()));
        assert!(3.pack_eq(&list.peek(Edge::Right).unwrap()));
    }

    #[test]
    fn test_set() {
        let mut list = PackList::default();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.push(&0, Edge::Right, -2);
        list.push(&10, Edge::Right, -2);
        list.push(&2, Edge::Right, -2);
        list.push(&30, Edge::Right, -2);

        assert!(list.set(&1, 1));
        assert!(list.set(&3, 3));
        assert!(!list.set(&4, 4));

        let mut expected = Pack::default();
        expected.append(&0);
        expected.append(&1);
        expected.append(&2);
        expected.append(&3);

        assert_eq!(expected, list.pack);
    }

    #[test]
    fn test_insert() {
        let mut list = PackList::default();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.push(&0, Edge::Right, -2);
        list.push(&2, Edge::Right, -2);

        assert_eq!(list.insert(&1, b"2", true, -2), PackListInsert::Inserted);
        assert_eq!(list.insert(&3, b"2", false, -2), PackListInsert::Inserted);
        assert_eq!(list.insert(&4, b"5", true, -2), PackListInsert::NotFound);

        let mut expected = Pack::default();
        expected.append(&0);
        expected.append(&1);
        expected.append(&2);
        expected.append(&3);

        assert_eq!(expected, list.pack);
    }

    #[test]
    fn test_remove() {
        let mut list = PackList::default();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.push(&0, Edge::Right, -2);
        list.push(&4, Edge::Right, -2);
        list.push(&4, Edge::Right, -2);
        list.push(&5, Edge::Right, -2);
        list.push(&1, Edge::Right, -2);
        list.push(&4, Edge::Right, -2);
        list.push(&0, Edge::Right, -2);
        list.push(&0, Edge::Right, -2);
        list.push(&2, Edge::Right, -2);
        list.push(&3, Edge::Right, -2);
        list.push(&0, Edge::Right, -2);
        list.push(&4, Edge::Right, -2);

        assert_eq!(list.remove(b"4", 3, Edge::Left), 3);
        assert_eq!(list.remove(b"5", 3, Edge::Left), 1);
        assert_eq!(list.remove(b"0", 3, Edge::Right), 3);
        assert_eq!(list.remove(b"100", 3, Edge::Left), 0);

        let mut expected = Pack::default();
        expected.append(&0);
        expected.append(&1);
        expected.append(&2);
        expected.append(&3);
        expected.append(&4);

        assert_eq!(expected, list.pack);
    }

    #[test]
    fn debug() {
        let mut list = PackList::default();
        list.push(&"foo", Edge::Right, -2);
        list.push(&"bar", Edge::Right, -2);
        list.push(&2, Edge::Right, -2);
        list.push(&5, Edge::Right, -2);
        let s = format!("{list:?}");
        assert_eq!(s, "[\"foo\", \"bar\", 2, 5]");
    }
}
