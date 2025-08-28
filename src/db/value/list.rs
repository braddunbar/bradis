use crate::{
    PackIter, Reversible,
    db::Edge,
    pack::{PackList, PackListInsert, PackRef, Packable},
    quicklist::{Iter as QuickListIter, QuickList},
};

/// A list value, stored as a [`Pack`][`crate::Pack`] when it's small enough
/// and otherwise as a [`QuickList`].
#[derive(Clone, Debug, PartialEq)]
pub enum List {
    Pack(PackList),
    Quick(QuickList),
}

impl Default for List {
    fn default() -> Self {
        List::Pack(PackList::default())
    }
}

impl List {
    /// Is the list empty?
    pub fn is_empty(&self) -> bool {
        match self {
            List::Pack(list) => list.is_empty(),
            List::Quick(list) => list.is_empty(),
        }
    }

    /// Returns the number of elements in the list.
    pub fn len(&self) -> usize {
        match self {
            List::Pack(list) => list.len(),
            List::Quick(list) => list.len(),
        }
    }

    /// Trim `count` values from `edge`.
    pub fn trim(&mut self, edge: Edge, count: usize, max: i64) {
        match self {
            List::Pack(list) => list.trim(edge, count),
            List::Quick(quick) => {
                quick.trim(edge, count);
                if let Some(pack) = quick.convert(max) {
                    *self = List::Pack(pack);
                }
            }
        }
    }

    /// Peek at the value on `edge` end of the list.
    pub fn peek<'a>(&'a self, edge: Edge) -> Option<PackRef<'a>> {
        match self {
            List::Pack(list) => list.peek(edge),
            List::Quick(list) => list.peek(edge),
        }
    }

    /// Push `value` into the `edge` end of the list.
    pub fn push<E>(&mut self, value: &E, edge: Edge, max: i64)
    where
        E: Packable,
    {
        match self {
            List::Pack(pack) => {
                if pack.push(value, edge, max) {
                    return;
                }

                let mut quick = QuickList::from(std::mem::take(pack));
                quick.push(value, edge, max);
                *self = List::Quick(quick);
            }
            List::Quick(quick) => quick.push(value, edge, max),
        }
    }

    /// Set the value at `index`. Return true if the value exists, otherwise false.
    pub fn set(&mut self, element: &[u8], index: usize) -> bool {
        match self {
            // TODO: What if the element doesn't fit into the pack?
            List::Pack(list) => list.set(&element, index),
            List::Quick(list) => list.set(&element, index),
        }
    }

    /// Remove up to `count` values from the list on the `edge` side. Return
    /// the number of values that were removed.
    pub fn remove<E>(&mut self, element: E, count: usize, edge: Edge) -> usize
    where
        E: AsRef<[u8]>,
    {
        match self {
            List::Pack(list) => list.remove(&element, count, edge),
            List::Quick(list) => list.remove(&element, count, edge),
        }
    }

    /// Move an element from one edge to the other.
    pub fn mv(&mut self, from: Edge, to: Edge, max: i64) {
        if from == to {
            return;
        }
        match self {
            List::Pack(list) => {
                list.mv(from);
            }
            List::Quick(list) => {
                let element = list.iter_from(from).next().unwrap().to_owned().clone();
                list.push(&element, to, max);
                list.trim(from, 1);
            }
        }
    }

    /// Insert `element` into the list around `pivot`. Return true if the
    /// element was inserted. Otherwise, return false.
    pub fn insert(&mut self, element: &[u8], pivot: &[u8], before: bool, max: i64) -> bool {
        match self {
            List::Pack(pack) => {
                use PackListInsert::*;
                match pack.insert(&element, pivot, before, max) {
                    After => {
                        let packs = [std::mem::take(pack), (&element).into()];
                        *self = List::Quick(packs.into_iter().collect());
                        true
                    }
                    Before => {
                        let packs = [(&element).into(), std::mem::take(pack)];
                        *self = List::Quick(packs.into_iter().collect());
                        true
                    }
                    Inserted => true,
                    NotFound => false,
                    Split(split) => {
                        let packs = [std::mem::take(pack), split];
                        *self = List::Quick(packs.into_iter().collect());
                        true
                    }
                }
            }
            List::Quick(list) => list.insert(&element, pivot, before, max),
        }
    }

    /// Return an iterator over the list.
    pub fn iter(&self) -> Iter<'_> {
        match self {
            List::Pack(list) => Iter::Pack(list.iter()),
            List::Quick(list) => Iter::Quick(list.iter()),
        }
    }

    /// Return an iterator over the list from `edge`.
    pub fn iter_from(&self, edge: Edge) -> Reversible<Iter<'_>> {
        match edge {
            Edge::Left => Reversible::Forward(self.iter()),
            Edge::Right => Reversible::Reverse(self.iter().rev()),
        }
    }

    /// How much effort is required to drop this value?
    pub fn drop_effort(&self) -> usize {
        match self {
            List::Pack(_) => 1,
            List::Quick(list) => list.packs(),
        }
    }
}

/// An iterator of the values in a list.
pub enum Iter<'a> {
    Pack(PackIter<'a>),
    Quick(QuickListIter<'a>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = PackRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Pack(iter) => iter.next(),
            Iter::Quick(iter) => iter.next(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Iter::Pack(iter) => iter.nth(n),
            Iter::Quick(iter) => iter.nth(n),
        }
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Pack(iter) => iter.next_back(),
            Iter::Quick(iter) => iter.next_back(),
        }
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Iter::Pack(iter) => iter.nth(n),
            Iter::Quick(iter) => iter.nth(n),
        }
    }
}

/// Is a particular `len` and `size` valid for `max`?
pub fn list_is_valid(len: usize, size: usize, max: i64) -> bool {
    // One entry is always valid.
    if len == 1 {
        return true;
    }

    match max {
        -1 => size <= 2usize.pow(12),
        -2 => size <= 2usize.pow(13),
        -3 => size <= 2usize.pow(14),
        -4 => size <= 2usize.pow(15),
        max => match max.try_into() {
            Ok(max) => len <= max,
            Err(_) => size <= 2usize.pow(16),
        },
    }
}
