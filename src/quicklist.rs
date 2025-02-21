use crate::{
    PackIter,
    db::{Edge, list_is_valid},
    linked_list::{Iter as LinkedListIter, LinkedList},
    pack::{PackList, PackListInsert, PackRef, Packable},
    reversible::Reversible,
};

/// Redis lists are stored as a linked list of packed lists.
/// This allows quick insertion and deletion while also maintaining good
/// memory locality.
#[derive(Debug, Default, Clone)]
pub struct QuickList {
    /// The number of elements in the list.
    len: usize,

    /// A linked list of packs.
    list: LinkedList<PackList>,
}

impl PartialEq for QuickList {
    fn eq(&self, other: &QuickList) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl From<PackList> for QuickList {
    fn from(pack: PackList) -> Self {
        let mut list = LinkedList::default();
        let len = pack.len();
        list.push_back(pack);
        Self { len, list }
    }
}

impl FromIterator<PackList> for QuickList {
    fn from_iter<I: IntoIterator<Item = PackList>>(iter: I) -> Self {
        let mut len = 0;
        let mut list = LinkedList::default();
        for pack in iter {
            len += pack.len();
            list.push_back(pack);
        }
        Self { len, list }
    }
}

impl QuickList {
    /// Return the number of elements in the list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return `true` if the list has no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The number of packs in this quicklist.
    pub fn packs(&self) -> usize {
        self.list.len()
    }

    /// Convert this [`QuickList`] into a [`PackList`] if valid.
    pub fn convert(&mut self, max: i64) -> Option<PackList> {
        if self.list.len() != 1 {
            return None;
        }

        let pack = self.list.front().unwrap();
        let len = pack.len();
        let size = pack.size();

        if list_is_valid(2 * len, 2 * size, max) {
            return self.list.pop(Edge::Left);
        }

        None
    }

    /// Return a reference to the element at the `edge` end of the list.
    pub fn peek(&self, edge: Edge) -> Option<PackRef> {
        self.list.edge(edge).and_then(|pack| pack.peek(edge))
    }

    /// Trim at most `count` elements from the `edge` end of the list.
    pub fn trim(&mut self, edge: Edge, mut count: usize) {
        let mut cursor = self.list.cursor(edge);

        while let Some(pack) = cursor.peek_next() {
            if pack.len() > count {
                pack.trim(edge, count);
                self.len -= count;
                return;
            }

            count -= pack.len();
            self.len -= pack.len();
            cursor.remove();
        }
    }

    /// Push `value` into the `edge` end of the list.
    pub fn push<V>(&mut self, value: &V, edge: Edge, max: i64)
    where
        V: Packable,
    {
        self.len += 1;
        let pack = self.list.edge_mut(edge);

        // If the list is empty, just add a node.
        let Some(pack) = pack else {
            self.list.push_front(value.into());
            return;
        };

        if !pack.push(value, edge, max) {
            self.list.push(value.into(), edge);
        }
    }

    pub fn iter(&self) -> Iter {
        Iter {
            iter: self.list.iter(),
            front: None,
            back: None,
        }
    }

    /// Return an iterator over the elements in the list.
    pub fn iter_from(&self, edge: Edge) -> Reversible<Iter> {
        match edge {
            Edge::Left => Reversible::Forward(self.iter()),
            Edge::Right => Reversible::Reverse(self.iter().rev()),
        }
    }

    /// Remove at most `count` elements at the `edge` end of the list.
    pub fn remove<E>(&mut self, element: &E, count: usize, edge: Edge) -> usize
    where
        E: AsRef<[u8]>,
    {
        let mut result = 0;
        let mut cursor = self.list.cursor(edge);

        while let Some(pack) = cursor.peek_next() {
            let remaining = count.saturating_sub(result);
            result += pack.remove(element, remaining, edge);

            if pack.is_empty() {
                cursor.remove();
            } else {
                cursor.next();
            }

            if count != 0 && result == count {
                break;
            }
        }

        self.len -= result;
        result
    }

    /// Set the element at `index` to `value`. Return `false` if the
    /// element doesn't exist.
    pub fn set<V>(&mut self, value: &V, mut index: usize) -> bool
    where
        V: Packable,
    {
        let len = self.len();

        if index >= len {
            return false;
        }

        let mut cursor = self.list.cursor(Edge::Left);

        // Setting the last element should be O(1)
        if index == len - 1 {
            cursor.prev();
            cursor.prev();
            if let Some(pack) = cursor.peek_next() {
                index = pack.len() - 1;
            }
        }

        while let Some(pack) = cursor.next() {
            if pack.set(value, index) {
                break;
            }
            index -= pack.len();
        }

        true
    }

    /// Insert `value` into the list around `pivot` and
    /// return `true` if successful.
    pub fn insert<P, V>(&mut self, value: &V, pivot: P, before: bool, max: i64) -> bool
    where
        P: AsRef<[u8]>,
        V: Packable,
    {
        let mut cursor = self.list.cursor(Edge::Left);

        while let Some(pack) = cursor.next() {
            use PackListInsert::*;
            match pack.insert(value, pivot.as_ref(), before, max) {
                After => {
                    let pushed = cursor
                        .peek_next()
                        .is_some_and(|pack| pack.push(value, Edge::Left, max));

                    if !pushed {
                        cursor.insert(value.into());
                    }

                    self.len += 1;
                    return true;
                }
                Before => {
                    cursor.prev();

                    let pushed = cursor
                        .peek_prev()
                        .is_some_and(|pack| pack.push(value, Edge::Right, max));

                    if !pushed {
                        cursor.insert(value.into());
                    }

                    self.len += 1;
                    return true;
                }
                Split(pack) => {
                    cursor.insert(pack);
                    self.len += 1;
                    return true;
                }
                Inserted => {
                    self.len += 1;
                    return true;
                }
                NotFound => {}
            }
        }

        false
    }
}

/// An iterator over the elements in a [`QuickList`].
pub struct Iter<'a> {
    /// An iterator over the linked list.
    iter: LinkedListIter<'a, PackList>,

    /// An iterator over the front [`PackList`].
    front: Option<PackIter<'a>>,

    /// An iterator over the back [`PackList`].
    back: Option<PackIter<'a>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = PackRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.front.as_mut().and_then(|i| i.next()) {
            return Some(item);
        }

        for list in self.iter.by_ref() {
            let mut iter = list.iter();
            if let Some(item) = iter.next() {
                self.front = Some(iter);
                return Some(item);
            }
        }

        self.back.as_mut().and_then(|i| i.next())
    }

    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        if let Some(front) = self.front.as_mut() {
            if n < front.len() {
                return front.nth(n);
            }
            n -= front.len();
            self.front = None;
        }

        for list in self.iter.by_ref() {
            let mut iter = list.iter();
            if n < iter.len() {
                let result = iter.nth(n);
                self.front = Some(iter);
                return result;
            }
            n -= iter.len();
        }

        if let Some(back) = self.back.as_mut() {
            if n < back.len() {
                return back.nth(n);
            }
            self.back = None;
        }

        None
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.back.as_mut().and_then(|i| i.next_back()) {
            return Some(item);
        }

        for list in self.iter.by_ref().rev() {
            let mut iter = list.iter();
            if let Some(item) = iter.next_back() {
                self.back = Some(iter);
                return Some(item);
            }
        }

        self.front.as_mut().and_then(|i| i.next_back())
    }

    fn nth_back(&mut self, mut n: usize) -> Option<Self::Item> {
        if let Some(back) = self.back.as_mut() {
            if n < back.len() {
                return back.nth_back(n);
            }
            n -= back.len();
            self.back = None;
        }

        for list in self.iter.by_ref().rev() {
            let mut iter = list.iter();
            if n < iter.len() {
                let result = iter.nth_back(n);
                self.back = Some(iter);
                return result;
            }
            n -= iter.len();
        }

        if let Some(front) = self.front.as_mut() {
            if n < front.len() {
                return front.nth_back(n);
            }
            self.front = None;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! pack {
        ([ $($value:expr),* ]) => {{
            let mut pack = PackList::default();
            $(pack.push(&$value, Edge::Right, i64::MAX);)*
            pack
        }};
    }

    macro_rules! linked {
        ( $($pack:tt),* ) => {{
            let mut linked = LinkedList::default();
            $(linked.push_back(pack!($pack));)*
            linked
        }};
    }

    macro_rules! quick {
        ( $($pack:tt),* ) => {{
            let mut len = 0;
            let mut list = LinkedList::default();
            $(
                let pack = pack!($pack);
                len += pack.len();
                list.push_back(pack);
            )*
            QuickList {
                len,
                list
            }
        }};
    }

    #[test]
    fn test_new() {
        let mut pack = PackList::default();
        pack.push(&14, Edge::Right, -2);
        pack.push(&17, Edge::Right, -2);
        let quick = QuickList::from(pack);
        assert_eq!(quick.len(), 2);
        assert!(!quick.is_empty());
    }

    #[test]
    fn test_iter() {
        let mut pack = PackList::default();
        pack.push(&14, Edge::Right, -2);
        pack.push(&17, Edge::Right, -2);
        let quick = QuickList::from(pack);
        let mut iter = quick.iter();
        assert_eq!(iter.next(), Some(14.into()));
        assert_eq!(iter.next(), Some(17.into()));
        assert_eq!(iter.next(), None);
        let mut iter = quick.iter().rev();
        assert_eq!(iter.next(), Some(17.into()));
        assert_eq!(iter.next(), Some(14.into()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_push() {
        let mut quick = QuickList::default();
        assert_eq!(quick.len(), 0);
        assert!(quick.is_empty());

        let max = 3;
        quick.push(&2, Edge::Left, max);
        quick.push(&1, Edge::Left, max);
        quick.push(&3, Edge::Right, max);
        quick.push(&0, Edge::Left, max);
        quick.push(&4, Edge::Right, max);
        assert_eq!(quick.len(), 5);

        assert_eq!(quick.list, linked!([0], [1, 2, 3], [4]));
    }

    #[test]
    fn test_peek() {
        let quick = quick!([0], [1, 2, 3], [4]);
        assert_eq!(quick.peek(Edge::Left), Some(0.into()));
        assert_eq!(quick.peek(Edge::Right), Some(4.into()));
    }

    #[test]
    fn test_trim() {
        let mut quick = quick!([0], [1, 2, 3], [4]);

        quick.trim(Edge::Left, 2);
        assert_eq!(quick.len(), 3);
        assert_eq!(quick.list, linked!([2, 3], [4]));

        quick.trim(Edge::Right, 1);
        assert_eq!(quick.len(), 2);
        assert_eq!(quick.list, linked!([2, 3]));

        quick.trim(Edge::Right, 1);
        assert_eq!(quick.len(), 1);
        assert_eq!(quick.list, linked!([2]));

        quick.trim(Edge::Right, 1);
        assert_eq!(quick.len(), 0);
        assert_eq!(quick.list, LinkedList::default());
    }

    #[test]
    fn push_with_negative_limit() {
        let sizes: [(i64, usize); 6] = [
            (-1, 1000),
            (-2, 2000),
            (-3, 4000),
            (-4, 8000),
            (-5, 16000),
            (-6, 16000),
        ];

        for (max, len) in &sizes {
            let mut quick = QuickList::default();
            let x = "x".repeat(*len);
            let x = x.as_bytes();
            quick.push(&x, Edge::Right, *max);
            quick.push(&x, Edge::Right, *max);
            quick.push(&x, Edge::Right, *max);
            quick.push(&x, Edge::Right, *max);

            quick.push(&x, Edge::Left, *max);
            quick.push(&x, Edge::Right, *max);

            assert_eq!(quick.list, linked!([x], [x, x, x, x], [x]));
        }
    }

    #[test]
    fn test_partial_eq() {
        let one = quick!([0], [1, 2, 3], [4]);
        let mut two = quick!([0, 1, 2, 3, 4]);
        assert_eq!(one, two);

        two.push(&5, Edge::Right, 200);
        assert_ne!(one, two);
    }

    #[test]
    fn test_remove() {
        let mut quick = quick!([0, 4, 4], [5, 1, 4], [0, 0, 2], [3, 0, 4]);

        assert_eq!(quick.remove(b"4", 3, Edge::Left), 3);
        assert_eq!(quick.list, linked!([0], [5, 1], [0, 0, 2], [3, 0, 4]));
        assert_eq!(quick.len(), 9);

        assert_eq!(quick.remove(b"5", 3, Edge::Left), 1);
        assert_eq!(quick.list, linked!([0], [1], [0, 0, 2], [3, 0, 4]));
        assert_eq!(quick.len(), 8);

        assert_eq!(quick.remove(b"0", 3, Edge::Right), 3);
        assert_eq!(quick.list, linked!([0], [1], [2], [3, 4]));
        assert_eq!(quick.len(), 5);

        assert_eq!(quick.remove(b"100", 3, Edge::Left), 0);
        assert_eq!(quick.list, linked!([0], [1], [2], [3, 4]));
        assert_eq!(quick.len(), 5);

        assert_eq!(quick.list, linked!([0], [1], [2], [3, 4]));
    }

    #[test]
    fn test_set() {
        let mut quick = quick!([1, 2, 3], [4, 5, 6]);

        assert!(!quick.set(&4, 8));
        assert_eq!(quick.list, linked!([1, 2, 3], [4, 5, 6]));

        assert!(quick.set(&10, 0));
        assert_eq!(quick.list, linked!([10, 2, 3], [4, 5, 6]));

        assert!(quick.set(&60, 5));
        assert_eq!(quick.list, linked!([10, 2, 3], [4, 5, 60]));

        assert!(quick.set(&40, 3));
        assert_eq!(quick.list, linked!([10, 2, 3], [40, 5, 60]));
    }

    #[test]
    fn test_insert() {
        let mut quick = quick!([0, 2]);

        assert!(quick.insert(&1, b"2", true, -2));
        assert!(quick.insert(&3, b"2", false, -2));
        assert!(!quick.insert(&4, b"5", true, -2));

        let expected = quick!([0, 1, 2, 3]);
        assert_eq!(expected, quick);
    }

    #[test]
    fn test_insert_after_new_node() {
        let mut quick = quick!([0, 1, 2, 3], [5, 6, 7, 8]);
        assert!(quick.insert(&4, b"3", false, 4));
        assert_eq!(quick.list, linked!([0, 1, 2, 3], [4], [5, 6, 7, 8]));
    }

    #[test]
    fn test_insert_after_next_node() {
        let mut quick = quick!([0, 1, 2, 3], [5, 6, 7]);
        assert!(quick.insert(&4, b"3", false, 4));
        assert_eq!(quick.list, linked!([0, 1, 2, 3], [4, 5, 6, 7]));
    }

    #[test]
    fn test_insert_before_prev_node() {
        let mut quick = quick!([0, 1, 2], [4, 5, 6, 7]);
        assert!(quick.insert(&3, b"4", true, 4));
        assert_eq!(quick.list, linked!([0, 1, 2, 3], [4, 5, 6, 7]));
    }

    #[test]
    fn test_insert_before_new_node() {
        let mut quick = quick!([0, 1, 2, 3], [5, 6, 7, 8]);
        assert!(quick.insert(&4, b"5", true, 4));
        assert_eq!(quick.list, linked!([0, 1, 2, 3], [4], [5, 6, 7, 8]));
    }

    #[test]
    fn test_insert_split() {
        let mut quick = quick!([0, 1, 3, 4], [5, 6, 7, 8]);
        assert!(quick.insert(&2, b"3", true, 4));
        assert_eq!(quick.list, linked!([0, 1, 2], [3, 4], [5, 6, 7, 8]));
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod proptests {
    use super::*;
    use prop::sample::Index;
    use proptest::{collection::vec, prelude::*};

    proptest! {
        #[test]
        fn iter(
            items in vec(vec(any::<u8>(), 0..250), 5..20),
            indexes in vec(any::<Index>(), 5..10),
        ) {
            let mut list = QuickList::default();
            for item in &items {
                list.push(&&item[..], Edge::Right, 3);
            }

            // Correct len
            prop_assert_eq!(items.len(), list.len());

            // Check actual count
            prop_assert_eq!(list.iter().count(), items.len());

            // Check forward equality
            prop_assert!(items.iter().zip(list.iter()).all(|(a, b)| (&&a[..]).pack_eq(&b)));

            // Check reverse equality
            prop_assert!(items.iter().rev().zip(list.iter().rev()).all(|(a, b)| (&&a[..]).pack_eq(&b)));

            // Check nth and nth_back
            for index in indexes {
                let n = index.index(items.len());
                let expected = items.get(n).unwrap();
                let actual = list.iter().nth(n).unwrap();
                prop_assert!((&&expected[..]).pack_eq(&actual));

                let n = index.index(items.len());
                let expected = items.iter().nth_back(n).unwrap();
                let actual = list.iter().nth_back(n).unwrap();
                prop_assert!((&&expected[..]).pack_eq(&actual));
            }
        }
    }
}
