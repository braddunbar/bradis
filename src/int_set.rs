use crate::bytes::i64_len;
use rand::Rng;
use std::slice::Iter as SliceIter;

/// A set of variable sized integers, stored in a `Vec`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntSet {
    /// A `Vec` of `i8`.
    I8(Vec<i8>),

    /// A `Vec` of `i16`.
    I16(Vec<i16>),

    /// A `Vec` of `i32`.
    I32(Vec<i32>),

    /// A `Vec` of `i64`.
    I64(Vec<i64>),
}

impl Default for IntSet {
    fn default() -> Self {
        IntSet::I8(Vec::new())
    }
}

impl IntSet {
    /// The number of values in this set.
    pub fn len(&self) -> usize {
        use IntSet::*;
        match self {
            I8(set) => set.len(),
            I16(set) => set.len(),
            I32(set) => set.len(),
            I64(set) => set.len(),
        }
    }

    /// Is this set empty?
    pub fn is_empty(&self) -> bool {
        use IntSet::*;
        match self {
            I8(set) => set.is_empty(),
            I16(set) => set.is_empty(),
            I32(set) => set.is_empty(),
            I64(set) => set.is_empty(),
        }
    }

    /// Does this set contain `value`?
    pub fn contains(&self, value: i64) -> bool {
        fn contains<T: Ord + TryFrom<i64>>(set: &[T], value: i64) -> bool {
            value
                .try_into()
                .map(|i| set.binary_search(&i).is_ok())
                .unwrap_or(false)
        }

        use IntSet::*;
        match self {
            I8(set) => contains(set, value),
            I16(set) => contains(set, value),
            I32(set) => contains(set, value),
            I64(set) => contains(set, value),
        }
    }

    /// Insert `value`. Return `false` if it's already present.
    pub fn insert(&mut self, value: i64) -> bool {
        fn convert<A: Copy, B: From<A>>(set: &Vec<A>, value: B) -> Vec<B> {
            let mut new: Vec<B> = Vec::with_capacity(set.len() + 1);
            for item in set {
                new.push((*item).into());
            }
            new.push(value);
            new
        }

        fn insert<T: PartialEq + Ord>(set: &mut Vec<T>, value: T) -> bool {
            if let Err(n) = set.binary_search(&value) {
                set.insert(n, value);
                true
            } else {
                false
            }
        }

        use IntSet::*;
        match self {
            I8(set) => {
                if let Ok(value) = value.try_into() {
                    insert(set, value)
                } else if let Ok(value) = value.try_into() {
                    *self = I16(convert(set, value));
                    true
                } else if let Ok(value) = value.try_into() {
                    *self = I32(convert(set, value));
                    true
                } else {
                    *self = I64(convert(set, value));
                    true
                }
            }
            I16(set) => {
                if let Ok(value) = value.try_into() {
                    insert(set, value)
                } else if let Ok(value) = value.try_into() {
                    *self = I32(convert(set, value));
                    true
                } else {
                    *self = I64(convert(set, value));
                    true
                }
            }
            I32(set) => {
                if let Ok(value) = value.try_into() {
                    insert(set, value)
                } else {
                    *self = I64(convert(set, value));
                    true
                }
            }
            I64(set) => insert(set, value),
        }
    }

    /// Remove `value`. Return false if it wasn't found.
    pub fn remove(&mut self, value: i64) -> bool {
        fn remove<T: Ord + PartialEq>(set: &mut Vec<T>, value: &T) -> bool {
            if let Ok(n) = set.binary_search(value) {
                set.remove(n);
                true
            } else {
                false
            }
        }

        if self.is_empty() {
            return false;
        }

        use IntSet::*;
        let result = match self {
            I8(set) => value.try_into().map(|i| remove(set, &i)).unwrap_or(false),
            I16(set) => value.try_into().map(|i| remove(set, &i)).unwrap_or(false),
            I32(set) => value.try_into().map(|i| remove(set, &i)).unwrap_or(false),
            I64(set) => remove(set, &value),
        };
        if result {
            self.shrink();
        }
        result
    }

    /// Return an iterator over the values.
    pub fn iter(&self) -> Iter<'_> {
        use IntSet::*;
        match self {
            I8(set) => Iter::I8(set.iter()),
            I16(set) => Iter::I16(set.iter()),
            I32(set) => Iter::I32(set.iter()),
            I64(set) => Iter::I64(set.iter()),
        }
    }

    /// Pop a random value.
    pub fn pop(&mut self) -> Option<i64> {
        if self.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.len());

        use IntSet::*;
        let result = match self {
            I8(set) => i64::from(set.remove(index)),
            I16(set) => i64::from(set.remove(index)),
            I32(set) => i64::from(set.remove(index)),
            I64(set) => set.remove(index),
        };
        self.shrink();
        Some(result)
    }

    /// The maximum length of an element in base 10 bytes.
    pub fn longest(&self) -> usize {
        let mut iter = self.iter();
        let first = iter.next().map_or(0, i64_len);
        let last = iter.next_back().map_or(0, i64_len);
        std::cmp::max(first, last)
    }

    /// Shrink the vec if necessary.
    fn shrink(&mut self) {
        fn shrink<T>(set: &mut Vec<T>) {
            if set.capacity() / 4 >= set.len() {
                set.shrink_to(set.capacity() / 2);
            }
        }

        use IntSet::*;
        match self {
            I8(set) => shrink(set),
            I16(set) => shrink(set),
            I32(set) => shrink(set),
            I64(set) => shrink(set),
        }
    }
}

/// An iterator over the values in an [`IntSet`].
#[derive(Clone)]
pub enum Iter<'a> {
    I8(SliceIter<'a, i8>),
    I16(SliceIter<'a, i16>),
    I32(SliceIter<'a, i32>),
    I64(SliceIter<'a, i64>),
}

impl Iterator for Iter<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::I8(iter) => iter.next().map(|&i| i.into()),
            Iter::I16(iter) => iter.next().map(|&i| i.into()),
            Iter::I32(iter) => iter.next().map(|&i| i.into()),
            Iter::I64(iter) => iter.next().copied(),
        }
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Iter::I8(iter) => iter.next_back().map(|&i| i.into()),
            Iter::I16(iter) => iter.next_back().map(|&i| i.into()),
            Iter::I32(iter) => iter.next_back().map(|&i| i.into()),
            Iter::I64(iter) => iter.next_back().copied(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert() {
        let mut set = IntSet::default();

        assert!(set.is_empty());

        // i8
        assert!(set.insert(1));
        assert!(!set.insert(1));
        assert!(!set.insert(1));
        assert!(set.insert(0));

        assert!(set.contains(0));
        assert!(set.contains(1));
        assert!(!set.contains(2));
        assert!(!set.is_empty());

        // i16
        assert!(set.insert(i64::from(i8::MAX) + 1));
        assert!(!set.insert(i64::from(i8::MAX) + 1));
        assert!(set.insert(i64::from(i8::MIN) - 1));
        assert!(set.contains(i64::from(i8::MAX) + 1));
        assert!(set.contains(i64::from(i8::MIN) - 1));
        assert!(!set.contains(i64::from(i8::MAX) + 3));
        assert!(!set.is_empty());

        // i32
        assert!(set.insert(i64::from(i16::MAX) + 1));
        assert!(!set.insert(i64::from(i16::MAX) + 1));
        assert!(set.insert(i64::from(i16::MIN) - 1));
        assert!(set.contains(i64::from(i16::MAX) + 1));
        assert!(set.contains(i64::from(i16::MIN) - 1));
        assert!(!set.contains(i64::from(i16::MAX) + 3));
        assert!(!set.is_empty());

        // i64
        assert!(set.insert(i64::from(i32::MAX) + 1));
        assert!(!set.insert(i64::from(i32::MAX) + 1));
        assert!(set.insert(i64::from(i32::MIN) - 1));
        assert!(set.contains(i64::from(i32::MAX) + 1));
        assert!(set.contains(i64::from(i32::MIN) - 1));
        assert!(!set.contains(i64::from(i32::MAX) + 3));
        assert!(!set.is_empty());
    }

    #[test]
    fn remove() {
        let mut set = IntSet::default();

        // i8
        set.insert(0);
        set.insert(1);
        assert!(set.remove(0));
        assert!(!set.remove(0));
        assert!(!set.contains(0));

        // i16
        set.insert(i64::from(i8::MAX) + 1);
        assert!(set.remove(i64::from(i8::MAX) + 1));
        assert!(!set.remove(i64::from(i8::MAX) + 1));
        assert!(!set.contains(i64::from(i8::MAX) + 1));

        // i32
        set.insert(i64::from(i16::MAX) + 1);
        assert!(set.remove(i64::from(i16::MAX) + 1));
        assert!(!set.remove(i64::from(i16::MAX) + 1));
        assert!(!set.contains(i64::from(i16::MAX) + 1));

        // i64
        set.insert(i64::from(i32::MAX) + 1);
        assert!(set.remove(i64::from(i32::MAX) + 1));
        assert!(!set.remove(i64::from(i32::MAX) + 1));
        assert!(!set.contains(i64::from(i32::MAX) + 1));
    }

    #[test]
    fn pop() {
        let mut set = IntSet::default();

        // i8
        set.insert(0);
        assert_eq!(Some(0), set.pop());

        // i16
        set.insert(i64::from(i8::MAX) + 1);
        assert_eq!(Some(i64::from(i8::MAX) + 1), set.pop());

        // i32
        set.insert(i64::from(i16::MAX) + 1);
        assert_eq!(Some(i64::from(i16::MAX) + 1), set.pop());

        // i64
        set.insert(i64::from(i32::MAX) + 1);
        assert_eq!(Some(i64::from(i32::MAX) + 1), set.pop());
    }

    #[test]
    fn iter() {
        let mut set = IntSet::default();

        // i8
        set.insert(0);
        let expected: Vec<i64> = vec![0];
        assert_eq!(expected, set.iter().collect::<Vec<i64>>());

        // i16
        set.insert(i64::from(i8::MAX) + 1);
        let expected: Vec<i64> = vec![0, i64::from(i8::MAX) + 1];
        assert_eq!(expected, set.iter().collect::<Vec<i64>>());

        // i32
        set.insert(i64::from(i16::MAX) + 1);
        let expected: Vec<i64> = vec![0, i64::from(i8::MAX) + 1, i64::from(i16::MAX) + 1];
        assert_eq!(expected, set.iter().collect::<Vec<i64>>());

        // i64
        set.insert(i64::from(i32::MAX) + 1);
        let expected: Vec<i64> = vec![
            0,
            i64::from(i8::MAX) + 1,
            i64::from(i16::MAX) + 1,
            i64::from(i32::MAX) + 1,
        ];
        assert_eq!(expected, set.iter().collect::<Vec<i64>>());
    }

    #[test]
    fn longest() {
        let mut set = IntSet::default();
        assert_eq!(0, set.longest());
        set.insert(0);
        assert_eq!(1, set.longest());
        set.insert(10);
        assert_eq!(2, set.longest());
        set.insert(-10);
        assert_eq!(3, set.longest());
        set.insert(-2_345_678);
        assert_eq!(8, set.longest());
        set.insert(1_234_567_890);
        assert_eq!(10, set.longest());
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
        fn intset(
            mut items in vec(any::<i64>(), 40..50),
            others in vec(any::<i64>(), 15..20),
            indexes in vec(any::<Index>(), 5..10),
        ) {
            items.sort_unstable();
            let mut set = IntSet::default();
            for item in &items {
                set.insert(*item);
            }

            // Correct len
            prop_assert_eq!(items.len(), set.len());

            // Check actual count
            prop_assert_eq!(set.iter().count(), items.len());

            // Check forward equality
            prop_assert!(items.iter().zip(set.iter()).all(|(a, b)| *a == b));

            for other in &others {
                let expected = items.contains(other);
                let actual = set.contains(*other);
                prop_assert_eq!(expected, actual);
            }

            for index in &indexes {
                let n = index.index(items.len());
                prop_assert!(set.contains(items[n]));
            }
        }
    }
}
