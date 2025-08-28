use crate::{
    Pack, PackIter, PackRef, PackValue, Packable,
    buffer::ArrayBuffer,
    db::{Edge, Extreme, Insertion},
};
use ordered_float::NotNan;
use std::{
    iter::Rev,
    ops::{Range, RangeBounds},
};

/// A sorted set value, stored in a [`Pack`] to improve memory usage and locality. Score value
/// pairs are stored in a alternating pattern, scores first.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PackSortedSet {
    pack: Pack,
}

impl PackSortedSet {
    /// The number of values in this set.
    pub fn len(&self) -> usize {
        self.pack.len() / 2
    }

    /// Is this set empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return an iterator over the score value pairs in this set.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter(self.pack.iter())
    }

    /// Does this set contain `value`?
    pub fn contains<V: Packable>(&self, value: &V) -> bool {
        self.iter().any(|(_, other)| value.pack_eq(&other))
    }

    /// Return an iterator over a `range` of indexes.
    pub fn range<'a>(
        &'a self,
        range: Range<usize>,
    ) -> impl ExactSizeIterator<Item = (f64, PackRef<'a>)> + DoubleEndedIterator {
        let take = range.end.saturating_sub(range.start);
        self.iter().skip(range.start).take(take)
    }

    /// Return a reverse iterator over a `range` of indexes.
    pub fn rev_range<'a>(
        &'a self,
        range: Range<usize>,
    ) -> impl ExactSizeIterator<Item = (f64, PackRef<'a>)> {
        self.range(range).rev()
    }

    /// Return an iterator over the values with scores within `bounds`.
    pub fn range_score<'a, R>(&'a self, bounds: &R) -> Iter<'a>
    where
        R: RangeBounds<f64>,
    {
        let mut iter = self.iter();

        while let Some((score, _)) = iter.next() {
            if bounds.contains(&score) {
                iter.prev();
                break;
            }
        }

        while let Some((score, _)) = iter.next_back() {
            if bounds.contains(&score) {
                iter.prev_back();
                break;
            }
        }

        iter
    }

    /// Return a reverse iterator over the values with scores within `bounds`.
    pub fn rev_range_score<'a, R>(&'a self, bounds: &R) -> Rev<Iter<'a>>
    where
        R: RangeBounds<f64>,
    {
        self.range_score(bounds).rev()
    }

    /// Return the rank of `value`.
    pub fn rank<V: Packable>(&self, value: &V) -> Option<usize> {
        self.iter()
            .enumerate()
            .find(|(_, (_, other))| value.pack_eq(other))
            .map(|(rank, _)| rank)
    }

    /// Return the number of elements within a given `bounds`.
    pub fn count<R>(&self, bounds: &R) -> usize
    where
        R: RangeBounds<f64>,
    {
        self.range_score(bounds).len()
    }

    /// Return the score for `value`.
    pub fn score<V: Packable>(&self, value: &V) -> Option<f64> {
        self.iter()
            .find(|(_, other)| value.pack_eq(other))
            .map(|(score, _)| score)
    }

    /// Insert `score` and `value` into the set, returning the type of [`Insertion`].
    pub fn insert(&mut self, score: NotNan<f64>, value: &[u8]) -> Option<Insertion> {
        let mut result = Some(Insertion::Added);
        let mut cursor = self.pack.cursor(Edge::Left);

        while let Some(other_score) = cursor.next() {
            let other_score = other_score.float().unwrap();
            let other_value = cursor.next().unwrap();

            if value.pack_eq(&other_value) {
                if (*score - other_score).abs() < f64::EPSILON {
                    return None;
                }
                cursor.prev();
                cursor.prev();
                cursor.remove(2);
                result = Some(Insertion::Changed);
                break;
            }
        }

        let mut buffer = ArrayBuffer::default();
        let mut cursor = self.pack.cursor(Edge::Left);

        while let Some(other_score) = cursor.next() {
            let other_score = other_score.float().unwrap();
            let other_value = cursor.next().unwrap();
            let other_value = other_value.as_bytes(&mut buffer);

            if (other_score, other_value) > (*score, value) {
                cursor.prev();
                cursor.prev();
                cursor.insert2(&*score, &value);
                return result;
            }
        }

        self.pack.append2(&*score, &value);
        result
    }

    /// Remvoe `value` from the set.
    pub fn remove<V: Packable>(&mut self, value: &V) -> bool {
        let mut cursor = self.pack.cursor(Edge::Left);

        while cursor.next().is_some() {
            if value.pack_eq(&cursor.next().unwrap()) {
                cursor.prev();
                cursor.prev();
                cursor.remove(2);
                return true;
            }
        }
        false
    }

    /// Remove all values within `bounds` from the set.
    pub fn remove_range_score<R>(&mut self, bounds: &R) -> usize
    where
        R: RangeBounds<f64>,
    {
        let mut count = 0;
        let mut cursor = self.pack.cursor(Edge::Left);

        while let Some(score) = cursor.next() {
            if bounds.contains(&score.float().unwrap()) {
                count += 1;
                cursor.prev();
                cursor.remove(2);
            } else {
                cursor.next();
            }
        }

        count
    }

    /// Pop a score value pair from one `extreme`.
    pub fn pop(&mut self, extreme: Extreme) -> Option<(f64, PackValue)> {
        let (edge, entry) = match extreme {
            Extreme::Min => (Edge::Left, self.iter().next()),
            Extreme::Max => (Edge::Right, self.iter().next_back()),
        };
        if let Some((score, value)) = entry {
            let result = (score, value.to_owned());
            self.pack.cursor(edge).remove(2);
            return Some(result);
        }
        None
    }
}

/// An iterator over a packed sorted set.
pub struct Iter<'a>(PackIter<'a>);

impl<'a> Iter<'a> {
    /// Reverse a call to `next`.
    fn prev(&mut self) -> Option<(f64, PackRef<'a>)> {
        let value = self.0.prev()?;
        let score = self.0.prev().unwrap();
        let score = score.float().unwrap();
        Some((score, value))
    }

    /// Reverse a call to `next_back`.
    fn prev_back(&mut self) -> Option<(f64, PackRef<'a>)> {
        let score = self.0.prev_back()?;
        let value = self.0.prev_back().unwrap();
        let score = score.float().unwrap();
        Some((score, value))
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (f64, PackRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let score = self.0.next()?;
        let value = self.0.next().unwrap();
        let score = score.float().unwrap();
        Some((score, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.0.size_hint();
        (lower / 2, upper.map(|upper| upper / 2))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let value = self.0.next_back()?;
        let score = self.0.next_back().unwrap();
        let score = score.float().unwrap();
        Some((score, value))
    }
}

impl ExactSizeIterator for Iter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Raw;

    macro_rules! pack_sorted_set {
        ( $(($score:expr, $value:expr)),* $(,)?) => {{
            let mut set = PackSortedSet::default();
            $(set.insert(NotNan::new($score).unwrap(), &$value[..]);)*
            set
        }};
    }

    macro_rules! assert_pack_sorted_set_eq {
        ($iter:expr, $(($score:expr, $value:expr)),* $(,)?) => {{
            let mut buffer = Vec::new();
            let expected: Vec<(f64, Raw)> = vec![$(($score, $value[..].into()),)*];
            let actual: Vec<(f64, Raw)> = $iter.map(|(score, value)| {
                (score, value.as_bytes(&mut buffer).into())
            }).collect();
            assert_eq!(expected, actual);
        }};
    }

    #[test]
    fn insert() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert_eq!(set.len(), 5);
        assert_pack_sorted_set_eq!(
            set.iter(),
            (0f64, b"a"),
            (1f64, b"b"),
            (2f64, b"c"),
            (3f64, b"d"),
            (4f64, b"e"),
        );
    }

    #[test]
    fn insert_with_update() {
        let set = pack_sorted_set!(
            (1123f64, b"b"),
            (29f64, b"c"),
            (999f64, b"a"),
            (412f64, b"e"),
            (5123f64, b"d"),
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert_eq!(set.len(), 5);
        assert_pack_sorted_set_eq!(
            set.iter(),
            (0f64, b"a"),
            (1f64, b"b"),
            (2f64, b"c"),
            (3f64, b"d"),
            (4f64, b"e"),
        );
    }

    #[test]
    fn insert_result() {
        let mut set = PackSortedSet::default();
        assert_eq!(
            set.insert(0f64.try_into().unwrap(), &b"a"[..]),
            Some(Insertion::Added)
        );
        assert_eq!(
            set.insert(1f64.try_into().unwrap(), &b"a"[..]),
            Some(Insertion::Changed)
        );
        assert_eq!(set.insert(1f64.try_into().unwrap(), &b"a"[..]), None);
    }

    #[test]
    fn score() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert_eq!(set.score(&&b"a"[..]), Some(0f64));
        assert_eq!(set.score(&&b"c"[..]), Some(2f64));
        assert_eq!(set.score(&&b"e"[..]), Some(4f64));
    }

    #[test]
    fn contains() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert!(set.contains(&&b"a"[..]));
        assert!(set.contains(&&b"c"[..]));
        assert!(set.contains(&&b"e"[..]));
        assert!(!set.contains(&&b"aa"[..]));
        assert!(!set.contains(&&b"x"[..]));
        assert!(!set.contains(&&b""[..]));
    }

    #[test]
    fn rank() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert_eq!(set.rank(&&b"a"[..]), Some(0));
        assert_eq!(set.rank(&&b"c"[..]), Some(2));
        assert_eq!(set.rank(&&b"e"[..]), Some(4));
        assert_eq!(set.rank(&&b"aa"[..]), None);
    }

    #[test]
    fn count() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert_eq!(set.count(&(0f64..2f64)), 2);
        assert_eq!(set.count(&(1f64..=5f64)), 4);
        assert_eq!(set.count(&(1f64..4f64)), 3);
        assert_eq!(set.count(&(7f64..10f64)), 0);
    }

    #[test]
    fn remove() {
        let mut set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );
        assert!(set.contains(&&b"a"[..]));
        set.remove(&&b"a"[..]);
        assert!(!set.contains(&&b"a"[..]));
        assert_pack_sorted_set_eq!(
            set.iter(),
            (1f64, b"b"),
            (2f64, b"c"),
            (3f64, b"d"),
            (4f64, b"e"),
        );
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn range() {
        let set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );

        assert_eq!(set.len(), 5);
        assert_eq!(set.range(1..3).len(), 2);
        assert_eq!(set.range(0..5).len(), 5);
        assert_eq!(set.range(0..6).len(), 5);
        assert_eq!(set.range(0..12).len(), 5);
        assert_eq!(set.range(1..20).len(), 4);
        assert_eq!(set.range(2..20).len(), 3);

        assert_eq!(set.len(), 5);
        assert_eq!(set.rev_range(1..3).len(), 2);
        assert_eq!(set.rev_range(0..5).len(), 5);
        assert_eq!(set.rev_range(0..6).len(), 5);
        assert_eq!(set.rev_range(0..12).len(), 5);
        assert_eq!(set.rev_range(1..20).len(), 4);
        assert_eq!(set.rev_range(2..20).len(), 3);
    }

    #[test]
    fn remove_range_score() {
        let mut set = pack_sorted_set!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (4f64, b"e"),
            (3f64, b"d"),
        );

        set.remove_range_score(&(01f64..03f64));

        assert_pack_sorted_set_eq!(set.iter(), (0f64, b"a"), (3f64, b"d"), (4f64, b"e"));
    }
}
