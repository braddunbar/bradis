use crate::{
    db::{Extreme, KeyRef, StringValue},
    pack::{PackRef, PackSortedSet, PackValue, Packable},
    skiplist::Skiplist,
};
use hashbrown::{hash_map::EntryRef, HashMap};
use ordered_float::NotNan;
use std::ops::{Range, RangeBounds};

#[derive(Debug, Eq, PartialEq)]
pub enum Insertion {
    Added,
    Changed,
}

#[derive(Debug)]
pub enum SortedSetRef<'a> {
    Pack(PackRef<'a>),
    String(&'a StringValue),
}

impl<'a> From<PackRef<'a>> for SortedSetRef<'a> {
    fn from(value: PackRef<'a>) -> Self {
        SortedSetRef::Pack(value)
    }
}

impl<'a> From<&'a StringValue> for SortedSetRef<'a> {
    fn from(value: &'a StringValue) -> Self {
        SortedSetRef::String(value)
    }
}

#[derive(Debug)]
pub enum SortedSetValue {
    Pack(PackValue),
    String(StringValue),
}

impl From<PackValue> for SortedSetValue {
    fn from(value: PackValue) -> Self {
        SortedSetValue::Pack(value)
    }
}

impl From<StringValue> for SortedSetValue {
    fn from(value: StringValue) -> Self {
        SortedSetValue::String(value)
    }
}

#[derive(Clone, Debug)]
pub enum SortedSet {
    Pack(PackSortedSet),
    Skiplist(Skiplist, HashMap<StringValue, NotNan<f64>>),
}

impl Default for SortedSet {
    fn default() -> Self {
        SortedSet::Pack(PackSortedSet::default())
    }
}

impl PartialEq for SortedSet {
    fn eq(&self, _: &Self) -> bool {
        todo!()
    }
}

impl SortedSet {
    pub fn len(&self) -> usize {
        match self {
            SortedSet::Pack(set) => set.len(),
            SortedSet::Skiplist(_, map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            SortedSet::Pack(set) => set.is_empty(),
            SortedSet::Skiplist(_, map) => map.is_empty(),
        }
    }

    /// How much effort is required to drop this value?
    pub fn drop_effort(&self) -> usize {
        match self {
            SortedSet::Pack(_) => 1,
            SortedSet::Skiplist(_, _) => self.len(),
        }
    }

    pub fn contains(&self, value: impl AsRef<[u8]>) -> bool {
        match self {
            SortedSet::Pack(set) => set.contains(&value.as_ref()),
            SortedSet::Skiplist(_, map) => map.contains_key(value.as_ref()),
        }
    }

    pub fn score(&self, value: impl AsRef<[u8]>) -> Option<f64> {
        match self {
            SortedSet::Pack(set) => set.score(&value.as_ref()),
            SortedSet::Skiplist(_, map) => map.get(value.as_ref()).map(|&score| *score),
        }
    }

    pub fn insert<'a, Q>(
        &mut self,
        score: NotNan<f64>,
        value: &'a Q,
        max_len: usize,
        max_size: usize,
    ) -> Option<Insertion>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a + AsRef<[u8]>,
        StringValue: From<&'a Q>,
    {
        if let SortedSet::Pack(_) = self {
            if value.as_ref().pack_size() > max_size {
                self.convert();
            }
        }

        match self {
            SortedSet::Pack(set) => {
                let result = set.insert(score, value.as_ref());
                if set.len() > max_len {
                    self.convert();
                }
                result
            }
            SortedSet::Skiplist(list, map) => {
                let entry = map.entry_ref(value);

                if let EntryRef::Occupied(mut entry) = entry {
                    if *entry.get() == score {
                        None
                    } else {
                        list.remove(**entry.get(), entry.key());
                        list.insert(score, entry.key().clone());
                        entry.insert(score);
                        Some(Insertion::Changed)
                    }
                } else {
                    let entry = entry.insert(score);
                    list.insert(score, entry.key().clone());
                    Some(Insertion::Added)
                }
            }
        }
    }

    pub fn rank(&self, value: impl AsRef<[u8]>) -> Option<usize> {
        match self {
            SortedSet::Pack(set) => set.rank(&value.as_ref()),
            SortedSet::Skiplist(list, map) => {
                let value = value.as_ref();
                let (value, score) = map.get_key_value(value)?;
                list.rank(**score, value)
            }
        }
    }

    pub fn count<R>(&self, bounds: &R) -> usize
    where
        R: RangeBounds<f64>,
    {
        match self {
            SortedSet::Pack(set) => set.count(bounds),
            SortedSet::Skiplist(list, _) => list.count(bounds),
        }
    }

    pub fn remove(&mut self, value: impl AsRef<[u8]>) -> bool {
        match self {
            SortedSet::Pack(set) => set.remove(&value.as_ref()),
            SortedSet::Skiplist(list, map) => {
                if let EntryRef::Occupied(entry) = map.entry_ref(value.as_ref()) {
                    let (value, score) = entry.remove_entry();
                    list.remove(*score, &value);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn remove_range_score<R>(&mut self, bounds: &R) -> usize
    where
        R: RangeBounds<f64>,
    {
        match self {
            SortedSet::Pack(set) => set.remove_range_score(bounds),
            SortedSet::Skiplist(list, map) => list.remove_range_score(bounds, |value| {
                map.remove(value);
            }),
        }
    }

    pub fn pop(&mut self, extreme: Extreme) -> Option<(f64, SortedSetValue)> {
        match self {
            SortedSet::Pack(set) => set.pop(extreme).map(|(score, value)| (score, value.into())),
            SortedSet::Skiplist(list, map) => list.pop(extreme).map(|(score, value)| {
                map.remove(&value);
                (score, value.into())
            }),
        }
    }

    pub fn range(&self, range: Range<usize>) -> impl ExactSizeIterator<Item = (f64, SortedSetRef)> {
        match self {
            SortedSet::Pack(set) => Iter::Pack(set.range(range)),
            SortedSet::Skiplist(list, _) => Iter::Skiplist(list.range(range)),
        }
    }

    pub fn rev_range(
        &self,
        range: Range<usize>,
    ) -> impl ExactSizeIterator<Item = (f64, SortedSetRef)> {
        match self {
            SortedSet::Pack(set) => Iter::Pack(set.rev_range(range)),
            SortedSet::Skiplist(list, _) => Iter::Skiplist(list.rev_range(range)),
        }
    }

    pub fn range_score<'a, R>(
        &'a self,
        bounds: &'a R,
    ) -> impl ExactSizeIterator<Item = (f64, SortedSetRef<'a>)>
    where
        R: RangeBounds<f64>,
    {
        match self {
            SortedSet::Pack(set) => Iter::Pack(set.range_score(bounds)),
            SortedSet::Skiplist(list, _) => Iter::Skiplist(list.range_score(bounds)),
        }
    }

    pub fn rev_range_score<'a, R>(
        &'a self,
        bounds: &'a R,
    ) -> impl ExactSizeIterator<Item = (f64, SortedSetRef<'a>)>
    where
        R: RangeBounds<f64>,
    {
        match self {
            SortedSet::Pack(set) => Iter::Pack(set.rev_range_score(bounds)),
            SortedSet::Skiplist(list, _) => Iter::Skiplist(list.rev_range_score(bounds)),
        }
    }

    fn convert(&mut self) {
        match self {
            SortedSet::Skiplist(_, _) => {}
            SortedSet::Pack(set) => {
                let mut list = Skiplist::default();
                let mut map = HashMap::default();
                for (score, value) in set.iter().rev() {
                    let score = NotNan::new(score).unwrap();
                    let value: StringValue = value.into();
                    map.insert(value.clone(), score);
                    list.insert(score, value);
                }
                *self = SortedSet::Skiplist(list, map);
            }
        }
    }
}

pub enum Iter<P, S> {
    Pack(P),
    Skiplist(S),
}

impl<'a, P, S> Iterator for Iter<P, S>
where
    P: Iterator<Item = (f64, PackRef<'a>)>,
    S: Iterator<Item = (f64, &'a StringValue)>,
{
    type Item = (f64, SortedSetRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Pack(iter) => iter.next().map(|(score, value)| (score, value.into())),
            Iter::Skiplist(iter) => iter.next().map(|(score, value)| (score, value.into())),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Pack(iter) => iter.size_hint(),
            Iter::Skiplist(iter) => iter.size_hint(),
        }
    }
}

impl<'a, P, S> ExactSizeIterator for Iter<P, S>
where
    P: ExactSizeIterator<Item = (f64, PackRef<'a>)>,
    S: ExactSizeIterator<Item = (f64, &'a StringValue)>,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn size() {
        assert_eq!(80, std::mem::size_of::<SortedSet>());
    }
}
