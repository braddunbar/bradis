use crate::db::{Extreme, StringValue};
use ordered_float::NotNan;
use rand::Rng;
use seq_macro::seq;
use std::{
    cmp::{Ordering, PartialOrd},
    marker::PhantomData,
    ops::{Bound, Range, RangeBounds},
    ptr::{NonNull, null_mut},
};

/// The maximum number of levels in a node.
const MAX_LEVEL: usize = 32;

/// The chance of adding another level.
const P: f64 = 0.25;

/// One link in a skiplist node.
type Link = NonNull<Node<[Lane]>>;

/// One step in a walk operation.
struct Step<'a> {
    /// A link to the node for this step.
    link: Link,

    /// The node for this step.
    node: &'a Node<[Lane]>,

    /// The level for this step.
    level: usize,

    /// The rank of the node.
    rank: usize,
}

/// The result of one step, directing the next one.
enum Walk<T> {
    /// Move "down" to the next level.
    NextLevel,

    /// Move "across" to the next node.
    NextNode,

    /// End the walk, returning a value.
    Return(Option<T>),
}

/// One step in a mutable walk operation.
struct StepMut<'a> {
    /// A link to the node for this step.
    link: Link,

    /// The node for this step.
    node: &'a mut Node<[Lane]>,
}

/// The result of one mutable step, directing the next one.
enum WalkMut {
    /// Move "down" to the next level.
    NextLevel,

    /// Move "across" to the next node.
    NextNode,
}

/// One node in a skiplist. Currently, dynamically sized types can only be
/// created with a static length. This means we have to create a static array
/// of functions - one for each possible size.
/// <https://doc.rust-lang.org/nomicon/exotic-sizes.html>
#[derive(Debug)]
pub struct Node<T: ?Sized> {
    /// The score associated with the node's value.
    score: NotNan<f64>,

    /// The value associated with the node.
    value: StringValue,

    /// A link to the previous node, for iterating in reverse.
    previous: Option<Link>,

    /// A slice of lanes, pointing to the next node in the chain.
    lanes: T,
}

seq!(N in 1..=32 {
    fn new_node~N(score: NotNan<f64>, value: StringValue) -> Link {
        let node: Node<[Lane; N]> = Node {
            score,
            value,
            previous: None,
            lanes: [Lane::default(); N],
        };
        let node: Box<Node<[Lane]>> = Box::new(node);
        Box::leak(node).into()
    }
});

seq!(N in 1..=32 {
    type NewNode = fn(NotNan<f64>, StringValue) -> Link;
    static NEW_NODE: [NewNode; MAX_LEVEL] = [
        #(new_node~N,)*
    ];
});

impl Node<[Lane]> {
    /// Create a new node with the correct number of lanes.
    pub fn new(score: NotNan<f64>, value: StringValue) -> Link {
        let mut level = 1;
        let mut rng = rand::thread_rng();

        while level < MAX_LEVEL && rng.r#gen::<f64>() < P {
            level += 1;
        }

        NEW_NODE[level - 1](score, value)
    }

    /// The maximum level of this node.
    pub fn level(&self) -> usize {
        self.lanes.len()
    }

    /// Return `true` if the node is before `bounds`.
    fn before<R: RangeBounds<f64>>(&self, bounds: &R) -> bool {
        use Bound::*;
        match bounds.start_bound() {
            Excluded(start) => *self.score <= *start,
            Included(start) => *self.score < *start,
            _ => false,
        }
    }

    /// Return `true` if the node is after `bounds`.
    fn after<R: RangeBounds<f64>>(&self, bounds: &R) -> bool {
        use Bound::*;
        match bounds.end_bound() {
            Excluded(end) => *self.score >= *end,
            Included(end) => *self.score > *end,
            _ => false,
        }
    }
}

unsafe impl<T> Send for Node<T> {}

impl PartialEq<(f64, &StringValue)> for Node<[Lane]> {
    fn eq(&self, other: &(f64, &StringValue)) -> bool {
        self.score == other.0 && &self.value == other.1
    }
}

impl PartialOrd<(f64, &StringValue)> for Node<[Lane]> {
    fn partial_cmp(&self, other: &(f64, &StringValue)) -> Option<Ordering> {
        (*self.score, &self.value).partial_cmp(other)
    }
}

/// One link in a list of nodes at a particular level.
#[derive(Clone, Copy, Default)]
pub struct Lane {
    /// A link to the next node at this level.
    next: Option<Link>,

    /// The number of nodes in the skiplist between this one and the next.
    span: usize,
}

impl std::fmt::Debug for Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lane(")?;
        if let Some(next) = self.next {
            let node = unsafe { next.as_ref() };
            write!(f, "Node({:?}, {:?})", *node.score, node.value)?;
        } else {
            write!(f, "None")?;
        }
        write!(f, ", {:?})", self.span)?;
        Ok(())
    }
}

/// The route taken to find a particular node.
pub struct Route([*mut Lane; MAX_LEVEL]);

impl Default for Route {
    fn default() -> Self {
        Self([null_mut(); MAX_LEVEL])
    }
}

impl std::ops::Deref for Route {
    type Target = [*mut Lane];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl std::ops::DerefMut for Route {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[..]
    }
}

/// A [skiplist](https://en.wikipedia.org/wiki/Skip_list), with a few extras
/// for redis specific functionality.
pub struct Skiplist {
    /// The number of elements in the list.
    len: usize,

    /// A link to the first node at each level.
    head: Box<[Lane; MAX_LEVEL]>,

    /// The last element in the list, for iterating in reverse.
    tail: Option<Link>,

    /// The maximum level of a node in the list.
    level: usize,
}

unsafe impl Send for Skiplist {}

impl PartialEq for Skiplist {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl std::fmt::Debug for Skiplist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl Default for Skiplist {
    fn default() -> Self {
        Self {
            len: 0,
            head: Box::new([Lane::default(); MAX_LEVEL]),
            tail: None,
            level: 0,
        }
    }
}

impl Clone for Skiplist {
    fn clone(&self) -> Self {
        let mut list = Skiplist::default();
        for (score, value) in self.iter_rev() {
            list.insert(NotNan::new(score).unwrap(), value.clone());
        }
        list
    }
}

impl Drop for Skiplist {
    fn drop(&mut self) {
        let mut lane = self.head[0];
        while let Some(next) = lane.next {
            let node = unsafe { Box::from_raw(next.as_ptr()) };
            lane = node.lanes[0];
        }
    }
}

impl Skiplist {
    /// Return the number of elements in the list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Pop an element from the `extreme` end of the list.
    pub fn pop(&mut self, extreme: Extreme) -> Option<(f64, StringValue)> {
        let (score, value) = match extreme {
            Extreme::Min => self.iter().next()?,
            Extreme::Max => self.iter_rev().next()?,
        };
        let value = value.clone();
        self.remove(score, &value);
        Some((score, value.clone()))
    }

    /// Insert `score` and `value` into the list.
    pub fn insert(&mut self, score: NotNan<f64>, value: StringValue) {
        let mut found = false;
        let mut previous = None;
        let (mut route, mut ranks) = self.walk_mut(|step| {
            if *step.node < (*score, &value) {
                previous = Some(step.link);
                return WalkMut::NextNode;
            }

            if *step.node == (*score, &value) {
                found = true;
            }

            WalkMut::NextLevel
        });

        if found {
            return;
        }

        let mut link = Node::new(score, value);
        let node = unsafe { link.as_mut() };
        node.previous = previous;

        for level in 0..std::cmp::max(self.level, node.level()) {
            // Fill in new levels if the node is taller than the list
            if level >= self.level {
                self.head[level] = Lane {
                    next: None,
                    span: self.len,
                };
                route[level] = &mut self.head[level];
                ranks[level] = 0;
                self.level += 1;
            }

            let stop = unsafe { &mut *route[level] };

            if let Some(ref mut lane) = node.lanes.get_mut(level) {
                let span = ranks[0] - ranks[level];
                lane.span = stop.span - span;
                stop.span = span + 1;

                lane.next = stop.next;
                stop.next = Some(link);
            } else {
                stop.span += 1;
            }
        }

        if let Some(mut next) = node.lanes[0].next {
            let next = unsafe { next.as_mut() };
            next.previous = Some(link);
        } else {
            self.tail = Some(link);
        }

        self.len += 1;
    }

    /// Unlink an element from the list, following `route`.
    fn unlink(&mut self, link: Link, route: &mut Route) {
        let node = unsafe { Box::from_raw(link.as_ptr()) };
        for level in 0..self.level {
            let stop = unsafe { &mut *route[level] };
            if let Some(ref mut lane) = node.lanes.get(level) {
                // Subtract separately because lane.span - 1 can overflow
                stop.span -= 1;
                stop.span += lane.span;
                stop.next = lane.next;
            } else {
                stop.span -= 1;
            }
        }

        if let Some(mut next) = node.lanes[0].next {
            unsafe { next.as_mut() }.previous = node.previous;
        } else {
            self.tail = node.previous;
        }

        while self.level > 1 && self.head[self.level - 1].next.is_none() {
            self.level -= 1;
        }

        self.len -= 1;
    }

    /// Remove a `score` `value` pair from the list.
    /// Return `true` if the pair is removed.
    pub fn remove(&mut self, score: f64, value: &StringValue) -> bool {
        let mut link = None;

        let (mut route, _) = self.walk_mut(|step| {
            if *step.node < (score, value) {
                return WalkMut::NextNode;
            }

            if *step.node == (score, value) {
                link = Some(step.link);
            }

            WalkMut::NextLevel
        });

        if let Some(link) = link {
            self.unlink(link, &mut route);
            return true;
        }

        false
    }

    /// Remove all elements contained in `bounds` and call `f` with each.
    pub fn remove_range_score<R, F>(&mut self, bounds: &R, mut f: F) -> usize
    where
        R: RangeBounds<f64>,
        F: FnMut(&StringValue),
    {
        let mut next = None;

        let (mut route, _) = self.walk_mut(|step| {
            if step.node.before(bounds) {
                return WalkMut::NextNode;
            }

            if !step.node.after(bounds) {
                next = Some(step.link);
            }

            WalkMut::NextLevel
        });

        let mut count = 0;

        while let Some(link) = next {
            let node = unsafe { link.as_ref() };
            if node.after(bounds) {
                break;
            }
            count += 1;
            f(&node.value);
            next = node.lanes[0].next;
            self.unlink(link, &mut route);
        }

        count
    }

    /// Return the rank of a `score` `value` pair.
    pub fn rank(&self, score: f64, value: &StringValue) -> Option<usize> {
        self.walk(|step| {
            if *step.node > (score, value) {
                return Walk::NextLevel;
            }

            if *step.node == (score, value) {
                return Walk::Return(Some(step.rank));
            }

            Walk::NextNode
        })
    }

    /// Return the number of elements within `bounds`.
    pub fn count<R>(&self, bounds: &R) -> usize
    where
        R: RangeBounds<f64>,
    {
        self.first_and_last(bounds).map_or(0, |(_, _, count)| count)
    }

    /// Get a link to the element at index `n`.
    fn nth(&self, n: usize) -> Option<Link> {
        if n >= self.len() {
            return None;
        }

        self.walk(|step| {
            if step.rank == n {
                return Walk::Return(Some(step.link));
            }

            // Move to the next level if we've passed the end
            if step.rank > n {
                return Walk::NextLevel;
            }

            Walk::NextNode
        })
    }

    /// Get the first element within `bounds` and its rank.
    fn first<R>(&self, bounds: &R) -> Option<(Link, usize)>
    where
        R: RangeBounds<f64>,
    {
        self.walk(|step| {
            if step.node.before(bounds) {
                return Walk::NextNode;
            }

            if step.level != 0 {
                return Walk::NextLevel;
            }

            if step.node.after(bounds) {
                Walk::Return(None)
            } else {
                Walk::Return(Some((step.link, step.rank)))
            }
        })
    }

    /// Get the last element within `bounds` and its rank.
    fn last<R>(&self, bounds: &R) -> Option<(Link, usize)>
    where
        R: RangeBounds<f64>,
    {
        let mut result = None;

        self.walk(|step| {
            if step.node.before(bounds) {
                return Walk::NextNode;
            }

            if step.node.after(bounds) {
                if step.level == 0 {
                    return Walk::Return(result);
                } else {
                    return Walk::NextLevel;
                }
            }

            result = Some((step.link, step.rank));
            Walk::NextNode
        })
        .or(result)
    }

    /// Get the first and last element in `bounds`
    /// and the distance between them.
    fn first_and_last<R>(&self, bounds: &R) -> Option<(Link, Link, usize)>
    where
        R: RangeBounds<f64>,
    {
        let (first, start) = self.first(bounds)?;
        let (last, end) = self.last(bounds)?;

        Some((first, last, 1 + end - start))
    }

    /// Return an iterator over the elements in the list.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter::new(self.head[0].next, self.len())
    }

    /// Return a reverse iterator over the elements in the list.
    pub fn iter_rev<'a>(&'a self) -> Iter<'a> {
        Iter::rev(self.tail, self.len())
    }

    /// Return an iterator over all elements in `range`.
    pub fn range<'a>(&'a self, range: Range<usize>) -> Iter<'a> {
        let end = std::cmp::min(range.end, self.len);
        let len = end.saturating_sub(range.start);
        Iter::new(self.nth(range.start), len)
    }

    /// Return a reverse iterator over all elements in `range`.
    pub fn rev_range<'a>(&'a self, range: Range<usize>) -> Iter<'a> {
        let end = std::cmp::min(range.end, self.len);
        let len = end.saturating_sub(range.start);
        Iter::rev(self.nth(range.end.saturating_sub(1)), len)
    }

    /// Return an iterator over all elements in `bounds`.
    pub fn range_score<'a, R>(&'a self, bounds: &R) -> Iter<'a>
    where
        R: RangeBounds<f64>,
    {
        let (first, len) = match self.first_and_last(bounds) {
            Some((first, _, len)) => (Some(first), len),
            None => (None, 0),
        };

        Iter::new(first, len)
    }

    /// Return a reverse iterator over all elements in `bounds`.
    pub fn rev_range_score<'a, R>(&'a self, bounds: &R) -> Iter<'a>
    where
        R: RangeBounds<f64>,
    {
        let (last, len) = match self.first_and_last(bounds) {
            Some((_, last, len)) => (Some(last), len),
            None => (None, 0),
        };

        Iter::rev(last, len)
    }

    /// Walk the list, calling `f` for each step and continuing
    /// according to the result.
    fn walk<F, T>(&self, mut f: F) -> Option<T>
    where
        F: FnMut(Step) -> Walk<T>,
    {
        let mut rank = 0;
        let mut lanes = &self.head[..];

        for level in (0..self.level).rev() {
            while let Some(link) = lanes[level].next {
                let span = lanes[level].span;
                let node = unsafe { link.as_ref() };
                let step = Step {
                    link,
                    node,
                    level,
                    rank: rank + span - 1,
                };
                use Walk::*;
                lanes = match f(step) {
                    NextLevel => break,
                    NextNode => {
                        rank += span;
                        &node.lanes[..]
                    }
                    Return(result) => return result,
                };
            }
        }

        None
    }

    /// Walk the list, calling `f` for each step and returning the route
    /// and ranks taken.
    fn walk_mut<F>(&mut self, mut f: F) -> (Route, [usize; MAX_LEVEL])
    where
        F: FnMut(StepMut) -> WalkMut,
    {
        let mut rank = 0;
        let mut lanes = &mut self.head[..];
        let mut route = Route::default();
        let mut ranks: [usize; MAX_LEVEL] = [0; MAX_LEVEL];

        for level in (0..self.level).rev() {
            ranks[level] = rank;
            while let Some(mut link) = lanes[level].next {
                let span = lanes[level].span;
                let node = unsafe { link.as_mut() };
                let step = StepMut { link, node };
                use WalkMut::*;
                lanes = match f(step) {
                    NextLevel => break,
                    NextNode => {
                        rank += span;
                        ranks[level] = rank;
                        &mut node.lanes[..]
                    }
                };
            }
            route[level] = &mut lanes[level];
        }

        (route, ranks)
    }
}

/// An iterator over the elements in a list.
pub struct Iter<'a> {
    node: Option<Link>,
    phantom: PhantomData<(&'a NotNan<f64>, &'a StringValue)>,
    remaining: usize,
    reverse: bool,
}

impl Iter<'_> {
    fn new(node: Option<Link>, remaining: usize) -> Self {
        Self {
            node,
            phantom: PhantomData,
            remaining,
            reverse: false,
        }
    }

    fn rev(node: Option<Link>, remaining: usize) -> Self {
        Self {
            node,
            phantom: PhantomData,
            remaining,
            reverse: true,
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (f64, &'a StringValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let node = self.node.expect("incorrect skiplist length");
        let node = unsafe { node.as_ref() };
        self.node = if self.reverse {
            node.previous
        } else {
            node.lanes[0].next
        };
        self.remaining -= 1;
        Some((*node.score, &node.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for Iter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! skiplist {
        ( $(($score:expr, $value:expr)),* $(,)?) => {{
            let mut list = Skiplist::default();
            $(list.insert(NotNan::new($score).unwrap(), $value.into());)*
            list
        }};
    }

    macro_rules! assert_skiplist_eq {
        ($iter:expr, $(($score:expr, $value:expr)),* $(,)?) => {{
            let mut buffer = Vec::new();
            let expected: Vec<(f64, StringValue)> = vec![$(($score, $value[..].into()),)*];
            let actual: Vec<(f64, StringValue)> = $iter.map(|(score, value)| {
                (score, value.as_bytes(&mut buffer).into())
            }).collect();
            assert_eq!(expected, actual);
        }};
    }

    #[test]
    fn insert_and_remove() {
        let mut list = skiplist!(
            (1f64, b"b"),
            (2f64, b"c"),
            (0f64, b"a"),
            (3f64, b"x"),
            (7f64, b"z"),
            (5f64, b"y"),
            (5f64, b"y")
        );

        assert_eq!(list.len(), 6);

        list.remove(1f64, &b"b".into());
        list.remove(5f64, &b"y".into());

        assert_eq!(list.len(), 4);

        let expected = skiplist!((0f64, b"a"), (2f64, b"c"), (3f64, b"x"), (7f64, b"z"));

        assert_eq!(expected, list);
    }

    #[test]
    fn rank() {
        let list = skiplist!(
            (1f64, b"a"),
            (2f64, b"b"),
            (3f64, b"c"),
            (4f64, b"d"),
            (5f64, b"e"),
            (6f64, b"f")
        );

        assert_eq!(list.rank(1f64, &b"a".into()), Some(0));
        assert_eq!(list.rank(3f64, &b"c".into()), Some(2));
        assert_eq!(list.rank(3f64, &b"nope".into()), None);
        assert_eq!(list.rank(6f64, &b"f".into()), Some(5));
    }

    #[test]
    fn count() {
        let list = skiplist!(
            (1f64, b"a"),
            (2f64, b"b"),
            (3f64, b"c"),
            (4f64, b"d"),
            (5f64, b"e"),
            (6f64, b"f")
        );

        assert_eq!(list.count(&(2f64..=5f64)), 4);
        assert_eq!(list.count(&(3f64..=5f64)), 3);
        assert_eq!(list.count(&(2f64..2f64)), 0);
        assert_eq!(list.count(&(..2f64)), 1);
        assert_eq!(list.count(&(2f64..)), 5);
    }

    #[test]
    fn range() {
        let list = skiplist!(
            (0f64, b"a"),
            (1f64, b"b"),
            (2f64, b"c"),
            (3f64, b"x"),
            (4f64, b"y"),
            (5f64, b"z"),
        );

        assert_eq!(list.len(), 6);
        assert_skiplist_eq!(list.range(0..3), (0f64, b"a"), (1f64, b"b"), (2f64, b"c"));
        assert_skiplist_eq!(list.range(4..12), (4f64, b"y"), (5f64, b"z"));
        assert_eq!(0, list.range(10..12).count());
        assert_skiplist_eq!(list.range(0..2), (0f64, b"a"), (1f64, b"b"));
        assert_skiplist_eq!(list.range(3..6), (3f64, b"x"), (4f64, b"y"), (5f64, b"z"));
        assert_skiplist_eq!(list.range(3..5), (3f64, b"x"), (4f64, b"y"));
        assert_skiplist_eq!(list.rev_range(3..5), (4f64, b"y"), (3f64, b"x"));
        assert_skiplist_eq!(list.range_score(&(0f64..2f64)), (0f64, b"a"), (1f64, b"b"));

        assert_skiplist_eq!(
            list.range_score(&(0f64..=2f64)),
            (0f64, b"a"),
            (1f64, b"b"),
            (2f64, b"c"),
        );

        assert_skiplist_eq!(
            list.rev_range_score(&(0f64..2f64)),
            (1f64, b"b"),
            (0f64, b"a"),
        );

        assert_skiplist_eq!(
            list.rev_range_score(&(0f64..=2f64)),
            (2f64, b"c"),
            (1f64, b"b"),
            (0f64, b"a"),
        );
    }

    #[test]
    fn remove_range_score() {
        let mut list = skiplist!(
            (0f64, b"a"),
            (1f64, b"b"),
            (2f64, b"c"),
            (3f64, b"d"),
            (4f64, b"e"),
            (5f64, b"f"),
        );

        assert_eq!(list.remove_range_score(&(1f64..4f64), |_| {}), 3);
        assert_skiplist_eq!(list.iter(), (0f64, b"a"), (4f64, b"e"), (5f64, b"f"));
    }
}
