use crate::db::KeyRef;
use std::{
    cmp::{Eq, PartialEq},
    hash::{Hash, Hasher},
    marker::PhantomData,
    ptr::NonNull,
};

use hashbrown::{Equivalent, HashSet};

type Link<T> = Option<NonNull<Node<T>>>;

/// This is one node in a linked list for embedding in a hash table.
#[derive(Debug)]
struct Node<T> {
    next: Link<T>,
    prev: Link<T>,
    value: T,
}

#[derive(Debug)]
struct NodePointer<T>(NonNull<Node<T>>);

unsafe impl<T: Send> Send for NodePointer<T> {}

impl<T: PartialEq> PartialEq for NodePointer<T> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.0.as_ref().value == other.0.as_ref().value }
    }
}

impl<T: Eq> Eq for NodePointer<T> {}

impl<T: Hash> Hash for NodePointer<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            self.0.as_ref().value.hash(state);
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
struct Wrapper<'a, T: ?Sized>(&'a T);

impl<Q, T> Equivalent<NodePointer<T>> for Wrapper<'_, Q>
where
    Q: KeyRef<T> + ?Sized,
{
    fn equivalent(&self, key: &NodePointer<T>) -> bool {
        unsafe { self.0.equivalent(&key.0.as_ref().value) }
    }
}

/// There are several instances in which we need an ordered list of elements with constant time
/// membership and removal operations. For instance, a list of subscribers to a particular PUBSUB
/// key. A linked list embedded in a hash table is a pretty good solution.
pub struct LinkedHashSet<T> {
    front: Link<T>,
    back: Link<T>,
    set: HashSet<NodePointer<T>>,
}

impl<T: Eq + Hash + std::fmt::Debug> std::fmt::Debug for LinkedHashSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl<T> Drop for LinkedHashSet<T> {
    fn drop(&mut self) {
        for node in self.set.drain() {
            unsafe { drop(Box::from_raw(node.0.as_ptr())) };
        }
    }
}

unsafe impl<T: Send> Send for LinkedHashSet<T> {}

impl<T: Eq + Hash> Default for LinkedHashSet<T> {
    fn default() -> Self {
        LinkedHashSet {
            front: None,
            back: None,
            set: HashSet::default(),
        }
    }
}

impl<T: Clone + Eq + Hash> Clone for LinkedHashSet<T> {
    fn clone(&self) -> Self {
        let mut set = LinkedHashSet::new();
        for t in self.iter() {
            set.insert_back(t.clone());
        }
        set
    }
}

impl<T: Eq + Hash> LinkedHashSet<T> {
    pub fn new() -> Self {
        LinkedHashSet::default()
    }

    /// Is this set empty?
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// The number of elements in the set
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Insert an element into the set at the back of the list
    pub fn insert_back(&mut self, value: T) {
        if self.set.contains(&Wrapper(&value)) {
            return;
        }

        let node = Box::leak(Box::new(Node {
            prev: self.back,
            next: None,
            value,
        }))
        .into();

        // Update the back of the list
        if let Some(mut back) = self.back {
            unsafe { back.as_mut() }.next = Some(node);
        }
        self.back = Some(node);

        // Update the front of the list
        if self.front.is_none() {
            self.front = Some(node);
        }

        self.set.insert(NodePointer(node));
    }

    /// Remove an element from the set
    pub fn remove<Q>(&mut self, value: &Q) -> Option<T>
    where
        Q: KeyRef<T> + ?Sized,
    {
        let node = self.set.take(&Wrapper(value))?;
        let node = *unsafe { Box::from_raw(node.0.as_ptr()) };

        let next = node.next;
        let prev = node.prev;

        // Update the previous node
        if let Some(mut prev) = prev {
            unsafe { prev.as_mut() }.next = next;
        } else {
            self.front = next;
        }

        // Update the next node
        if let Some(mut next) = next {
            unsafe { next.as_mut() }.prev = prev;
        } else {
            self.back = prev;
        }

        Some(node.value)
    }

    /// The front element
    pub fn front(&self) -> Option<&T> {
        self.front.map(|node| &unsafe { node.as_ref() }.value)
    }

    /// The back element
    #[cfg(test)]
    pub fn back(&self) -> Option<&T> {
        self.back.map(|node| &unsafe { node.as_ref() }.value)
    }

    /// An iterator over the elements of the set
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        Iter {
            next: self.front,
            phantom: PhantomData,
        }
    }
}

struct Iter<'a, T> {
    next: Link<T>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T: 'a> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.next?;
        let node = unsafe { node.as_ref() };
        self.next = node.next;
        Some(&node.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_twice() {
        let mut set: LinkedHashSet<i64> = LinkedHashSet::new();
        set.insert_back(1);
        set.insert_back(1);
        let items: Vec<_> = set.iter().collect();
        assert_eq!(items, vec![&1]);
    }

    #[test]
    fn insert_back() {
        let mut set: LinkedHashSet<i64> = LinkedHashSet::new();
        assert_eq!(set.front(), None);
        assert_eq!(set.back(), None);

        set.insert_back(1);
        let items: Vec<_> = set.iter().collect();
        assert_eq!(items, vec![&1]);
        assert_eq!(set.len(), 1);
        assert_eq!(set.front(), Some(&1));
        assert_eq!(set.back(), Some(&1));

        set.insert_back(2);
        let items: Vec<_> = set.iter().collect();
        assert_eq!(items, vec![&1, &2]);
        assert_eq!(set.len(), 2);
        assert_eq!(set.front(), Some(&1));
        assert_eq!(set.back(), Some(&2));

        set.insert_back(3);
        let items: Vec<_> = set.iter().collect();
        assert_eq!(items, vec![&1, &2, &3]);
        assert_eq!(set.len(), 3);
        assert_eq!(set.front(), Some(&1));
        assert_eq!(set.back(), Some(&3));

        set.remove(&2);
        let items: Vec<&i64> = set.iter().collect();
        assert_eq!(items, vec![&1, &3]);
        assert_eq!(set.len(), 2);
        assert_eq!(set.front(), Some(&1));
        assert_eq!(set.back(), Some(&3));

        set.remove(&1);
        let items: Vec<&i64> = set.iter().collect();
        assert_eq!(items, vec![&3]);
        assert_eq!(set.len(), 1);
        assert_eq!(set.front(), Some(&3));
        assert_eq!(set.back(), Some(&3));

        set.remove(&3);

        assert_eq!(set.iter().count(), 0);
        assert_eq!(set.len(), 0);
        assert_eq!(set.front(), None);
        assert_eq!(set.back(), None);
    }

    #[test]
    fn borrow() {
        let mut set: LinkedHashSet<Vec<u8>> = LinkedHashSet::new();
        set.insert_back(b"foo".to_vec());
        assert_eq!(set.len(), 1);

        set.remove(&b"foo"[..]);
        assert!(set.is_empty());
    }
}
