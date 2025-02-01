use crate::db::Edge;
use std::{marker::PhantomData, ptr::NonNull};

type Link<T> = Option<NonNull<Node<T>>>;

struct Node<T> {
    prev: Link<T>,
    next: Link<T>,
    value: T,
}

impl<T> Node<T> {
    fn link(self) -> Link<T> {
        Some(Box::leak(Box::new(self)).into())
    }
}

/// <http://antirez.com/news/138>
pub struct LinkedList<T> {
    front: Link<T>,
    back: Link<T>,
    len: usize,
}

unsafe impl<T: Send> Send for LinkedList<T> {}

impl<T: PartialEq> PartialEq for LinkedList<T> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other)
    }
}

impl<T: Eq> Eq for LinkedList<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for LinkedList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self).finish()
    }
}

impl<T> Default for LinkedList<T> {
    fn default() -> Self {
        Self {
            front: None,
            back: None,
            len: 0,
        }
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        while self.pop(Edge::Left).is_some() {}
    }
}

impl<T: Clone> Clone for LinkedList<T> {
    fn clone(&self) -> Self {
        let mut list = Self::default();
        for item in self {
            list.push_back(item.clone());
        }
        list
    }
}

impl<T> LinkedList<T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push_front(&mut self, value: T) {
        let new = Node {
            prev: None,
            next: self.front,
            value,
        }
        .link();

        if let Some(mut front) = self.front {
            unsafe { front.as_mut() }.prev = new;
        } else {
            self.back = new;
        }

        self.front = new;
        self.len += 1;
    }

    pub fn push_back(&mut self, value: T) {
        let new = Node {
            prev: self.back,
            next: None,
            value,
        }
        .link();

        if let Some(mut back) = self.back {
            unsafe { back.as_mut() }.next = new;
        } else {
            self.front = new;
        }

        self.back = new;
        self.len += 1;
    }

    pub fn push(&mut self, value: T, edge: Edge) {
        match edge {
            Edge::Left => self.push_front(value),
            Edge::Right => self.push_back(value),
        }
    }

    pub fn pop(&mut self, edge: Edge) -> Option<T> {
        let link = match edge {
            Edge::Left => self.front,
            Edge::Right => self.back,
        }?;

        Some(self.remove(link).value)
    }

    pub fn front(&self) -> Option<&T> {
        let front = self.front?;
        Some(&unsafe { front.as_ref() }.value)
    }

    pub fn front_mut(&mut self) -> Option<&mut T> {
        let mut front = self.front?;
        Some(&mut unsafe { front.as_mut() }.value)
    }

    pub fn back(&self) -> Option<&T> {
        let back = self.back?;
        Some(&unsafe { back.as_ref() }.value)
    }

    pub fn back_mut(&mut self) -> Option<&mut T> {
        let mut back = self.back?;
        Some(&mut unsafe { back.as_mut() }.value)
    }

    pub fn edge(&self, edge: Edge) -> Option<&T> {
        match edge {
            Edge::Left => self.front(),
            Edge::Right => self.back(),
        }
    }

    pub fn edge_mut(&mut self, edge: Edge) -> Option<&mut T> {
        match edge {
            Edge::Left => self.front_mut(),
            Edge::Right => self.back_mut(),
        }
    }

    pub fn iter(&self) -> Iter<T> {
        Iter {
            front: self.front,
            back: self.back,
            len: self.len,
            phantom: PhantomData,
        }
    }

    pub fn cursor(&mut self, edge: Edge) -> Cursor<'_, T> {
        let next = self.front;
        let prev = self.back;
        match edge {
            Edge::Left => Cursor {
                list: self,
                next,
                prev: None,
                reverse: false,
            },
            Edge::Right => Cursor {
                list: self,
                next: None,
                prev,
                reverse: true,
            },
        }
    }

    fn remove(&mut self, link: NonNull<Node<T>>) -> Node<T> {
        let node = unsafe { Box::from_raw(link.as_ptr()) };

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

        self.len -= 1;
        *node
    }
}

pub struct Iter<'a, T> {
    front: Link<T>,
    back: Link<T>,
    len: usize,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }

        let front = self.front?;
        self.len -= 1;
        let node = unsafe { front.as_ref() };
        self.front = node.next;
        Some(&node.value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }

        let back = self.back?;
        self.len -= 1;
        let node = unsafe { back.as_ref() };
        self.back = node.prev;
        Some(&node.value)
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a, T> IntoIterator for &'a LinkedList<T> {
    type IntoIter = Iter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct Cursor<'a, T> {
    list: &'a mut LinkedList<T>,
    next: Link<T>,
    prev: Link<T>,
    reverse: bool,
}

impl<T> Cursor<'_, T> {
    pub fn peek_next(&mut self) -> Option<&mut T> {
        let link = if self.reverse { self.prev } else { self.next };
        link.map(|mut link| &mut unsafe { link.as_mut() }.value)
    }

    pub fn peek_prev(&mut self) -> Option<&mut T> {
        let link = if self.reverse { self.next } else { self.prev };
        link.map(|mut link| &mut unsafe { link.as_mut() }.value)
    }

    pub fn next(&mut self) -> Option<&mut T> {
        if self.reverse {
            self.back()
        } else {
            self.forward()
        }
    }

    pub fn prev(&mut self) -> Option<&mut T> {
        if self.reverse {
            self.forward()
        } else {
            self.back()
        }
    }

    fn forward(&mut self) -> Option<&mut T> {
        if let Some(mut next) = self.next {
            let node = unsafe { next.as_mut() };
            self.prev = self.next;
            self.next = node.next;
            Some(&mut node.value)
        } else {
            self.prev = None;
            self.next = self.list.front;
            None
        }
    }

    fn back(&mut self) -> Option<&mut T> {
        if let Some(mut prev) = self.prev {
            let node = unsafe { prev.as_mut() };
            self.next = self.prev;
            self.prev = node.prev;
            Some(&mut node.value)
        } else {
            self.next = None;
            self.prev = self.list.back;
            None
        }
    }

    pub fn remove(&mut self) -> Option<T> {
        if self.reverse {
            let prev = self.prev?;
            let node = self.list.remove(prev);
            self.prev = node.prev;
            Some(node.value)
        } else {
            let next = self.next?;
            let node = self.list.remove(next);
            self.next = node.next;
            Some(node.value)
        }
    }

    pub fn insert(&mut self, value: T) {
        self.list.len += 1;
        let new = Node {
            prev: self.prev,
            next: self.next,
            value,
        }
        .link();

        // Update the previous node
        if let Some(mut link) = self.prev {
            let node = unsafe { link.as_mut() };
            node.next = new;
        } else {
            self.list.front = new;
        }

        // Update the next node
        if let Some(mut link) = self.next {
            let node = unsafe { link.as_mut() };
            node.prev = new;
        } else {
            self.list.back = new;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_front() {
        let mut list = LinkedList::default();
        assert_eq!(list.front(), None);
        assert_eq!(list.back(), None);
        list.push_front(2);
        list.push_front(1);
        list.push_front(0);
        assert_eq!(list.front(), Some(&0));
        assert_eq!(list.edge(Edge::Left), Some(&0));
        assert_eq!(list.back(), Some(&2));
        assert_eq!(list.edge(Edge::Right), Some(&2));
        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn push_back() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn push() {
        let mut list = LinkedList::default();
        list.push_back(1);
        list.push(0, Edge::Left);
        list.push(2, Edge::Right);
        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn cursor_next_wrapping() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut cursor = list.cursor(Edge::Left);
        assert_eq!(cursor.next(), Some(&mut 0));
        assert_eq!(cursor.next(), Some(&mut 1));
        assert_eq!(cursor.next(), Some(&mut 2));
        assert_eq!(cursor.next(), None);
        assert_eq!(cursor.next(), Some(&mut 0));
    }

    #[test]
    fn cursor_prev_wrapping() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut cursor = list.cursor(Edge::Left);
        assert_eq!(cursor.prev(), None);
        assert_eq!(cursor.prev(), Some(&mut 2));
        assert_eq!(cursor.prev(), Some(&mut 1));
        assert_eq!(cursor.prev(), Some(&mut 0));
        assert_eq!(cursor.prev(), None);
        assert_eq!(cursor.prev(), Some(&mut 2));
    }

    #[test]
    fn cursor_peek() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut cursor = list.cursor(Edge::Left);
        assert_eq!(cursor.peek_next(), Some(&mut 0));
        assert_eq!(cursor.peek_next(), Some(&mut 0));
        let mut cursor = list.cursor(Edge::Right);
        assert_eq!(cursor.peek_next(), Some(&mut 2));
        assert_eq!(cursor.peek_next(), Some(&mut 2));
    }

    #[test]
    fn cursor_next_reverse() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut cursor = list.cursor(Edge::Right);
        assert_eq!(cursor.next(), Some(&mut 2));
        assert_eq!(cursor.next(), Some(&mut 1));
        assert_eq!(cursor.next(), Some(&mut 0));
        assert_eq!(cursor.next(), None);
        assert_eq!(cursor.next(), Some(&mut 2));
    }

    #[test]
    fn cursor_prev_reverse() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        let mut cursor = list.cursor(Edge::Right);
        assert_eq!(cursor.prev(), None);
        assert_eq!(cursor.prev(), Some(&mut 0));
        assert_eq!(cursor.prev(), Some(&mut 1));
        assert_eq!(cursor.prev(), Some(&mut 2));
        assert_eq!(cursor.prev(), None);
        assert_eq!(cursor.prev(), Some(&mut 0));
    }

    #[test]
    fn remove() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        list.push_back(3);
        assert_eq!(list.len(), 4);

        let mut cursor = list.cursor(Edge::Left);
        cursor.next();
        assert_eq!(cursor.remove(), Some(1));
        assert_eq!(cursor.peek_next(), Some(&mut 2));
        assert_eq!(list.len(), 3);

        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), None);

        let mut cursor = list.cursor(Edge::Right);
        cursor.next();
        assert_eq!(cursor.remove(), Some(2));
        assert_eq!(cursor.peek_next(), Some(&mut 0));
        assert_eq!(list.len(), 2);

        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn pop() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        list.push_back(2);
        list.push_back(3);

        assert_eq!(list.pop(Edge::Left), Some(0));
        assert_eq!(list.pop(Edge::Right), Some(3));

        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn insert() {
        let mut list = LinkedList::default();
        list.push_back(0);
        list.push_back(2);
        assert_eq!(list.len(), 2);

        let mut cursor = list.cursor(Edge::Left);
        cursor.next();
        cursor.insert(1);

        assert_eq!(list.len(), 3);
        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn front_mut_and_back_mut() {
        let mut list: LinkedList<i64> = LinkedList::default();
        list.push_back(0);
        list.push_back(1);
        assert_eq!(list.front(), Some(&0));
        assert_eq!(list.back(), Some(&1));
        *list.back_mut().unwrap() = 0;
        *list.front_mut().unwrap() = 1;
        assert_eq!(list.back(), Some(&0));
        assert_eq!(list.front(), Some(&1));
    }
}
