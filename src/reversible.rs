use std::iter::Rev;

/// A wrapper for iterators that can be reversed.
pub enum Reversible<I> {
    Forward(I),
    Reverse(Rev<I>),
}

impl<I: Iterator<Item = T> + DoubleEndedIterator, T> Iterator for Reversible<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        use Reversible::*;
        match self {
            Forward(f) => f.next(),
            Reverse(r) => r.next(),
        }
    }
}

impl<I: Iterator<Item = T> + DoubleEndedIterator, T> DoubleEndedIterator for Reversible<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        use Reversible::*;
        match self {
            Forward(f) => f.next_back(),
            Reverse(r) => r.next_back(),
        }
    }
}

impl<I: Iterator<Item = T> + DoubleEndedIterator + ExactSizeIterator, T> ExactSizeIterator
    for Reversible<I>
{
    fn len(&self) -> usize {
        use Reversible::*;
        match self {
            Forward(f) => f.len(),
            Reverse(r) => r.len(),
        }
    }
}
