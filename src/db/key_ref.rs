use hashbrown::Equivalent;
use std::hash::Hash;

/// Implementing this trait allows use as a key ref to look up values in hashes and sets.
/// See hashbrown documentation for details.
pub trait KeyRef<K>: Eq + Equivalent<K> + Hash {}

/// Impl `KeyRef` for all values that impl the prerequisite traits.
impl<K, T> KeyRef<K> for T where T: Eq + Equivalent<K> + Hash + ?Sized {}
