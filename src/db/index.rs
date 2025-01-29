/// A database index in a [`Store`][`crate::Store`] for formatting and type safety.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DBIndex(
    /// The numeric index of a database in a [`Store`][`crate::Store`].
    pub usize,
);

impl std::fmt::Display for DBIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
