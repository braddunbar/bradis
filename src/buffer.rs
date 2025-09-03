use std::io::Write;

mod array;

pub use array::ArrayBuffer;

/// In some cases, redis stores string values with different encodings for convenient manipulation.
/// For instance, incrementing a value with `INCR` will cause it to be stored as an integer. In
/// theses cases, we still need to view the value as bytes sometimes (e.g. `INCR` followed by
/// `STRLEN`). In these cases, we can convert the value to bytes on the stack or a shared
/// allocation.
pub trait Buffer {
    /// Write an f64 and return the written slice.
    fn write_f64(&mut self, value: f64) -> &[u8];

    /// Write an i64 and return the written slice.
    fn write_i64(&mut self, value: i64) -> &[u8];
}

impl Buffer for Vec<u8> {
    fn write_f64(&mut self, value: f64) -> &[u8] {
        self.clear();
        let _ = write!(self, "{value}");
        &self[..]
    }

    fn write_i64(&mut self, value: i64) -> &[u8] {
        self.clear();
        let _ = write!(self, "{value}");
        &self[..]
    }
}
