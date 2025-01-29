use crate::buffer::Buffer;
use arrayvec::ArrayVec;
use std::io::Write;

/// It's often convenient to write a value to the stack instead of the heap.
/// This buffer is used to make sure we can write an entire value without
/// overflowing.
#[derive(Default)]
pub struct ArrayBuffer(ArrayVec<u8, SIZE>);

/// The string representation of [`f64`] can be nearly 5kb.
/// See <https://github.com/redis/redis/pull/3745> for deets.
const SIZE: usize = 5 * 1024;

impl Buffer for ArrayBuffer {
    fn write_f64(&mut self, value: f64) -> &[u8] {
        self.0.clear();
        write!(self.0, "{value}").expect("f64 value too long");
        &self.0[..]
    }

    fn write_i64(&mut self, value: i64) -> &[u8] {
        self.0.clear();
        write!(self.0, "{value}").expect("i64 value too long");
        &self.0[..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write() {
        let mut buffer = ArrayBuffer::default();
        assert_eq!(buffer.write_f64(4.5), b"4.5");

        let mut buffer = ArrayBuffer::default();
        assert_eq!(buffer.write_i64(-45), b"-45");
    }
}
