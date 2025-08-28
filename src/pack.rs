mod list;
mod map;
mod packable;
mod r#ref;
mod set;
mod sorted_set;
mod value;

pub use list::{PackList, PackListInsert};
pub use map::PackMap;
pub use packable::Packable;
pub use r#ref::PackRef;
pub use set::PackSet;
pub use sorted_set::PackSortedSet;
pub use value::PackValue;

use crate::db::{Edge, Raw};
use bytes::Buf;

/// An implementation of [ListPack](https://github.com/antirez/listpack/blob/master/listpack.md),
/// containing a packed representation of a list of redis values. Different from the c redis
/// version in several ways…
///
/// * The length and size is stored in the header instead of in the data.
/// * Has a dedicated tag for f64 rather than storing as i64.
/// * Does not append an end byte for detecting the end of the data.
#[derive(Clone, Eq, PartialEq)]
pub struct Pack {
    /// Shareable bytes representing the list of values.
    data: Raw,

    /// The number of values in this pack.
    len: usize,
}

impl Default for Pack {
    fn default() -> Self {
        Self {
            data: Vec::new().into(),
            len: 0,
        }
    }
}

impl std::fmt::Debug for Pack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()?;
        Ok(())
    }
}

impl Pack {
    /// Create a [`Pack`] with a specific `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        let data = Vec::with_capacity(capacity).into();
        Pack { data, len: 0 }
    }

    /// Get a mutable reference to the data.
    pub fn make_mut(&mut self) -> &mut Vec<u8> {
        self.data.make_mut()
    }

    /// Add a value to the beginning of the pack.
    pub fn prepend<T>(&mut self, value: &T)
    where
        T: Packable,
    {
        let mut cursor = self.cursor(Edge::Left);
        cursor.insert(value);
    }

    /// Add a value to the end of the pack.
    pub fn append<T>(&mut self, value: &T)
    where
        T: Packable,
    {
        let mut data = self.make_mut();
        data.reserve(value.pack_size());
        value.pack_write(&mut data);
        self.len += 1;
    }

    /// Add two values to the end of the pack, done together to prevent reallocating twice.
    pub fn append2<A, B>(&mut self, a: &A, b: &B)
    where
        A: Packable,
        B: Packable,
    {
        let mut data = self.make_mut();
        data.reserve(a.pack_size() + b.pack_size());
        a.pack_write(&mut data);
        b.pack_write(&mut data);
        self.len += 2;
    }

    /// The number of values in the pack.
    pub fn len(&self) -> usize {
        self.len
    }

    /// The byte length of the packed data.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Read one value, starting at `offset`, and return it along with the offset of the next
    /// value, or `None` if `offset` is the end of the pack.
    fn read<'a>(&'a self, offset: usize) -> Option<(PackRef<'a>, usize)> {
        use PackRef::*;
        let mut all = self.data.get(offset..)?;

        let value = match all.first()? {
            b if 0xc0 & b == 0x80 => {
                let len = usize::from(!0xc0 & *b);
                let start = offset + 1;
                let end = start + len;

                Slice(self.data.slice(start..end))
            }
            b if 0xf0 & b == 0xe0 => {
                let len = usize::from(0x0fff & all.get_u16());
                let start = offset + 2;
                let end = start + len;

                Slice(self.data.slice(start..end))
            }
            0xf0 => {
                all.advance(1);
                let len = usize::try_from(all.get_u32_le()).unwrap();
                let start = offset + 5;
                let end = start + len;

                Slice(self.data.slice(start..end))
            }
            // u7
            b if 0x80 & b == 0x00 => Integer(i64::from(*b)),
            // i13
            b if 0xe0 & b == 0xc0 => {
                // Shift left and then right to get the correct leading bits
                let n = (all.get_i16() << 3) >> 3;

                Integer(i64::from(n))
            }
            // i16
            0xf1 => {
                all.advance(1);
                Integer(i64::from(all.get_i16_le()))
            }
            // i24
            0xf2 => Integer(i64::from(all.get_i32_le() >> 8)),
            // i32
            0xf3 => {
                all.advance(1);
                Integer(i64::from(all.get_i32_le()))
            }
            // i64
            0xf4 => {
                all.advance(1);
                Integer(all.get_i64_le())
            }
            // f64
            0xf5 => {
                all.advance(1);
                Float(all.get_f64_le())
            }
            _ => panic!("unknown pack encoding"),
        };

        let next = offset + value.size();
        Some((value, next))
    }

    /// Read one value, starting from the offset of the following value, and return it along with
    /// its offset, or `None` if `offset` is the beginning of the pack.
    fn read_rev<'a>(&'a self, mut offset: usize) -> Option<(PackRef<'a>, usize)> {
        if offset == 0 {
            return None;
        }

        offset -= 1;
        let mut len: usize = 0;

        while 0x80 & self.data[offset] == 0x80 {
            len |= usize::from(!0x80 & self.data[offset]);
            len <<= 7;
            offset -= 1;
        }

        len |= usize::from(self.data[offset]);
        offset -= len;
        self.read(offset).map(|(value, _)| (value, offset))
    }

    /// An iterator over the values in the pack.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            pack: self,
            next_front: 0,
            next_back: self.data.len(),
            remaining: self.len(),
        }
    }

    /// A cursor over the values in the pack, starting from `edge`.
    pub fn cursor(&mut self, edge: Edge) -> Cursor<'_> {
        match edge {
            Edge::Left => Cursor {
                index: 0,
                offset: 0,
                pack: self,
                reverse: false,
            },
            Edge::Right => Cursor {
                index: self.len(),
                offset: self.data.len(),
                pack: self,
                reverse: true,
            },
        }
    }

    /// Move an element from one edge to the other.
    pub fn mv(&mut self, from: Edge) {
        let mut cursor = self.cursor(from);
        let element = cursor.peek().unwrap().to_owned().clone();
        let size = element.pack_size();
        let data = self.make_mut();
        let len = data.len();
        data.reserve(size);

        match from {
            Edge::Left => {
                unsafe {
                    // Move the first element to the end.
                    let from = data.as_mut_ptr();
                    let to = from.add(len);
                    from.copy_to(to, size);

                    // Move everything back
                    let to = data.as_mut_ptr();
                    let from = to.add(size);
                    from.copy_to(to, len);
                }
            }
            Edge::Right => {
                unsafe {
                    // Create space on the left
                    let from = data.as_mut_ptr();
                    let to = from.add(size);
                    from.copy_to(to, len);

                    // Copy the last element from the right to the left.
                    let to = data.as_mut_ptr();
                    let from = to.add(len);
                    from.copy_to(to, size);
                }
            }
        }
    }
}

/// A double ended iterator over the values in a pack. By keeping track of the next front and back
/// offset, we can iterate from either end of the pack.
pub struct Iter<'a> {
    pack: &'a Pack,
    next_back: usize,
    next_front: usize,
    remaining: usize,
}

impl<'a> Iter<'a> {
    /// Get the previous value.
    pub fn prev(&mut self) -> Option<PackRef<'a>> {
        let (value, next) = self.pack.read_rev(self.next_front)?;
        self.next_front = next;
        self.remaining += 1;
        Some(value)
    }

    /// Get the previous value from the back (which is the front).
    pub fn prev_back(&mut self) -> Option<PackRef<'a>> {
        let (value, next) = self.pack.read(self.next_back)?;
        self.next_back = next;
        self.remaining += 1;
        Some(value)
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = PackRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_front >= self.next_back {
            return None;
        }

        let (value, next) = self.pack.read(self.next_front)?;
        self.next_front = next;
        self.remaining -= 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next_front >= self.next_back {
            return None;
        }

        let (value, next) = self.pack.read_rev(self.next_back)?;
        self.next_back = next;
        self.remaining -= 1;
        Some(value)
    }
}

impl ExactSizeIterator for Iter<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

/// A cursor over the values in a pack. This enables us to iterate over the pack and make changes
/// much more easily than passing around offsets directly. It also enables us to provide a
/// direction for iterating from the left or from the right.
#[derive(Debug)]
pub struct Cursor<'a> {
    index: usize,
    offset: usize,
    pack: &'a mut Pack,
    reverse: bool,
}

impl Cursor<'_> {
    /// The number of values in the pack.
    pub fn len(&self) -> usize {
        self.pack.len()
    }

    /// The index of the current value.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Skip over `n` values.
    pub fn skip(&mut self, n: usize) {
        for _ in 0..n {
            self.next();
        }
    }

    /// Take a peek at the current value, without consuming it.
    pub fn peek(&mut self) -> Option<PackRef<'_>> {
        if self.reverse {
            self.peek_backward()
        } else {
            self.peek_forward()
        }
    }

    /// Peek at the next value to the right.
    fn peek_forward(&mut self) -> Option<PackRef<'_>> {
        self.pack.read(self.offset).map(|(value, _)| value)
    }

    /// Peek at the next value to the left.
    fn peek_backward(&mut self) -> Option<PackRef<'_>> {
        self.pack.read_rev(self.offset).map(|(value, _)| value)
    }

    /// Consume the next value.
    pub fn next(&mut self) -> Option<PackRef<'_>> {
        if self.reverse {
            self.backward()
        } else {
            self.forward()
        }
    }

    /// Consume the previous value.
    pub fn prev(&mut self) -> Option<PackRef<'_>> {
        if self.reverse {
            self.forward()
        } else {
            self.backward()
        }
    }

    /// Consume the next value to the right.
    fn forward(&mut self) -> Option<PackRef<'_>> {
        if self.index == self.pack.len() {
            self.offset = 0;
            self.index = 0;
            return None;
        }
        self.pack.read(self.offset).map(|(value, next)| {
            self.index += 1;
            self.offset = next;
            value
        })
    }

    /// Consume the next value to the left.
    fn backward(&mut self) -> Option<PackRef<'_>> {
        if self.index == 0 {
            self.offset = self.pack.data.len();
            self.index = self.pack.len();
            return None;
        }
        self.pack.read_rev(self.offset).map(|(value, next)| {
            self.index -= 1;
            self.offset = next;
            value
        })
    }

    /// Split the pack at the current index and return a new pack containing the values to the
    /// right.
    pub fn split(&mut self) -> Pack {
        let len = self.pack.len();
        let data = self.pack.make_mut();
        let pack = Pack {
            data: data[self.offset..].into(),
            len: len - self.index,
        };
        data.truncate(self.offset);
        self.pack.len = self.index;
        pack
    }

    /// Remove the next value in the appropriate direction.
    pub fn remove(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        let mut start = self.offset;
        let mut end = self.offset;

        if self.reverse {
            for _ in 0..count {
                let Some((_, next)) = self.pack.read_rev(start) else {
                    break;
                };
                start = next;
                self.offset = next;
                self.index -= 1;
                self.pack.len -= 1;
            }
        } else {
            for _ in 0..count {
                let Some((_, next)) = self.pack.read(end) else {
                    break;
                };
                end = next;
                self.pack.len -= 1;
            }
        }
        self.pack.make_mut().drain(start..end);
    }

    /// Insert a value at the current index.
    pub fn insert<A>(&mut self, a: &A)
    where
        A: Packable,
    {
        self.pack.len += 1;
        let size = a.pack_size();
        let mut data = self.pack.make_mut();
        data.reserve(size);
        let tail_len = data.len() - self.offset;
        unsafe {
            let from = data.as_mut_ptr().add(self.offset);
            let to = from.add(size);
            from.copy_to(to, tail_len);
            data.set_len(self.offset);
        }
        a.pack_write(&mut data);
        unsafe {
            data.set_len(self.offset + size + tail_len);
        }
    }

    /// Insert two values at the current index together to avoid reallocating twice.
    pub fn insert2<A, B>(&mut self, a: &A, b: &B)
    where
        A: Packable,
        B: Packable,
    {
        self.pack.len += 2;
        let size = a.pack_size() + b.pack_size();
        let mut data = self.pack.make_mut();
        data.reserve(size);
        let tail_len = data.len() - self.offset;
        unsafe {
            let from = data.as_mut_ptr().add(self.offset);
            let to = from.add(size);
            from.copy_to(to, tail_len);
            data.set_len(self.offset);
        }
        a.pack_write(&mut data);
        b.pack_write(&mut data);
        unsafe {
            data.set_len(self.offset + size + tail_len);
        }
    }

    /// Replace the value at the current index.
    pub fn replace<V: Packable>(&mut self, value: &V) {
        let Some(old_size) = self.peek().map(|v| v.size()) else {
            return;
        };
        let offset = if self.reverse {
            self.offset - old_size
        } else {
            self.offset
        };
        let new_size = value.pack_size();
        let mut data = self.pack.make_mut();

        if old_size == new_size {
            value.pack_write(&mut data[offset..]);
            return;
        }

        if let Some(delta) = new_size.checked_sub(old_size) {
            data.reserve(delta);
        }
        let tail_len = data.len() - self.offset - old_size;
        unsafe {
            let start = data.as_mut_ptr().add(self.offset);
            let from = start.add(old_size);
            let to = start.add(new_size);
            from.copy_to(to, tail_len);
            data.set_len(offset);
        }
        value.pack_write(&mut data);
        unsafe {
            data.set_len(offset + new_size + tail_len);
        }
    }
}

impl<I, TI, TV> From<(I, TV)> for Pack
where
    TI: Packable,
    TV: Packable,
    I: Iterator<Item = TI> + Clone,
{
    fn from(value: (I, TV)) -> Self {
        let (iterator, value) = value;
        let sizes = iterator.clone().map(|entry| entry.pack_size());
        let capacity: usize = sizes.sum::<usize>() + value.pack_size();
        let mut pack = Pack::with_capacity(capacity);
        for entry in iterator {
            pack.append(&entry);
        }
        pack.append(&value);
        pack
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_size() {
        // Tiny numbers
        assert_eq!("0".pack_size(), 2);
        assert_eq!("1".pack_size(), 2);
        assert_eq!("52".pack_size(), 2);
        assert_eq!("127".pack_size(), 2);

        // Tiny strings
        assert_eq!("a".pack_size(), 3);
        assert_eq!("ab".pack_size(), 4);
        assert_eq!("abc".pack_size(), 5);
        assert_eq!("abcdefg".pack_size(), 9);
    }

    #[test]
    fn write_empty_string() {
        let mut pack = Pack::default();
        let value: Raw = "".into();
        pack.append(&&value[..]);
        assert_eq!(pack.len(), 1);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(value.slice(0..0).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_tiny_number() {
        let mut pack = Pack::default();
        pack.append(&"0");
        assert_eq!(*pack.data, b"\x00\x01"[..]);
        assert_eq!(pack.data.len(), 2);
        assert_eq!(pack.len(), 1);

        let mut pack = Pack::default();
        pack.append(&"6");
        pack.append(&"8");
        assert_eq!(*pack.data, b"\x06\x01\x08\x01"[..]);
        assert_eq!(pack.data.len(), 4);
        assert_eq!(pack.len(), 2);
    }

    #[test]
    fn read_tiny_number() {
        let mut pack = Pack::default();
        pack.append(&"0");
        assert_eq!(pack.read(0), Some((0.into(), 2)));
        assert_eq!(pack.read(2), None);

        let mut pack = Pack::default();
        pack.append(&"6");
        pack.append(&"8");
        assert_eq!(pack.read(0), Some((6.into(), 2)));
        assert_eq!(pack.read(2), Some((8.into(), 4)));
        assert_eq!(pack.read(4), None);
    }

    #[test]
    fn read_tiny_number_with_7th_bit_set() {
        let mut pack = Pack::default();
        pack.append(&"123");
        assert_eq!(pack.read(0), Some((123.into(), 2)));
        assert_eq!(pack.read(2), None);
    }

    #[test]
    fn write_13_bit_number() {
        let mut pack = Pack::default();
        pack.append(&"128");
        pack.append(&"-1");
        assert_eq!(*pack.data, b"\xc0\x80\x02\xdf\xff\x02"[..]);
        assert_eq!(pack.data.len(), 6);
        assert_eq!(pack.len(), 2);
    }

    #[test]
    fn read_13_bit_number() {
        let mut pack = Pack::default();
        pack.append(&"128");
        pack.append(&"-1");
        assert_eq!(pack.read(0), Some((128.into(), 3)));
        assert_eq!(pack.read(3), Some(((-1).into(), 6)));
        assert_eq!(pack.read(6), None);
    }

    #[test]
    fn write_tiny_string() {
        let mut pack = Pack::default();
        pack.append(&"abc");
        pack.append(&"de");
        assert_eq!(*pack.data, b"\x83abc\x04\x82de\x03"[..]);
        assert_eq!(pack.data.len(), 9);
        assert_eq!(pack.len(), 2);
    }

    #[test]
    fn read_tiny_string() {
        let mut pack = Pack::default();
        pack.append(&"abc");
        pack.append(&"de");
        let abc: Raw = "abc".into();
        let de: Raw = "de".into();
        assert_eq!(pack.read(0), Some((abc.slice(0..3).into(), 5)));
        assert_eq!(pack.read(5), Some((de.slice(0..2).into(), 9)));
        assert_eq!(pack.read(9), None);
    }

    #[test]
    fn write_medium_string_with_one_byte_back_len() {
        let value = &"x".repeat(64)[..];
        let mut pack = Pack::default();
        pack.append(&value);
        assert_eq!(pack.data.len(), 67);
        assert_eq!(&pack.data[0..2], &b"\xe0\x40"[..]);
        assert_eq!(&pack.data[2..66], value.as_bytes());
        assert_eq!(pack.data[66], 0x42);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn write_medium_string_with_two_byte_back_len() {
        let value = &"x".repeat(128)[..];
        let mut pack = Pack::default();
        pack.append(&value);
        assert_eq!(pack.data.len(), 132);
        assert_eq!(&pack.data[0..2], &b"\xe0\x80"[..]);
        assert_eq!(&pack.data[2..130], value.as_bytes());
        assert_eq!(&pack.data[130..132], &b"\x02\x81"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn write_medium_string_with_nine_bit_len() {
        let value: Raw = "x".repeat(256)[..].into();
        let mut pack = Pack::default();
        pack.append(&&value[..]);
        assert_eq!(pack.data.len(), 260);
        assert_eq!(&pack.data[0..2], &b"\xe1\x00"[..]);
        assert_eq!(&pack.data[2..258], &value[..]);
        assert_eq!(&pack.data[258..260], &b"\x02\x82"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_medium_string() {
        let value: Raw = "x".repeat(256)[..].into();
        let mut pack = Pack::default();
        pack.append(&&value[..]);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(value.slice(0..256).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_large_string() {
        let value: Raw = "x".repeat(4096)[..].into();
        let mut pack = Pack::default();
        pack.append(&&value[..]);
        assert_eq!(pack.data.len(), 4103);
        assert_eq!(&pack.data[0..5], &b"\xf0\x00\x10\x00\x00"[..]);
        assert_eq!(&pack.data[5..4101], &value[..]);
        assert_eq!(&pack.data[4101..4103], &b"\x05\xa0"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_large_string() {
        let value_one: Raw = "x".repeat(4096)[..].into();
        let value_two: Raw = "y".repeat(5000)[..].into();
        let mut pack = Pack::default();
        pack.append(&&value_one[..]);
        pack.append(&&value_two[..]);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(value_one.slice(0..4096).into()));
        assert_eq!(iterator.next(), Some(value_two.slice(0..5000).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_i16() {
        let mut pack = Pack::default();
        pack.append(&"32752");
        assert_eq!(pack.data.len(), 4);
        assert_eq!(&pack.data[..], &b"\xf1\xf0\x7f\x03"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_i16() {
        let mut pack = Pack::default();
        pack.append(&"32752");
        pack.append(&"-32752");
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(32752.into()));
        assert_eq!(iterator.next(), Some((-32752).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_i24() {
        let mut pack = Pack::default();
        pack.append(&"8388607");
        assert_eq!(pack.data.len(), 5);
        assert_eq!(&pack.data[..], &b"\xf2\xff\xff\x7f\x04"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_i24() {
        let mut pack = Pack::default();
        pack.append(&"8388607");
        pack.append(&"-8388608");
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(8_388_607.into()));
        assert_eq!(iterator.next(), Some((-8_388_608).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_i32() {
        let mut pack = Pack::default();
        pack.append(&"2147483647");
        assert_eq!(pack.data.len(), 6);
        assert_eq!(&pack.data[..], &b"\xf3\xff\xff\xff\x7f\x05"[..]);
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_i32() {
        let mut pack = Pack::default();
        pack.append(&"2147483647");
        pack.append(&"-2147483648");
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2_147_483_647.into()));
        assert_eq!(iterator.next(), Some((-2_147_483_648).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_i64() {
        let mut pack = Pack::default();
        pack.append(&0x8000_0000i64);
        assert_eq!(pack.data.len(), 10);
        assert_eq!(
            &pack.data[..],
            &b"\xf4\x00\x00\x00\x80\x00\x00\x00\x00\x09"[..]
        );
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_i64() {
        let mut pack = Pack::default();
        pack.append(&"2147483648");
        pack.append(&"-2147483649");
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2_147_483_648.into()));
        assert_eq!(iterator.next(), Some((-2_147_483_649).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn write_f64() {
        let mut pack = Pack::default();
        pack.append(&1f64);
        assert_eq!(pack.data.len(), 10);
        assert_eq!(
            &pack.data[..],
            &b"\xf5\x00\x00\x00\x00\x00\x00\xf0\x3f\x09"[..]
        );
        assert_eq!(pack.len(), 1);
    }

    #[test]
    fn read_f64() {
        let mut pack = Pack::default();
        pack.append(&1f64);
        pack.append(&-2.5f64);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1f64.into()));
        assert_eq!(iterator.next(), Some((-2.5f64).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_iterator() {
        let mut pack = Pack::default();
        pack.append(&"1");
        pack.append(&"-15");
        pack.append(&"12");
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some((-15).into()));
        assert_eq!(iterator.next(), Some(12.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_reverse_iterator() {
        let mut pack = Pack::default();
        pack.append(&"1");
        pack.append(&"-15");
        pack.append(&"12");
        let mut iterator = pack.iter().rev();
        assert_eq!(iterator.next(), Some(12.into()));
        assert_eq!(iterator.next(), Some((-15).into()));
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_u7() {
        let mut pack = Pack::default();
        pack.append(&2);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&3);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(3.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_i13() {
        let mut pack = Pack::default();
        pack.append(&-4096);
        pack.append(&4095);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&4094);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(4094.into()));
        assert_eq!(iterator.next(), Some(4095.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_i16() {
        let mut pack = Pack::default();
        pack.append(&-32_768);
        pack.append(&32_767);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&32_766);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(32_766.into()));
        assert_eq!(iterator.next(), Some(32_767.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_i24() {
        let mut pack = Pack::default();
        pack.append(&-8_388_608);
        pack.append(&8_388_607);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&8_388_606);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(8_388_606.into()));
        assert_eq!(iterator.next(), Some(8_388_607.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_i32() {
        let mut pack = Pack::default();
        pack.append(&-2_147_483_648);
        pack.append(&2_147_483_647);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&2_147_483_646);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2_147_483_646.into()));
        assert_eq!(iterator.next(), Some(2_147_483_647.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_i64() {
        let mut pack = Pack::default();
        pack.append(&i64::MIN);
        pack.append(&i64::MAX);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&(i64::MAX - 1));
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some((i64::MAX - 1).into()));
        assert_eq!(iterator.next(), Some(i64::MAX.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_f64() {
        let mut pack = Pack::default();
        pack.append(&12f64);
        pack.append(&15f64);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.replace(&14f64);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(14f64.into()));
        assert_eq!(iterator.next(), Some(15f64.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn cursor_remove() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.next();
        cursor.remove(2);
        assert_eq!(pack.len(), 3);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn cursor_remove_reverse() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Right);
        cursor.next();
        cursor.next();
        cursor.remove(2);
        assert_eq!(cursor.peek(), Some(1.into()));
        assert_eq!(pack.len(), 3);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some(4.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn cursor_remove_too_many() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.next();
        cursor.remove(20);
        assert_eq!(pack.len(), 2);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn cursor_remove_too_many_reverse() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Right);
        cursor.next();
        cursor.next();
        cursor.remove(20);
        assert_eq!(cursor.peek(), None);
        assert_eq!(pack.len(), 2);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(4.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_with_larger() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.replace(&2_147_483_646);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some(2_147_483_646.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_with_smaller() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2_147_483_646);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.replace(&2);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(5.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_u7_reverse() {
        let mut pack = Pack::default();
        pack.append(&2);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Right);
        cursor.replace(&3);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(3.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_with_larger_reverse() {
        let mut pack = Pack::default();
        pack.append(&2);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Right);
        cursor.replace(&i64::from(i8::MAX));
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(i64::from(i8::MAX).into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn replace_with_smaller_reverse() {
        let mut pack = Pack::default();
        pack.append(&2);
        pack.append(&i64::from(i8::MAX));
        let mut cursor = pack.cursor(Edge::Right);
        cursor.replace(&3);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(3.into()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn cursor_prev() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.next();
        assert_eq!(cursor.peek(), Some(3.into()));
        cursor.prev();
        assert_eq!(cursor.peek(), Some(2.into()));
    }

    #[test]
    fn cursor_skip() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);

        let mut cursor = pack.cursor(Edge::Left);
        cursor.skip(1);
        assert_eq!(cursor.peek(), Some(2.into()));
        cursor.skip(3);
        assert_eq!(cursor.peek(), Some(5.into()));

        let mut cursor = pack.cursor(Edge::Left);
        cursor.skip(4);
        assert_eq!(cursor.peek(), Some(5.into()));
    }

    #[test]
    fn debug() {
        let mut pack = Pack::default();
        pack.append(&5i64);
        pack.append(&3.2f64);
        pack.append(&"abcd".as_bytes());
        let s = format!("{pack:?}");
        assert_eq!(s, "[5, 3.2, \"abcd\"]");
    }

    #[test]
    fn double_ended_iterator() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next_back(), Some(4.into()));
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next_back(), Some(3.into()));
        assert_eq!(iterator.next(), None);
        assert_eq!(iterator.next_back(), None);
    }

    #[test]
    fn cursor_wrapping() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        let mut cursor = pack.cursor(Edge::Left);
        assert_eq!(cursor.peek(), Some(1.into()));
        cursor.prev();
        assert_eq!(cursor.peek(), None);
        cursor.next();
        assert_eq!(cursor.peek(), Some(1.into()));
    }

    #[test]
    fn cursor_reverse() {
        let mut pack = Pack::default();
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        let mut cursor = pack.cursor(Edge::Right);
        assert_eq!(cursor.peek(), Some(4.into()));
        assert_eq!(cursor.next(), Some(4.into()));
        assert_eq!(cursor.next(), Some(3.into()));
        assert_eq!(cursor.next(), Some(2.into()));
        assert_eq!(cursor.next(), Some(1.into()));
        assert_eq!(cursor.next(), None);
        assert_eq!(cursor.next(), Some(4.into()));
    }

    #[test]
    fn prev() {
        let mut pack = Pack::default();
        pack.append(&0);
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        pack.append(&5);
        let mut iter = pack.iter();
        assert_eq!(iter.prev(), None);
        assert_eq!(iter.prev_back(), None);
        assert_eq!(iter.next(), Some(0.into()));
        assert_eq!(iter.next(), Some(1.into()));
        assert_eq!(iter.next_back(), Some(5.into()));
        assert_eq!(iter.next_back(), Some(4.into()));
        assert_eq!(iter.prev(), Some(1.into()));
        assert_eq!(iter.prev_back(), Some(4.into()));
        assert_eq!(iter.prev(), Some(0.into()));
        assert_eq!(iter.prev_back(), Some(5.into()));
        assert_eq!(iter.prev(), None);
        assert_eq!(iter.prev_back(), None);
    }

    #[test]
    fn split() {
        let mut pack = Pack::default();
        pack.append(&0);
        pack.append(&1);
        pack.append(&2);
        pack.append(&3);
        pack.append(&4);
        let mut cursor = pack.cursor(Edge::Left);
        cursor.next();
        cursor.next();
        let tail = cursor.split();
        assert_eq!(tail.len(), 3);
        let mut iterator = tail.iter();
        assert_eq!(iterator.next(), Some(2.into()));
        assert_eq!(iterator.next(), Some(3.into()));
        assert_eq!(iterator.next(), Some(4.into()));
        assert_eq!(iterator.next(), None);
        let mut iterator = pack.iter();
        assert_eq!(iterator.next(), Some(0.into()));
        assert_eq!(iterator.next(), Some(1.into()));
        assert_eq!(iterator.next(), None);
        assert_eq!(pack.len(), 2);
    }

    #[test]
    fn mv_right_to_left() {
        let mut pack = Pack::default();
        pack.append(&"ab");
        pack.append(&"cde");
        pack.append(&"fghi");
        pack.mv(Edge::Right);
        let mut iterator = pack.iter();
        assert!((&"fghi").pack_eq(&iterator.next().unwrap()));
        assert!((&"ab").pack_eq(&iterator.next().unwrap()));
        assert!((&"cde").pack_eq(&iterator.next().unwrap()));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn mv_left_to_right() {
        let mut pack = Pack::default();
        pack.append(&"ab");
        pack.append(&"cde");
        pack.append(&"fghi");
        pack.mv(Edge::Left);
        let mut iterator = pack.iter();
        assert!((&"cde").pack_eq(&iterator.next().unwrap()));
        assert!((&"fghi").pack_eq(&iterator.next().unwrap()));
        assert!((&"ab").pack_eq(&iterator.next().unwrap()));
        assert_eq!(iterator.next(), None);
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod proptests {
    use super::*;
    use proptest::{collection::vec, prelude::*};

    proptest! {
        #[test]
        fn append_one(expected in vec(any::<u8>(), 1..250)) {
            let mut pack = Pack::default();
            pack.append(&&expected[..]);
            let actual = pack.iter().next().unwrap();
            prop_assert_eq!(expected, &actual.raw().0[..]);
        }

        #[test]
        fn append_many(expected in vec(vec(any::<u8>(), 1..250), 5..10)) {
            let mut pack = Pack::default();
            for item in &expected {
                pack.append(&&item[..]);
            }
            let actual: Vec<Vec<u8>> = pack.iter().map(|i| i.raw().0.to_vec()).collect();
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn insert(
            mut items in vec(vec(any::<u8>(), 1..250), 5..10),
            item in vec(any::<u8>(), 1..250),
            index in any::<prop::sample::Index>(),
        ) {
            let mut pack = Pack::default();
            let index = index.index(items.len() - 1);
            for item in &items {
                pack.append(&&item[..]);
            }
            let mut cursor = pack.cursor(Edge::Left);
            cursor.skip(index);
            cursor.insert(&&item[..]);
            items.insert(index, item);
            let actual: Vec<Vec<u8>> = pack.iter().map(|i| i.raw().0.to_vec()).collect();
            prop_assert_eq!(items, actual);
        }
    }
}
