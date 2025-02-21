use crate::{
    PackRef, PackValue,
    buffer::{ArrayBuffer, Buffer},
    bytes::parse_i64_exact,
};
use bytes::{BufMut, Bytes};

/// A trait for values that can be directly written to a [`Pack`][`crate::Pack`].
pub trait Packable {
    /// The size of the packed value, including the trailing length.
    fn pack_size(&self) -> usize;

    /// Write this packable value to a buffer.
    fn pack_write(&self, buffer: impl BufMut);

    /// Compare a packable value with a [`PackRef`] in an existing [`Pack`][`crate::Pack`].
    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool;
}

impl Packable for f64 {
    fn pack_size(&self) -> usize {
        10
    }

    fn pack_write(&self, mut buffer: impl BufMut) {
        buffer.put_u8(0xf5);
        buffer.put_f64_le(*self);
        buffer.put_u8(9);
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        let mut buffer = ArrayBuffer::default();

        use PackRef::*;
        match other {
            Float(other) => self == other,
            Integer(other) => {
                if self.fract() == 0f64 {
                    #[allow(clippy::cast_possible_truncation)]
                    let i = *self as i64;
                    i == *other
                } else {
                    false
                }
            }
            Slice(other) => buffer.write_f64(*self) == &other[..],
        }
    }
}

impl Packable for i64 {
    fn pack_size(&self) -> usize {
        match self {
            // u7
            0..=0x7f => 2,
            // i13
            -0x1000..=0xfff => 3,
            // i16
            -0x8000..=0x7fff => 4,
            // i24
            -0x0080_0000..=0x007f_ffff => 5,
            // i32
            -0x8000_0000..=0x7fff_ffff => 6,
            // i64
            _ => 10,
        }
    }

    fn pack_write(&self, mut buffer: impl BufMut) {
        match self {
            // u7
            0..=0x7f => {
                buffer.put_int_le(*self, 1);
                buffer.put_u8(1);
            }
            // i13
            -0x1000..=0xfff => {
                let bytes = self.to_be_bytes();
                buffer.put_u8(0xc0 | (0xdf & bytes[6]));
                buffer.put_u8(bytes[7]);
                buffer.put_u8(2);
            }
            // i16
            -0x8000..=0x7fff => {
                buffer.put_u8(0xf1);
                buffer.put_int_le(*self, 2);
                buffer.put_u8(3);
            }
            // i24
            -0x0080_0000..=0x007f_ffff => {
                buffer.put_u8(0xf2);
                buffer.put_int_le(*self, 3);
                buffer.put_u8(4);
            }
            // i32
            -0x8000_0000..=0x7fff_ffff => {
                buffer.put_u8(0xf3);
                buffer.put_int_le(*self, 4);
                buffer.put_u8(5);
            }
            // i64
            _ => {
                buffer.put_u8(0xf4);
                buffer.put_i64_le(*self);
                buffer.put_u8(9);
            }
        }
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        use PackRef::*;
        match other {
            Float(f) => {
                if f.fract() == 0f64 {
                    #[allow(clippy::cast_possible_truncation)]
                    let i = *f as i64;
                    i == *self
                } else {
                    false
                }
            }
            Integer(i) => self == i,
            Slice(s) => match parse_i64_exact(&s[..]) {
                Some(i) => *self == i,
                None => false,
            },
        }
    }
}

impl Packable for &[u8] {
    fn pack_size(&self) -> usize {
        if let Some(i) = parse_i64_exact(self) {
            return i.pack_size();
        }

        match self.len() {
            0..=0x3f => self.len() + 2,
            0x40..=0xfff => self.len() + 2 + back_len_size(self.len() + 2),
            0x1000..=0xffff_ffff => self.len() + 5 + back_len_size(self.len() + 5),
            _ => todo!("xl string"),
        }
    }

    fn pack_write(&self, mut buffer: impl BufMut) {
        if let Some(i) = parse_i64_exact(self) {
            return i.pack_write(buffer);
        }

        match self.len() {
            0..=0x3f => {
                buffer.put_u8(0x80 | u8::try_from(self.len()).unwrap());
                buffer.put_slice(self);
                write_back_len(self.len() + 1, buffer);
            }
            0x40..=0xfff => {
                let len = u16::try_from(self.len()).unwrap();
                buffer.put_u16(0xe000 | (0x0fff & len));
                buffer.put_slice(self);
                write_back_len(self.len() + 2, buffer);
            }
            0x1000..=0xffff_ffff => {
                buffer.put_u8(0xf0);
                buffer.put_u32_le(u32::try_from(self.len()).unwrap());
                buffer.put_slice(self);
                write_back_len(self.len() + 5, buffer);
            }
            _ => todo!("xl string"),
        }
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        let mut buffer = ArrayBuffer::default();

        use PackRef::*;
        match other {
            Float(f) => &self[..] == buffer.write_f64(*f),
            Integer(i) => match parse_i64_exact(&self[..]) {
                Some(parsed) => *i == parsed,
                None => false,
            },
            Slice(s) => self[..] == s[..],
        }
    }
}

impl Packable for Bytes {
    fn pack_size(&self) -> usize {
        (&self[..]).pack_size()
    }

    fn pack_write(&self, buffer: impl BufMut) {
        (&self[..]).pack_write(buffer);
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        (&self[..]).pack_eq(other)
    }
}

impl Packable for &str {
    fn pack_size(&self) -> usize {
        self.as_bytes().pack_size()
    }

    fn pack_write(&self, buffer: impl BufMut) {
        self.as_bytes().pack_write(buffer);
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        self.as_bytes().pack_eq(other)
    }
}

impl Packable for PackRef<'_> {
    fn pack_size(&self) -> usize {
        use PackRef::*;
        match self {
            Float(f) => f.pack_size(),
            Integer(i) => i.pack_size(),
            Slice(s) => (&s[..]).pack_size(),
        }
    }

    fn pack_write(&self, buffer: impl BufMut) {
        use PackRef::*;
        match self {
            Float(f) => f.pack_write(buffer),
            Integer(i) => i.pack_write(buffer),
            Slice(s) => (&s[..]).pack_write(buffer),
        }
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        use PackRef::*;
        match self {
            Float(f) => f.pack_eq(other),
            Integer(i) => i.pack_eq(other),
            Slice(s) => (&s[..]).pack_eq(other),
        }
    }
}

impl Packable for PackValue {
    fn pack_size(&self) -> usize {
        use PackValue::*;
        match self {
            Float(f) => f.pack_size(),
            Integer(i) => i.pack_size(),
            Raw(s) => (&s[..]).pack_size(),
        }
    }

    fn pack_write(&self, buffer: impl BufMut) {
        use PackValue::*;
        match self {
            Float(f) => f.pack_write(buffer),
            Integer(i) => i.pack_write(buffer),
            Raw(s) => (&s[..]).pack_write(buffer),
        }
    }

    fn pack_eq<'a>(&'a self, other: &PackRef<'a>) -> bool {
        use PackValue::*;
        match self {
            Float(f) => f.pack_eq(other),
            Integer(i) => i.pack_eq(other),
            Raw(s) => (&s[..]).pack_eq(other),
        }
    }
}

fn back_len_size(mut len: usize) -> usize {
    let mut size = 0;
    while len > 0 {
        size += 1;
        len >>= 7;
    }
    size
}

fn write_back_len(mut len: usize, mut buffer: impl BufMut) {
    buffer.put_u8(u8::try_from(0x7f & len).unwrap());
    len >>= 7;
    while len > 0 {
        buffer.put_u8(0x80 | u8::try_from(0x7f & len).unwrap());
        len >>= 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pack;

    macro_rules! assert_back_len {
        ($len:expr, $expected:expr) => {{
            let mut buffer: Vec<u8> = Vec::new();
            write_back_len($len, &mut buffer);
            assert_eq!(&buffer[..], &$expected[..]);
        }};
    }

    #[test]
    fn test_back_len_size() {
        assert_eq!(back_len_size(0x01), 1);
        assert_eq!(back_len_size(0x7f), 1);

        assert_eq!(back_len_size(0x80), 2);
        assert_eq!(back_len_size(0x81), 2);
        assert_eq!(back_len_size(0x3fff), 2);

        assert_eq!(back_len_size(0x4000), 3);
        assert_eq!(back_len_size(0x4001), 3);
        assert_eq!(back_len_size(0x1fffff), 3);

        assert_eq!(back_len_size(0x200000), 4);
        assert_eq!(back_len_size(0x200001), 4);
        assert_eq!(back_len_size(0xfffffff), 4);

        assert_eq!(back_len_size(0x10000000), 5);
        assert_eq!(back_len_size(0x10000001), 5);
    }

    #[test]
    fn test_write_back_len() {
        // One byte
        assert_back_len!(0x01, b"\x01");
        assert_back_len!(0x02, b"\x02");
        assert_back_len!(0x7f, b"\x7f");

        // Two bytes
        assert_back_len!(0x80, b"\x00\x81");
        assert_back_len!(0x81, b"\x01\x81");
        assert_back_len!(0x3fff, b"\x7f\xff");

        // Three bytes
        assert_back_len!(0x4000, b"\x00\x80\x81");
        assert_back_len!(0x4001, b"\x01\x80\x81");
        assert_back_len!(0x1fffff, b"\x7f\xff\xff");

        // Four bytes
        assert_back_len!(0x200000, b"\x00\x80\x80\x81");
        assert_back_len!(0x200001, b"\x01\x80\x80\x81");
        assert_back_len!(0xfffffff, b"\x7f\xff\xff\xff");

        // Five bytes
        assert_back_len!(0x10000000, b"\x00\x80\x80\x80\x81");
        assert_back_len!(0x10000001, b"\x01\x80\x80\x80\x81");
    }

    #[test]
    fn test_pack_eq() {
        let mut pack = Pack::default();
        pack.append(&1234);
        pack.append(&12.34f64);
        pack.append(&"12.34");
        pack.append(&12f64);
        pack.append(&"12");
        let mut iter = pack.iter();

        let i = iter.next().unwrap();
        assert!("1234".pack_eq(&i));
        assert!(1234f64.pack_eq(&i));
        assert!(!1234.2f64.pack_eq(&i));

        let f = iter.next().unwrap();
        assert!("12.34".pack_eq(&f));

        let s = iter.next().unwrap();
        assert!(12.34f64.pack_eq(&s));

        let f = iter.next().unwrap();
        assert!(12i64.pack_eq(&f));
        assert!(!13i64.pack_eq(&f));

        let s = iter.next().unwrap();
        assert!(12i64.pack_eq(&s));
        assert!(!13i64.pack_eq(&s));
    }
}
