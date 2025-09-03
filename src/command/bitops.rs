use crate::{
    Client, CommandResult, Reply, ReplyError, Store,
    buffer::ArrayBuffer,
    bytes::{lex, parse},
    command::{Arity, Command, CommandKind, Keys},
    slice::slice,
};
use logos::Logos;
use std::{
    cmp::{max, min},
    mem::size_of,
    ops::Range,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum Unit {
    #[regex(b"(?i:bit)")]
    Bit,

    #[regex(b"(?i:byte)")]
    Byte,
}

fn increment_field(field: Field, value: i64, by: i64, overflow: Overflow) -> Option<i64> {
    let Field { signed, bits, .. } = field;

    // First, check if the i64 add overflows.
    let (result, mut wrapped) = value.overflowing_add(by);

    // Now check for overflow in smaller values.
    wrapped |= if signed {
        let mask = !0 << (bits - 1);

        // Using two's complement, positive values should be all zeros on the left and negative
        // values should be all ones.
        if result >= 0 {
            result & mask != 0
        } else {
            !result & mask != 0
        }
    } else {
        let mask = !0 << bits;

        // A negative value is an underflow, and any ones past the highest bit is an overflow.
        result < 0 || mask & result != 0
    };

    if !wrapped {
        return Some(result);
    }

    use Overflow::*;

    match overflow {
        Fail => None,
        // Prevent panic from shift left with overflow.
        Wrap if bits >= 64 => Some(result),
        Wrap => Some(result & !(!0 << bits)),
        Sat => Some(match (signed, result < 0) {
            (true, true) => !0 << (bits - 1),
            (true, false) => !(!0 << (bits - 1)),
            (false, true) => 0,
            (false, false) => !(!0 << bits),
        }),
    }
}

fn get_field(mut value: &[u8], field: Field) -> i64 {
    let Field {
        signed,
        bits,
        offset,
    } = field;

    // Move up to the offset if the value is long enough.
    if value.len() > offset / 8 {
        value = &value[offset / 8..];
    }

    let mut buffer = [0u8; 16];
    let len = min(value.len(), buffer.len());
    buffer[..len].copy_from_slice(&value[..len]);

    if signed {
        let result = i128::from_be_bytes(buffer) << (offset % 8);
        i64::try_from(result >> (128 - bits)).unwrap()
    } else {
        let result = u128::from_be_bytes(buffer) << (offset % 8);
        i64::try_from(result >> (128 - bits)).unwrap()
    }
}

fn set_field(value: &mut [u8], field: Field, n: i64) {
    let Field { bits, offset, .. } = field;

    // Slice just the required bytes, including leading and trailing bits.
    let value = {
        let end = (offset + bits - 1) / 8 + 1;
        &mut value[offset / 8..end]
    };

    // The inner value holds the bits to be set in their correct positions.
    #[allow(clippy::cast_sign_loss)]
    let inner = (n as u128) << (128 - bits - offset % 8);

    // The outer value is created from the existing bytes.
    let outer = {
        let mut bytes = [0u8; 16];
        bytes[0..value.len()].copy_from_slice(value);
        u128::from_be_bytes(bytes)
    };

    // The mask holds set bits where the new value should be.
    //
    // BITFIELD SET i5 #1 11
    //
    // Ones    11111111 11111111 11111111 …
    // <<      11111000 00000000 00000000 …
    // >>      00000111 10000000 00000000 …
    //
    let mask = (!0u128 << (128 - bits)) >> (offset % 8);

    // The result is created by masking the inner and outer values.
    //
    // BITFIELD SET i5 #1 11
    //
    // Mask    00000111 11000000 00000000 …
    // Inner   00000010 11000000 00000000 …
    // Outer   xxxxxxxx xxxxxxxx xxxxxxxx …
    // Result  xxxxx010 11xxxxxx xxxxxxxx …
    //
    let result = (outer & !mask | inner & mask).to_be_bytes();
    value.copy_from_slice(&result[0..value.len()]);
}

pub static BITCOUNT: Command = Command {
    kind: CommandKind::Bitcount,
    name: "bitcount",
    arity: Arity::Minimum(2),
    run: bitcount,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

trait CountBits {
    fn count_bits(&self) -> i64;
}

macro_rules! impl_count_bits {
    ($T:ty) => {
        impl CountBits for $T {
            fn count_bits(&self) -> i64 {
                self.count_ones().into()
            }
        }
    };
}

impl_count_bits!(u8);
impl_count_bits!(u128);

fn bitcount(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let mut buffer = ArrayBuffer::default();
    let mut value = db.get_string(&key)?.ok_or(0)?.as_bytes(&mut buffer);

    let (start, end) = match client.request.remaining() {
        0 => (0, -1),
        2 => {
            let start = client.request.i64()?;
            let end = client.request.i64()?;
            (8 * start, 7 + 8 * end)
        }
        3 => {
            let start = client.request.i64()?;
            let end = client.request.i64()?;
            match lex(&client.request.pop()?) {
                Some(Unit::Bit) => (start, end),
                Some(Unit::Byte) => (8 * start, 7 + 8 * end),
                None => return Err(ReplyError::Syntax.into()),
            }
        }
        _ => return Err(ReplyError::Syntax.into()),
    };

    let range = slice(8 * value.len(), start, end).ok_or(0)?;

    // Count the ones in the first n % 8 bits of slice[n / 8].
    fn count_first(slice: &[u8], n: usize) -> i64 {
        if n % 8 == 0 {
            return 0;
        }
        i64::from((!(!0 >> (n % 8)) & slice[n / 8]).count_ones())
    }

    // Count the ones in a slice of values.
    fn count_bits(slice: &[impl CountBits]) -> i64 {
        slice.iter().map(|x| x.count_bits()).sum()
    }

    // Convert from bits to bytes. This potentially includes leading bits in the first byte and
    // excludes trailing bits in the last byte so we adjust for those individually.
    //
    // BITCOUNT X 13 30 BIT
    //
    // bits ─────────┬──────────────────╮
    // bytes ───┬───────────────╮       │
    //          ┴    ┴          ┴       ┴
    // 00000000 00110000 00011000 01010000
    //          ───┬─             ─┬─────
    // subtract ───╯               │
    // add ────────────────────────╯

    let mut result: i64 = 0;

    // Subtract included bits from the first byte.
    result -= count_first(value, range.start);

    // Add excluded bits from the last byte.
    result += count_first(value, range.end);

    // Slice out excluded portions of the value. The last byte has already been counted above, so
    // we skip it here.
    value = &value[range.start / 8..range.end / 8];

    // SAFETY: There are no invalid bit patterns for u128 and we only use them for counting bits.
    let (prefix, middle, suffix) = unsafe { value.align_to::<u128>() };

    result += count_bits(prefix);
    result += count_bits(middle);
    result += count_bits(suffix);

    client.reply(result);
    Ok(None)
}

pub static BITFIELD: Command = Command {
    kind: CommandKind::Bitfield,
    name: "bitfield",
    arity: Arity::Minimum(2),
    run: bitfield,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

pub static BITFIELD_RO: Command = Command {
    kind: CommandKind::Bitfieldro,
    name: "bitfieldro",
    arity: Arity::Minimum(2),
    run: bitfield,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

#[derive(Clone, Copy, Debug, Eq, Logos, PartialEq)]
pub enum BitfieldOpType {
    #[regex(b"(?i:get)")]
    Get,

    #[regex(b"(?i:set)")]
    Set,

    #[regex(b"(?i:incrby)")]
    Incrby,

    #[regex(b"(?i:overflow)")]
    Overflow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Field {
    pub signed: bool,
    pub bits: usize,
    pub offset: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum BitfieldOp {
    Get(Field),
    Set(Field, i64),
    Incrby(Field, i64),
    Overflow(Overflow),
}

fn field(client: &mut Client) -> Result<Field, ReplyError> {
    let arg = client.request.pop()?;
    let signed = match arg.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(ReplyError::InvalidBitfield),
    };

    let bits = match parse(&arg[1..]) {
        Some(bits) if signed && bits <= 64 && bits > 0 => bits,
        Some(bits) if !signed && bits <= 63 && bits > 0 => bits,
        _ => return Err(ReplyError::InvalidBitfield),
    };

    let offset = client.request.pop()?;
    let offset = match offset.first() {
        Some(b'#') => parse::<usize>(&offset[1..]).map(|n| n * bits),
        _ => parse(&offset[..]),
    }
    .ok_or(ReplyError::BitOffset)?;

    Ok(Field {
        signed,
        bits,
        offset,
    })
}

#[derive(Clone, Copy, Debug, Eq, Logos, PartialEq)]
pub enum OverflowType {
    #[regex(b"(?i:wrap)")]
    Wrap,

    #[regex(b"(?i:sat)")]
    Sat,

    #[regex(b"(?i:fail)")]
    Fail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Overflow {
    Wrap,
    Sat,
    Fail,
}

fn overflow(client: &mut Client) -> Result<Overflow, ReplyError> {
    use OverflowType::*;
    match lex(&client.request.pop()?[..]) {
        Some(Wrap) => Ok(Overflow::Wrap),
        Some(Sat) => Ok(Overflow::Sat),
        Some(Fail) => Ok(Overflow::Fail),
        None => Err(ReplyError::InvalidOverflow),
    }
}

fn bitfield_op(client: &mut Client, readonly: bool) -> Result<BitfieldOp, ReplyError> {
    let argument = client.request.pop()?;
    let Some(op) = lex(&argument[..]) else {
        return Err(ReplyError::Syntax);
    };

    use BitfieldOpType::*;
    match op {
        Incrby | Overflow | Set if readonly => Err(ReplyError::Bitfieldro),
        Get => {
            let field = field(client)?;
            Ok(BitfieldOp::Get(field))
        }
        Set => {
            let field = field(client)?;
            let value = client.request.i64()?;
            Ok(BitfieldOp::Set(field, value))
        }
        Incrby => {
            let field = field(client)?;
            let value = client.request.i64()?;
            Ok(BitfieldOp::Incrby(field, value))
        }
        Overflow => {
            let overflow = overflow(client)?;
            Ok(BitfieldOp::Overflow(overflow))
        }
    }
}

fn bitfield(client: &mut Client, store: &mut Store) -> CommandResult {
    client.request.reset(2);
    let mut count = 0;
    let mut last_write = None;
    let readonly = client.request.command.readonly;

    // Count the operations, check for writes
    while !client.request.is_empty() {
        use BitfieldOp::*;
        match bitfield_op(client, readonly)? {
            Get(_) => {
                count += 1;
            }
            Incrby(field, _) | Set(field, _) => {
                count += 1;
                let byte = (field.offset + field.bits - 1) / 8 + 1;
                let max = max(byte, last_write.unwrap_or(0));
                last_write.replace(max);
            }
            Overflow(_) => {}
        }
    }

    client.request.reset(1);
    client.reply(Reply::Array(count));

    if let Some(byte) = last_write {
        bitfield_write(client, store, byte)
    } else {
        bitfield_read(client, store)
    }
}

fn bitfield_read(client: &mut Client, store: &mut Store) -> CommandResult {
    let readonly = client.request.command.readonly;
    let key = client.request.pop()?;
    let db = store.get_db(client.db())?;
    let mut buffer = ArrayBuffer::default();
    let value = db
        .get_string(&key)?
        .map_or(&[][..], |value| value.as_bytes(&mut buffer));

    while !client.request.is_empty() {
        use BitfieldOp::*;
        if let Get(field) = bitfield_op(client, readonly)? {
            client.reply(get_field(value, field));
        }
    }

    Ok(None)
}

fn bitfield_write(client: &mut Client, store: &mut Store, last_write: usize) -> CommandResult {
    let mut created = false;
    let key = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let value = db
        .entry_ref(&key)
        .or_insert_with(|| {
            created = true;
            Vec::with_capacity(last_write).into()
        })
        .mut_string()?
        .raw()
        .make_mut();

    if value.len() < last_write {
        value.resize(last_write, 0);
    }

    let mut changes = 0;
    let mut overflow = Overflow::Wrap;
    while !client.request.is_empty() {
        use BitfieldOp::*;

        match bitfield_op(client, false)? {
            Get(field) => {
                client.reply(get_field(value, field));
            }
            Set(field, n) => {
                let original = get_field(value, field);
                if let Some(result) = increment_field(field, n, 0, overflow) {
                    set_field(value, field, result);
                    if created || original != result {
                        changes += 1;
                    }
                    client.reply(original);
                } else {
                    client.reply(Reply::Nil);
                }
            }
            Incrby(field, by) => {
                let n = get_field(value, field);
                if let Some(result) = increment_field(field, n, by, overflow) {
                    set_field(value, field, result);
                    if created || n != result {
                        changes += 1;
                    }
                    client.reply(result);
                } else {
                    client.reply(Reply::Nil);
                }
            }
            Overflow(value) => {
                overflow = value;
            }
        }
    }

    if changes > 0 {
        store.dirty += changes;
        store.touch(client.db(), &key);
    }

    Ok(None)
}

pub static BITOP: Command = Command {
    kind: CommandKind::Bitop,
    name: "bitop",
    arity: Arity::Minimum(4),
    run: bitop,
    keys: Keys::SkipOne,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Bitop {
    And,
    Or,
    Xor,
}

#[derive(Debug, Eq, PartialEq, Logos)]
pub enum BitopType {
    #[regex(b"(?i:and)")]
    And,

    #[regex(b"(?i:or)")]
    Or,

    #[regex(b"(?i:xor)")]
    Xor,

    #[regex(b"(?i:not)")]
    Not,
}

// TODO: Try out packed_simd crate here
fn bitop(client: &mut Client, store: &mut Store) -> CommandResult {
    let op = {
        let op = client.request.pop()?;
        let Some(op) = lex(&op) else {
            return Err(ReplyError::Syntax.into());
        };

        use BitopType::*;
        match op {
            And => Bitop::And,
            Or => Bitop::Or,
            Xor => Bitop::Xor,
            Not => return bitop_not(client, store),
        }
    };

    let destination = client.request.pop()?;
    let db = store.mut_db(client.db())?;
    let mut max_len = 0;
    let mut buffer = ArrayBuffer::default();

    for key in client.request.iter() {
        if let Some(value) = db.get(&key) {
            let len = value.as_string()?.as_bytes(&mut buffer).len();
            max_len = max(len, max_len);
        }
    }

    // Are all the keys empty?
    if max_len == 0 {
        if db.remove(&destination).is_some() {
            store.dirty += 1;
            store.touch(client.db(), &destination);
        }
        client.reply(0);
        return Ok(None);
    }

    use Bitop::*;

    let init = match op {
        And => 0xff,
        Or => 0,
        Xor => 0,
    };

    let op = match op {
        And => |a: u8, b: u8| a & b,
        Or => |a: u8, b: u8| a | b,
        Xor => |a: u8, b: u8| a ^ b,
    };

    let mut result = vec![init; max_len];
    client.request.reset(3);

    for key in client.request.iter() {
        let bytes = match db.get(&key) {
            Some(value) => value.as_string()?.as_bytes(&mut buffer),
            None => &[],
        };
        for (index, value) in result.iter_mut().enumerate() {
            *value = op(*bytes.get(index).unwrap_or(&0), *value);
        }
    }

    db.set(&destination, result);
    store.dirty += 1;
    store.touch(client.db(), &destination);
    client.reply(max_len);
    Ok(None)
}

fn bitop_not(client: &mut Client, store: &mut Store) -> CommandResult {
    let destination = client.request.pop()?;
    let source = client.request.pop()?;

    if !client.request.is_empty() {
        return Err(ReplyError::BitopNot.into());
    }

    let db = store.mut_db(client.db())?;
    let mut buffer = ArrayBuffer::default();
    let value = db
        .get_string(&source)?
        .map_or(&[][..], |value| value.as_bytes(&mut buffer));
    let len = value.len();

    if value.is_empty() {
        if db.remove(&destination).is_some() {
            store.dirty += 1;
            store.touch(client.db(), &destination);
        }
        client.reply(0);
    } else {
        let mut result: Vec<u8> = Vec::from(value);

        // SAFETY: There are no invalid bit patterns for u128 and we only use them to negate bits.
        let (prefix, middle, suffix) = unsafe { result.align_to_mut::<u128>() };

        for x in prefix {
            *x = !*x;
        }
        for x in middle {
            *x = !*x;
        }
        for x in suffix {
            *x = !*x;
        }

        db.set(&destination, result);
        store.dirty += 1;
        store.touch(client.db(), &destination);
        client.reply(len);
    }
    Ok(None)
}

pub static BITPOS: Command = Command {
    kind: CommandKind::Bitpos,
    name: "bitpos",
    arity: Arity::Minimum(3),
    run: bitpos,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

trait BitIndex: std::fmt::Debug {
    const SIZE: usize;
    fn bit_index(&self, bit: bool) -> Option<usize>;
}

macro_rules! impl_bit_index {
    ($T:ty) => {
        impl BitIndex for $T {
            const SIZE: usize = size_of::<$T>();

            fn bit_index(&self, bit: bool) -> Option<usize> {
                let empty = if bit { 0 } else { !0 };

                if *self == empty {
                    return None;
                }

                if bit {
                    Some(self.to_be().leading_zeros() as usize)
                } else {
                    Some(self.to_be().leading_ones() as usize)
                }
            }
        }
    };
}

impl_bit_index!(u8);
impl_bit_index!(u128);

fn bitpos(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let bit = client.request.bit()?;
    let end_given = client.request.len() > 4;
    let (start, end) = match client.request.remaining() {
        0 => (0, -1),
        1 => {
            let start = client.request.i64()?;
            (8 * start, -1)
        }
        2 => {
            let start = client.request.i64()?;
            let end = client.request.i64()?;
            (8 * start, 7 + 8 * end)
        }
        3 => {
            let start = client.request.i64()?;
            let end = client.request.i64()?;
            let unit = client.request.pop()?;
            let Some(unit) = lex(&unit) else {
                return Err(ReplyError::Syntax.into());
            };

            match unit {
                Unit::Bit => (start, end),
                Unit::Byte => (8 * start, 7 + 8 * end),
            }
        }
        _ => return Err(ReplyError::Syntax.into()),
    };

    let db = store.get_db(client.db())?;
    let mut buffer = ArrayBuffer::default();
    let value = db
        .get_string(&key)?
        .ok_or(if bit { -1 } else { 0 })?
        .as_bytes(&mut buffer);

    fn search<T: BitIndex>(
        slice: &[T],
        bit: bool,
        range: &Range<usize>,
        position: &mut usize,
    ) -> Option<usize> {
        for (index, value) in slice.iter().enumerate() {
            if let Some(bits) = value.bit_index(bit) {
                let result = *position + 8 * T::SIZE * index + bits;
                // If the bit is out of range (in trailing bits), don't return it.
                if range.contains(&result) {
                    return Some(result);
                }
            }
        }
        *position += 8 * T::SIZE * slice.len();
        None
    }

    let range = slice(8 * value.len(), start, end).ok_or(-1)?;
    let first = value[range.start / 8];
    let rest = &value[range.start / 8 + 1..=(range.end - 1) / 8];

    // Mask the first byte if necessary.
    let first = if range.start % 8 == 0 {
        first
    } else if bit {
        first & (!0 >> (range.start % 8))
    } else {
        first | !(!0 >> (range.start % 8))
    };

    // SAFETY: There are no invalid bit patterns for u128 and we only use them for bit position.
    let (prefix, middle, suffix) = unsafe { rest.align_to::<u128>() };

    let mut position = range.start - range.start % 8;
    let result = search(&[first], bit, &range, &mut position)
        .or_else(|| search(prefix, bit, &range, &mut position))
        .or_else(|| search(middle, bit, &range, &mut position))
        .or_else(|| search(suffix, bit, &range, &mut position));

    if let Some(result) = result {
        client.reply(result);
    } else if end_given || bit {
        client.reply(-1);
    } else {
        client.reply(8 * value.len());
    }

    Ok(None)
}

pub static GETBIT: Command = Command {
    kind: CommandKind::Getbit,
    name: "getbit",
    arity: Arity::Exact(3),
    run: getbit,
    keys: Keys::Single,
    readonly: true,
    admin: false,
    noscript: false,
    pubsub: false,
    write: false,
};

fn getbit(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let offset = client.request.bit_offset()?;
    let db = store.get_db(client.db())?;
    let mut buffer = ArrayBuffer::default();
    let value = db.get_string(&key[..])?.ok_or(0)?.as_bytes(&mut buffer);

    let bytes = offset / 8;
    let bits = offset % 8;
    let mask = 0x80 >> bits;
    let byte = value.get(bytes).unwrap_or(&0);
    let result = byte & mask != 0;

    client.reply(i64::from(result));
    Ok(None)
}

pub static SETBIT: Command = Command {
    kind: CommandKind::Setbit,
    name: "setbit",
    arity: Arity::Exact(4),
    run: setbit,
    keys: Keys::Single,
    readonly: false,
    admin: false,
    noscript: false,
    pubsub: false,
    write: true,
};

fn setbit(client: &mut Client, store: &mut Store) -> CommandResult {
    let key = client.request.pop()?;
    let offset = client.request.bit_offset()?;
    let bit = client.request.bit()?;

    let bytes = offset / 8;
    let bits = offset % 8;
    let mask = 0x80 >> bits;

    let mut created = false;
    let db = store.mut_db(client.db())?;
    let value = db
        .entry_ref(&key)
        .or_insert_with(|| {
            created = true;
            Vec::with_capacity(bytes).into()
        })
        .mut_string()?
        .raw()
        .make_mut();

    if value.len() <= bytes {
        value.resize(bytes + 1, 0);
    }

    let original = value[bytes] & mask != 0;

    if bit {
        value[bytes] |= mask;
    } else {
        value[bytes] &= !mask;
    }

    if created || bit != original {
        store.dirty += 1;
        store.touch(client.db(), &key);
    }

    client.reply(i64::from(original));
    Ok(None)
}
