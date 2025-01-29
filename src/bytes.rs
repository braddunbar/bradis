use logos::Logos;

/// Lex a Logos token from a byte slice.
pub fn lex<'a, T>(bytes: &'a [u8]) -> Option<T>
where
    T: Logos<'a, Source = [u8]>,
    <T as Logos<'a>>::Extras: Default,
{
    let mut lexer = T::lexer(bytes);
    let token = lexer.next()?.ok()?;

    // Make sure there is only one token
    if lexer.remainder().is_empty() {
        Some(token)
    } else {
        None
    }
}

/// Parse a byte slice into an arbitrary type via utf8.
pub fn parse<T: std::str::FromStr>(bytes: &[u8]) -> Option<T> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Return the length of an i64 in base 10 bytes.
pub fn i64_len(n: i64) -> usize {
    let ilog10 = n.unsigned_abs().checked_ilog10().unwrap_or(0);
    usize::try_from(ilog10).unwrap() + if n.is_negative() { 2 } else { 1 }
}

/// Parse an i64 if the string representation can be exactly reproduced. This means no leading or
/// trailing space and no leading zeros.
pub fn parse_i64_exact(item: &[u8]) -> Option<i64> {
    let mut n: i64 = 0;
    let mut negative = false;
    let mut rest = match item {
        [b'0'] => return Some(0),
        [b'1'..=b'9', ..] => item,
        [b'-', b'1'..=b'9', ..] => {
            negative = true;
            &item[1..]
        }
        _ => return None,
    };

    loop {
        rest = match rest {
            [] => return Some(n),
            [b @ b'0'..=b'9', rest @ ..] => {
                let value = (*b - b'0').into();
                n = n.checked_mul(10)?;
                n = if negative {
                    n.checked_sub(value)?
                } else {
                    n.checked_add(value)?
                };
                rest
            }
            _ => return None,
        }
    }
}

/// An output wrapper for an arbitrary byte sequence. Printable ASCII characters are output
/// directly and all others are escaped.
pub struct Output<'a>(pub &'a [u8]);

impl std::fmt::Debug for Output<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for Output<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(self.0) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, "{}", self.0.escape_ascii()),
        }
    }
}

/// An output wrapper to print uppercase ascii characters.
pub struct AsciiUpper<'a>(pub &'a str);

impl std::fmt::Display for AsciiUpper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for c in self.0.chars() {
            write!(f, "{}", c.to_ascii_uppercase())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_i64_exact_success() {
        assert_eq!(parse_i64_exact(b"0"), Some(0));
        assert_eq!(parse_i64_exact(b"1"), Some(1));
        assert_eq!(parse_i64_exact(b"-1"), Some(-1));
        assert_eq!(parse_i64_exact(b"-9"), Some(-9));
        assert_eq!(parse_i64_exact(b"-901"), Some(-901));
        assert_eq!(parse_i64_exact(b"1000"), Some(1000));
        let mut buf = Vec::new();
        write!(buf, "{}", i64::MAX).unwrap();
        assert_eq!(parse_i64_exact(&buf), Some(i64::MAX));
        let mut buf = Vec::new();
        write!(buf, "{}", i64::MIN).unwrap();
        assert_eq!(parse_i64_exact(&buf), Some(i64::MIN));
    }

    #[test]
    fn parse_i64_exact_failure() {
        assert_eq!(parse_i64_exact(b"00"), None);
        assert_eq!(parse_i64_exact(b"01"), None);
        assert_eq!(parse_i64_exact(b"-"), None);
        assert_eq!(parse_i64_exact(b"-0"), None);
        assert_eq!(parse_i64_exact(b"-01"), None);
        assert_eq!(parse_i64_exact(b" "), None);
        assert_eq!(parse_i64_exact(b"0 "), None);
        assert_eq!(parse_i64_exact(b" 0 "), None);
        assert_eq!(parse_i64_exact(b"100 "), None);
        assert_eq!(parse_i64_exact(b"-100 "), None);
        assert_eq!(parse_i64_exact(b"214 321"), None);
        let mut buf = Vec::new();
        write!(buf, "{}", i128::from(i64::MAX) + 1).unwrap();
        assert_eq!(parse_i64_exact(&buf), None);
        let mut buf = Vec::new();
        write!(buf, "{}", i128::from(i64::MIN) - 1).unwrap();
        assert_eq!(parse_i64_exact(&buf), None);
    }

    #[test]
    fn lex_exact_bytes() {
        #[derive(Logos)]
        pub enum Test {
            #[regex(b"(?i:test)")]
            Test,
        }

        assert!(matches!(lex(b"test"), Some(Test::Test)));
        assert!(lex::<Test>(b"x").is_none());
        assert!(lex::<Test>(b"test  ").is_none());
    }

    #[test]
    fn length() {
        assert_eq!(1, i64_len(0));
        assert_eq!(2, i64_len(12));
        assert_eq!(3, i64_len(-23));
        assert_eq!(4, i64_len(-234));
        assert_eq!(9, i64_len(-23456789));
        assert_eq!(10, i64_len(1234567890));
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod proptests {
    use super::*;
    use proptest::{collection::vec, prelude::*};
    use std::io::Write;

    proptest! {
        #[test]
        fn parse_i64_exact_doesnt_crash(x in vec(any::<u8>(), 0..30)) {
            if let Some(i) = parse_i64_exact(&x[..]) {
                let mut v = Vec::new();
                write!(v, "{i}").unwrap();
                prop_assert_eq!(x, v);
            }
        }

        #[test]
        fn parse_i64_unicode(s in "\\PC*") {
            if let Some(i) = parse_i64_exact(s.as_bytes()) {
                let mut v = Vec::new();
                write!(v, "{i}").unwrap();
                prop_assert_eq!(s.as_bytes(), v);
            }
        }

        #[test]
        fn parse_i64_numbers(x in any::<i64>()) {
            let mut v = Vec::new();
            write!(v, "{x}").unwrap();
            let i = parse_i64_exact(&v[..]).unwrap();
            prop_assert_eq!(i, x);
        }

        #[test]
        fn i64_len_is_correct(x in any::<i64>()) {
            let mut v = Vec::new();
            write!(v, "{x}").unwrap();
            let len = i64_len(x);
            prop_assert_eq!(len, v.len());
        }
    }
}
