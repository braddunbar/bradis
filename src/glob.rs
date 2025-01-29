fn trim_start(slice: &[u8], value: u8) -> &[u8] {
    match slice.iter().position(|&byte| byte != value) {
        Some(index) => &slice[index..],
        None => &[],
    }
}

fn glob<'a, F>(string: &'a [u8], pattern: &'a [u8], case: F) -> bool
where
    F: Fn(u8) -> u8,
{
    let brackets = |pattern: &'a [u8], byte: u8| {
        let mut matched = false;

        let (mut pattern, not) = match pattern {
            [b'^', rest @ ..] => (rest, true),
            _ => (pattern, false),
        };

        loop {
            pattern = match pattern {
                [b']', rest @ ..] => {
                    pattern = rest;
                    break;
                }
                [b'\\', c, rest @ ..] => {
                    matched |= case(*c) == case(byte);
                    rest
                }
                [start, b'-', end, rest @ ..] => {
                    matched |= (case(*start)..=case(*end)).contains(&case(byte));
                    rest
                }
                [c, rest @ ..] => {
                    matched |= case(*c) == case(byte);
                    rest
                }
                _ => break,
            }
        }

        if not ^ matched {
            Some(pattern)
        } else {
            None
        }
    };

    let mut state = (pattern, string);
    loop {
        state = match state {
            ([b'?', pattern @ ..], [_, string @ ..]) => (pattern, string),
            ([b'*', pattern @ ..], _) => {
                let pattern = trim_start(pattern, b'*');
                return (0..=string.len()).any(|index| matches(&string[index..], pattern));
            }
            ([b'[', pattern @ ..], [c, string @ ..]) => {
                if let Some(end) = brackets(pattern, *c) {
                    (end, string)
                } else {
                    return false;
                }
            }
            ([b'\\', p, pattern @ ..] | [p, pattern @ ..], [c, string @ ..]) => {
                if case(*p) != case(*c) {
                    return false;
                }
                (pattern, string)
            }
            ([], []) => return true,
            (pattern, []) => return trim_start(pattern, b'*').is_empty(),
            _ => return false,
        };
    }
}

pub fn matches(string: &[u8], pattern: &[u8]) -> bool {
    glob(string, pattern, |x| x)
}

pub fn matches_nocase(string: &[u8], pattern: &[u8]) -> bool {
    glob(string, pattern, |x| x.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq() {
        assert!(matches(b"abc", b"abc"));
    }

    #[test]
    fn any() {
        assert!(matches(b"abc", b"ab?"));
        assert!(matches(b"abc", b"a??"));
        assert!(matches(b"abc", b"a?c"));
    }

    #[test]
    fn glob() {
        assert!(matches(b"abc", b"*"));
        assert!(matches(b"abc", b"a*c"));
        assert!(matches(b"abc", b"a**c"));
    }

    #[test]
    fn trailing() {
        assert!(matches(b"abc", b"abc*"));
    }

    #[test]
    fn leading() {
        assert!(matches(b"abc", b"*abc"));
        assert!(matches(b"abc", b"*bc"));
    }

    #[test]
    fn brackets() {
        assert!(matches(b"abd", b"a[bc]d"));
        assert!(matches(b"acd", b"a[bc]d"));
        assert!(matches(b"ac", b"a[bc"));
    }

    #[test]
    fn brackets_escapes() {
        assert!(matches(b"a-d", b"a[\\-]d"));
    }

    #[test]
    fn brackets_dash() {
        assert!(matches(b"abd", b"a[a-d]d"));
        assert!(!matches(b"afd", b"a[a-d]d"));
    }

    #[test]
    fn brackets_not() {
        assert!(!matches(b"abd", b"a[^bc]d"));
        assert!(!matches(b"acd", b"a[^bc]d"));
        assert!(matches(b"aed", b"a[^bc]d"));
        assert!(matches(b"afd", b"a[^bc]d"));
    }

    #[test]
    fn escapes() {
        assert!(matches(b"ab[d]", b"ab\\[d\\]"));
        assert!(matches(b"ab*", b"ab\\*"));
        assert!(!matches(b"abc", b"ab\\*"));
        assert!(matches(b"ab?", b"ab\\?"));
        assert!(!matches(b"abc", b"ab\\?"));
        assert!(matches(b"ab[", b"ab\\["));
        assert!(!matches(b"abc", b"ab\\["));
        assert!(matches(b"ab]", b"ab]"));
    }

    #[test]
    fn nocase() {
        assert!(matches_nocase(b"ABC", b"abc"));
        assert!(matches_nocase(b"abc", b"ABC"));
        assert!(matches_nocase(b"abc", b"AB[C]"));
        assert!(matches_nocase(b"abc", b"AB[C-D]"));
    }
}
