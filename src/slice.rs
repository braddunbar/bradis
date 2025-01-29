use std::ops::Range;

pub fn slice(len: usize, mut start: i64, mut end: i64) -> Option<Range<usize>> {
    if len == 0 {
        return None;
    }

    let len = i64::try_from(len).ok()?;

    if start < 0 {
        start += len;
    }

    if end < 0 {
        end += len;
    }

    if start > end {
        return None;
    }

    if start < 0 {
        start = 0;
    }

    end = end.clamp(0, len - 1) + 1;

    let start = start.try_into().ok()?;
    let end = end.try_into().ok()?;

    Some(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice() {
        // Redis ranges are inclusive.
        assert_eq!(slice(9, 3, 7), Some(3..8));
        assert_eq!(slice(9, 3, 8), Some(3..9));

        // If end > length, end = length.
        assert_eq!(slice(9, 3, 9), Some(3..9));

        // Negative values are inclusive and count from the end.
        assert_eq!(slice(9, 3, -1), Some(3..9));
        assert_eq!(slice(9, 3, -2), Some(3..8));
        assert_eq!(slice(9, -3, -2), Some(6..8));

        // Negative values before the beginning are set to 0.
        assert_eq!(slice(9, -10, -8), Some(0..2));

        // If start > end, return nothing.
        assert_eq!(slice(9, -10, -12), None);
        assert_eq!(slice(9, 5, 4), None);

        // If length is 0, return nothing.
        assert_eq!(slice(0, 1, 4), None);

        // Just one element.
        assert_eq!(slice(1, 0, -1), Some(0..1));
    }
}
