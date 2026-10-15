use super::*;

/// The member as an `i64` when it is exactly the canonical decimal text of one: no sign
/// but `-`, no leading zeros, no `-0`. Only then does `format_i64` give the same bytes back.
pub(super) fn parse_canonical_i64(bytes: &[u8]) -> Option<i64> {
    let digits = match bytes {
        [b'-', rest @ ..] => rest,
        _ => bytes,
    };

    if digits.is_empty() || digits.len() > 19 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }

    if digits[0] == b'0' && (digits.len() > 1 || bytes.len() != digits.len()) {
        return None;
    }

    let mut value = 0i128;

    for digit in digits {
        value = value * 10 + i128::from(digit - b'0');
    }

    if bytes.len() != digits.len() {
        value = -value;
    }

    i64::try_from(value).ok()
}

/// Decimal text of `value`, written at the end of `buf`; returns the used tail.
pub(super) fn format_i64(value: i64, buf: &mut [u8; 20]) -> &[u8] {
    let mut at = buf.len();
    let mut rest = value.unsigned_abs();

    loop {
        at -= 1;
        buf[at] = b'0' + (rest % 10) as u8;
        rest /= 10;

        if rest == 0 {
            break;
        }
    }

    if value < 0 {
        at -= 1;
        buf[at] = b'-';
    }

    &buf[at..]
}

/// A sorted integer array in the narrowest width that holds every member. Membership is a
/// binary search; a wider value widens the whole array once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum IntSet {
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn insert_sorted<T: Ord + Copy>(items: &mut Vec<T>, value: T) -> bool {
    match items.binary_search(&value) {
        Ok(_) => false,
        Err(at) => {
            if items.len() == items.capacity() {
                items.reserve_exact((items.len() / 4).max(4));
            }

            items.insert(at, value);
            true
        }
    }
}

impl IntSet {
    /// Builds from values in any order, without duplicates.
    pub(super) fn from_values(values: impl IntoIterator<Item = i64>) -> Self {
        let mut set = Self::I16(Vec::new());

        for value in values {
            set.insert(value);
        }

        set
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::I16(items) => items.len(),
            Self::I32(items) => items.len(),
            Self::I64(items) => items.len(),
        }
    }

    pub(super) fn contains(&self, value: i64) -> bool {
        match self {
            Self::I16(items) => i16::try_from(value).is_ok_and(|v| items.binary_search(&v).is_ok()),
            Self::I32(items) => i32::try_from(value).is_ok_and(|v| items.binary_search(&v).is_ok()),
            Self::I64(items) => items.binary_search(&value).is_ok(),
        }
    }

    pub(super) fn insert(&mut self, value: i64) -> bool {
        if let Self::I16(items) = self
            && i16::try_from(value).is_err()
        {
            let wide: Vec<i32> = items.iter().map(|&v| i32::from(v)).collect();

            *self = Self::I32(wide);
        }

        if let Self::I32(items) = self
            && i32::try_from(value).is_err()
        {
            let wide: Vec<i64> = items.iter().map(|&v| i64::from(v)).collect();

            *self = Self::I64(wide);
        }

        match self {
            Self::I16(items) => insert_sorted(items, value as i16),
            Self::I32(items) => insert_sorted(items, value as i32),
            Self::I64(items) => insert_sorted(items, value),
        }
    }

    pub(super) fn remove(&mut self, value: i64) -> bool {
        fn take<T: Ord + Copy>(items: &mut Vec<T>, value: T) -> bool {
            match items.binary_search(&value) {
                Ok(at) => {
                    items.remove(at);

                    if let Some(target) =
                        crate::index::reclaim_capacity(items.len(), items.capacity())
                    {
                        items.shrink_to(target);
                    }

                    true
                }
                Err(_) => false,
            }
        }

        match self {
            Self::I16(items) => i16::try_from(value).is_ok_and(|v| take(items, v)),
            Self::I32(items) => i32::try_from(value).is_ok_and(|v| take(items, v)),
            Self::I64(items) => take(items, value),
        }
    }

    /// The member at sorted position `at`, which must be below `len()`.
    pub(super) fn get(&self, at: usize) -> i64 {
        match self {
            Self::I16(items) => i64::from(items[at]),
            Self::I32(items) => i64::from(items[at]),
            Self::I64(items) => items[at],
        }
    }

    pub(super) fn iter(&self) -> Box<dyn Iterator<Item = i64> + '_> {
        match self {
            Self::I16(items) => Box::new(items.iter().map(|&v| i64::from(v))),
            Self::I32(items) => Box::new(items.iter().map(|&v| i64::from(v))),
            Self::I64(items) => Box::new(items.iter().copied()),
        }
    }

    pub(super) fn capacity_bytes(&self) -> usize {
        match self {
            Self::I16(items) => items.capacity() * size_of::<i16>(),
            Self::I32(items) => items.capacity() * size_of::<i32>(),
            Self::I64(items) => items.capacity() * size_of::<i64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_canonical_decimals_parse() {
        for text in [
            "0",
            "7",
            "-7",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            let value = parse_canonical_i64(text.as_bytes()).expect(text);
            let mut buf = [0u8; 20];

            assert_eq!(format_i64(value, &mut buf), text.as_bytes());
        }

        for text in [
            "",
            "-",
            "+1",
            "01",
            "-0",
            "00",
            "1.0",
            " 1",
            "1 ",
            "9223372036854775808",
            "-9223372036854775809",
            "99999999999999999999",
        ] {
            assert_eq!(parse_canonical_i64(text.as_bytes()), None, "{text}");
        }
    }

    #[test]
    fn widens_and_stays_sorted() {
        let mut set = IntSet::from_values([5, -3, 100]);

        assert!(matches!(set, IntSet::I16(_)));
        assert!(set.insert(70_000));
        assert!(matches!(set, IntSet::I32(_)));
        assert!(set.insert(i64::MIN));
        assert!(matches!(set, IntSet::I64(_)));
        assert!(!set.insert(5));
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![i64::MIN, -3, 5, 100, 70_000]
        );
        assert!(set.contains(70_000) && !set.contains(70_001));
        assert!(set.remove(5) && !set.remove(5));
        assert_eq!(set.len(), 4);
    }
}
