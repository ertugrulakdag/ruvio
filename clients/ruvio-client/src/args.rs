//! Argument plumbing that keeps command paths off the heap.
//!
//! [`Args`] lends any argument list to the encoder as `&[&str]`. A fixed array of `&str`
//! is lent as it is; owned lists of up to [`STACK_ARGS`] items are borrowed into a stack
//! array, so only long variadic commands allocate a pointer vector.
//!
//! [`Num`] formats an integer or a float into a stack buffer, so `EXPIRE key 60` or
//! `ZADD key 1.5 m` never builds a `String` for the number.

use std::fmt::{self, Write as _};

/// Owned argument lists up to this length borrow into a stack array.
pub(crate) const STACK_ARGS: usize = 16;

/// Anything a command can be built from. See the module docs.
pub(crate) trait Args {
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R;
}

impl<const N: usize> Args for [&str; N] {
    #[inline]
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        f(self)
    }
}

impl Args for &[&str] {
    #[inline]
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        f(self)
    }
}

impl Args for Vec<&str> {
    #[inline]
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        f(self)
    }
}

impl<const N: usize> Args for [String; N] {
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        borrow_owned(self, f)
    }
}

impl Args for Vec<String> {
    fn with_slice<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        borrow_owned(self, f)
    }
}

fn borrow_owned<R>(owned: &[String], f: impl FnOnce(&[&str]) -> R) -> R {
    if owned.len() <= STACK_ARGS {
        let mut stack = [""; STACK_ARGS];

        for (slot, argument) in stack.iter_mut().zip(owned) {
            *slot = argument.as_str();
        }

        return f(&stack[..owned.len()]);
    }

    let borrowed: Vec<&str> = owned.iter().map(String::as_str).collect();

    f(&borrowed)
}

/// A number rendered into a stack buffer. Integers print in decimal; floats print in
/// Rust's shortest round-trip form (`1.5`, `1e300`, `inf`), which Ruvio parses.
pub(crate) struct Num {
    buffer: [u8; 40],
    length: u8,
}

impl Num {
    #[inline]
    pub(crate) fn int(value: i64) -> Self {
        let mut number = Self::empty();
        let negative = value < 0;
        let mut magnitude = value.unsigned_abs();
        let mut digits = [0_u8; 20];
        let mut count = 0;

        loop {
            digits[count] = b'0' + (magnitude % 10) as u8;
            count += 1;
            magnitude /= 10;

            if magnitude == 0 {
                break;
            }
        }

        if negative {
            number.push(b'-');
        }

        while count > 0 {
            count -= 1;
            number.push(digits[count]);
        }

        number
    }

    #[inline]
    pub(crate) fn uint(value: u64) -> Self {
        let mut number = Self::empty();
        let mut magnitude = value;
        let mut digits = [0_u8; 20];
        let mut count = 0;

        loop {
            digits[count] = b'0' + (magnitude % 10) as u8;
            count += 1;
            magnitude /= 10;

            if magnitude == 0 {
                break;
            }
        }

        while count > 0 {
            count -= 1;
            number.push(digits[count]);
        }

        number
    }

    /// `#n`: the n-th field of a `BITFIELD` type.
    pub(crate) fn hash_uint(value: u64) -> Self {
        let mut number = Self::empty();

        number.push(b'#');

        let digits = Self::uint(value);

        for byte in digits.as_str().bytes() {
            number.push(byte);
        }

        number
    }

    /// Wide unsigned values (millisecond deadlines are `u128`); saturates at `u64::MAX`.
    #[inline]
    pub(crate) fn wide(value: u128) -> Self {
        Self::uint(u64::try_from(value).unwrap_or(u64::MAX))
    }

    pub(crate) fn float(value: f64) -> Self {
        let mut number = Self::empty();

        // Debug is the shortest round-trip form and switches to an exponent for very large or
        // small values, so the text always fits the buffer.
        let _ = write!(number, "{value:?}");

        number
    }

    /// A JSON-friendly number: whole values below 2^53 print as integers (`2`, not `2.0`), so
    /// `JSON.NUMINCRBY` keeps integer arithmetic; anything else prints as [`Self::float`].
    pub(crate) fn json_number(value: f64) -> Self {
        if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
            return Self::int(value as i64);
        }

        Self::float(value)
    }

    #[inline]
    pub(crate) fn as_str(&self) -> &str {
        // Only ASCII digits, signs, dots, `e`, `inf` and `NaN` are written.
        std::str::from_utf8(&self.buffer[..usize::from(self.length)]).unwrap_or("0")
    }

    fn empty() -> Self {
        Self {
            buffer: [0; 40],
            length: 0,
        }
    }

    #[inline]
    fn push(&mut self, byte: u8) {
        if usize::from(self.length) < self.buffer.len() {
            self.buffer[usize::from(self.length)] = byte;
            self.length += 1;
        }
    }
}

impl fmt::Write for Num {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for byte in text.bytes() {
            self.push(byte);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Args, Num};

    #[test]
    fn numbers_format_on_the_stack() {
        assert_eq!(Num::int(0).as_str(), "0");
        assert_eq!(Num::int(-42).as_str(), "-42");
        assert_eq!(Num::int(i64::MIN).as_str(), "-9223372036854775808");
        assert_eq!(Num::uint(u64::MAX).as_str(), "18446744073709551615");
        assert_eq!(Num::float(1.5).as_str(), "1.5");
        assert_eq!(Num::float(2.0).as_str(), "2.0");
        assert_eq!(Num::float(1e300).as_str(), "1e300");
        assert_eq!(
            Num::float(-2.2250738585072014e-308).as_str(),
            "-2.2250738585072014e-308"
        );
        assert_eq!(Num::float(f64::INFINITY).as_str(), "inf");
        assert_eq!(Num::json_number(2.0).as_str(), "2");
        assert_eq!(Num::json_number(-2.5).as_str(), "-2.5");
        assert_eq!("1e300".parse::<f64>().unwrap(), 1e300);
    }

    #[test]
    fn owned_lists_borrow_without_changing_order() {
        let owned = vec!["a".to_owned(), "b".to_owned()];

        assert_eq!(owned.with_slice(|slice| slice.join(",")), "a,b");

        let long: Vec<String> = (0..40).map(|index| index.to_string()).collect();

        assert_eq!(long.with_slice(|slice| slice.len()), 40);
        assert_eq!(["x", "y"].with_slice(|slice| slice.len()), 2);
        assert_eq!(
            ["p".to_owned(), "q".to_owned()].with_slice(|slice| slice.join("+")),
            "p+q"
        );
    }
}
