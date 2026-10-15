use std::fmt;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::{vceqq_u8, vld1q_u8, vmaxvq_u8};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[repr(align(64))]
struct AlignedBlock<const N: usize>([u8; N]);

impl<const N: usize> fmt::Debug for AlignedBlock<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AlignedBlock")
            .field(&self.0.as_slice())
            .finish()
    }
}

fn main() {
    let mut data = [b'a'; 64];

    data[37] = b'b';
    let block = AlignedBlock(data);
    let needle = b'b';

    let index = find_byte(&block.0, needle);

    println!("needle {needle:?} found at index {index:?}");
}

#[cfg(target_arch = "x86_64")]
fn find_byte(block: &[u8], needle: u8) -> Option<usize> {
    // Process 16 bytes at a time so the CPU can compare multiple candidates per instruction.
    // SAFETY: SSE2 is part of the x86_64 baseline.
    let needle_vec = unsafe { _mm_set1_epi8(needle as i8) };

    let mut index = 0usize;

    while index + 16 <= block.len() {
        // SAFETY: SSE2 is baseline on x86_64, and the 16-byte load stays inside `block` (`index + 16 <= len`).
        let mask = unsafe {
            let ptr = block.as_ptr().add(index) as *const __m128i;
            // Unaligned loads keep this helper correct for arbitrary subslices.
            let chunk = _mm_loadu_si128(ptr);
            _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, needle_vec)) as u32
        };

        if mask != 0 {
            return Some(index + mask.trailing_zeros() as usize);
        }

        index += 16;
    }

    scalar_find(&block[index..], needle).map(|offset| index + offset)
}

#[cfg(target_arch = "aarch64")]
fn find_byte(block: &[u8], needle: u8) -> Option<usize> {
    let needle_vec = unsafe { std::arch::aarch64::vdupq_n_u8(needle) };

    let mut index = 0usize;

    while index + 16 <= block.len() {
        let has_match = unsafe {
            let chunk = vld1q_u8(block.as_ptr().add(index));
            vmaxvq_u8(vceqq_u8(chunk, needle_vec)) != 0
        };

        if has_match {
            // NEON detects a matching lane; this short scalar scan identifies its index.
            return block[index..index + 16]
                .iter()
                .position(|&byte| byte == needle)
                .map(|offset| index + offset);
        }

        index += 16;
    }

    scalar_find(&block[index..], needle).map(|offset| index + offset)
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
fn find_byte(block: &[u8], needle: u8) -> Option<usize> {
    scalar_find(block, needle)
}

fn scalar_find(block: &[u8], needle: u8) -> Option<usize> {
    block.iter().position(|&byte| byte == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_bytes_in_vector_and_tail_regions() {
        let mut data = [0u8; 35];

        data[16] = 7;
        data[34] = 9;

        assert_eq!(find_byte(&data, 7), Some(16));
        assert_eq!(find_byte(&data, 9), Some(34));
        assert_eq!(find_byte(&data, 3), None);
    }

    #[test]
    fn accepts_unaligned_subslices() {
        let mut data = [0u8; 33];

        data[17] = 1;
        assert_eq!(find_byte(&data[1..], 1), Some(16));
    }
}
