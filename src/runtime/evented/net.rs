//! Bytes read from and written to client sockets. A shard is one thread, so the counters
//! are thread-local cells: one add per socket read or write, no atomic and no lock.

use std::cell::Cell;

thread_local! {
    static INPUT: Cell<u64> = const { Cell::new(0) };
    static OUTPUT: Cell<u64> = const { Cell::new(0) };
}

pub(super) fn note_input(bytes: usize) {
    INPUT.with(|total| total.set(total.get().wrapping_add(bytes as u64)));
}

pub(super) fn note_output(bytes: usize) {
    OUTPUT.with(|total| total.set(total.get().wrapping_add(bytes as u64)));
}

/// This shard's totals, read on the shard's own thread.
pub(super) fn totals() -> (u64, u64) {
    (INPUT.with(Cell::get), OUTPUT.with(Cell::get))
}
