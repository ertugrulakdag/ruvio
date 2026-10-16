# Vendored hashbrown

A trimmed copy of [hashbrown](https://github.com/rust-lang/hashbrown) **0.17.1** (crate release, git commit `c62a63a61b7caf2de8f9ecb7b06a66b0ab6bdf3d`), MIT or Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`). The server's key index, slot tables and collections use its `HashTable`.

## What differs from upstream

| Change | Where |
| --- | --- |
| Only `HashTable` and its raw table are kept; `map.rs`, `set.rs`, `raw_entry.rs`, `rustc_entry.rs`, `hasher.rs` and `external_trait_impls/` (rayon, serde) are removed | whole module |
| Load factor: one empty bucket in 64 instead of one in 8, so one million keys fit in 2^20 buckets | `raw.rs` `capacity_to_buckets`, `bucket_mask_to_capacity` |
| A rebuild counter (`epoch`, `seed_epoch`), advanced by `clear`, so a stale cursor can tell it belongs to an older table | `raw.rs`, `table.rs` |
| A scan that survives rehash, grow and shrink (`scan_homes`, `scan_block`, `scan_select_mask`), walked in reverse-binary order like Redis `dictScan` | `raw.rs`, `table.rs` |
| Capacity accessors and in-place tombstone cleanup (`allocated_buckets`, `growth_left`, `full_capacity`, `reclaim_tombstones`) | `raw.rs`, `table.rs` |
| `prefetch_read`, `prefetch_ptr` | `mod.rs` |
| Paths rewritten for a module inside the crate (`crate::hashbrown::...`), upstream tests compiled out with `#[cfg(any())]`, and small lint rewrites (`is_multiple_of`, `Option::map`) with no change in behavior | `alloc.rs`, `control/`, `raw.rs`, `table.rs` |

Each addition is marked `Ruvio:` or `RUVIO:` in a comment. `util.rs`, `scopeguard.rs` and `macros.rs` are unchanged.

## Tests

Upstream's own test modules are kept but compiled out (`#[cfg(any())]`). `table.rs` `ruvio_tests` covers the additions: the load factor, tombstone cleanup, the epoch, and the scan (every element exactly once on a stable table, at least once across grows). `scripts/run_sanitizers.sh miri` includes `hashbrown::`.

## Comparing with upstream

```bash
curl -sSL https://static.crates.io/crates/hashbrown/hashbrown-0.17.1.crate | tar xz
diff -r hashbrown-0.17.1/src/control src/hashbrown/control
diff hashbrown-0.17.1/src/raw.rs src/hashbrown/raw.rs
diff hashbrown-0.17.1/src/table.rs src/hashbrown/table.rs
```

To move to a newer upstream release, apply the rows above to its files, keep the markers, and run the `hashbrown::`, `store::` and `index::` tests.
