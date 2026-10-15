# Storage Layout Boundary

`KvShard` is generic over a `StorageIndex`. The default `AHashIndex` preserves
the current randomized hash-map layout. `BTreeIndex` is an ordered reference
implementation used to prove conformance and support controlled layout
experiments.

## Static Dispatch

`StorageIndex` is intentionally not object-safe: its generic
`get_or_insert_with` method prevents use as `dyn StorageIndex`. The command
engine is generic over the selected index, so Rust monomorphizes lookups and
mutations without a virtual call in the hot path.

Normal server shards use:

```rust
KvShard<SystemClock, AHashIndex>
```

Tests or experiments can construct another layout explicitly:

```rust
let shard = KvShard::with_index_and_clock(
    BTreeIndex::with_capacity(1_000),
    SystemClock::default(),
);
```

## Contract

An index owns keys and opaque `Entry` values. It provides:

- borrowed and mutable lookup by byte key,
- insertion and removal,
- lazy insertion for collection commands,
- iteration for logical key counts,
- deterministic retained-capacity charges for memory accounting.

Data-type semantics, expiration, pooling, mutation recording, RESP execution,
and persistence formats remain in `KvShard` and the command engine. A new index
must not reimplement those rules.

## Evaluation Rules

Alternative layouts must pass the complete store and command conformance suite
before performance comparison. Selection must be based on bare-metal profiles
covering:

- mixed reads and writes,
- realistic key and value distributions,
- deletion and TTL churn,
- retained memory and fragmentation,
- p99 and p99.9 latency under saturation.

`BTreeIndex` is a correctness baseline, not a claim that an ordered tree is
appropriate for the production hot path. Future candidates may include a
shard-local open-addressing table or arena-backed compact handles.
