# Memory Accounting

Ruvio maintains deterministic memory charges inside each shard. The charge
model is intended for future admission control, `maxmemory`, and eviction. It
is not an RSS estimator: allocator metadata, page fragmentation, runtime
buffers, and platform-specific collection internals can make process RSS
different.

## Categories

- `key_bytes`: bytes in shard-owned key allocations.
- `value_bytes`: byte-value capacity and collection element payload
  allocations. Sorted-set members are charged for both owned indexes because
  the current representation stores two copies.
- `collection_bytes`: capacities and estimated node/index storage owned by
  lists, sets, hashes, and sorted sets. Compact hashes charge vector capacity;
  promoted hashes charge table capacity.
- `index_bytes`: the primary key index capacity, including its inline key and
  entry slots. The default hash index charges every allocated bucket, so
  delete tombstones do not lower the charge. With a memory limit, a full table
  that still has at least 1/8 free capacity rehashes tombstones in place
  instead of doubling.
- `pooled_bytes`: cleared byte buffers retained by the bounded shard-local
  value pool.
- `entries`: physical entries currently retained by the shard.

`charged_bytes` is the saturating sum of all byte categories.

## Mutation Deltas

`KvShard::memory_usage()` is an O(1) snapshot. Subtract two snapshots with
`MemoryUsage::delta_from` to obtain a signed `MemoryDelta` for one command or
batch:

```rust
let before = shard.memory_usage();
shard.set_bytes(b"key", b"value");
let delta = shard.memory_usage().delta_from(before);
```

The shard reconciles the affected entry after every mutation. It does not scan
the whole keyspace. Index capacity and pool retention are refreshed from their
owners after each reconciliation.

Deleting a byte value may transfer bytes from `value_bytes` to `pooled_bytes`
instead of releasing them. Reusing that buffer performs the inverse transfer.
This is intentional: both states consume the shard's memory budget.

## Expiration

Passive expiration makes a key logically absent, but its allocation remains
charged until a mutation or the future active-expiration scheduler physically
removes it. This prevents memory limits from ignoring expired-but-retained
allocations.

## Limits

Configured `max_shard_bytes` minus `reserved_headroom_bytes` is the admission
budget. Connection, output, WAL dirty, and snapshot journal bytes are included
in `charged_bytes`. The default policy is `noeviction`: writes that would
exceed the budget return `OOM command not allowed when used memory > 'maxmemory'.`
and do not record a mutation.

Commands that can grow memory are refused; Ruvio refuses a few more than Redis
does (`SMOVE`, which can create or convert its destination set, the `HEXPIRE`
family, `GETEX`, `JSON.CLEAR`/`JSON.ARRPOP`/`JSON.ARRTRIM`, `XCLAIM`,
`XAUTOCLAIM` and `XREADGROUP`). Commands that only remove or move data, such as
`DEL`, `UNLINK`, `GETDEL`, `HDEL`, `SREM`, `ZREM`, `LPOP`/`RPOP`, `LTRIM`,
`XDEL`, `XTRIM`, `RENAME` and `PERSIST`, still run on a full store, directly,
inside `MULTI` and from scripts, so deleting keys frees memory. Commands that
set a TTL stay refused, because the first deadline allocates the expiry index,
except `EXPIRE` with a deadline already past, which deletes the key. While a
checkpoint is running, a deleted or changed key keeps its old value in the
snapshot journal until the checkpoint ends, so `charged_bytes` and process RSS
can stay at or above the limit until then.

`maxmemory-policy` may be `allkeys-lru`, `allkeys-lfu`, `volatile-lru`, or
`volatile-ttl`. Eviction is shard-local. The default examines 16 candidates
(`maxmemory-samples`) and deletes at most 16 keys per command. Active expiry
reaps `active-expire-keys` (default 16) per command or idle tick. After a
delete or a much smaller overwrite, the key index, set, hash, sorted-set
member map, list ring, compact hash vector, and string buffer shrink once
they are under a quarter full. The hot-key map uses the same rule. Victims are persisted as WAL `Delete` records. Evicted byte values are
discarded rather than returned to the shard pool so `charged_bytes` can fall.
Recency and frequency are not written to snapshots; recovered keys start cold.
A single write larger than the shard budget is rejected without evicting.

`allocation_events` / `allocation_bytes` count mutations that increased
`charged_bytes`. Pool retention is `pooled_bytes`.

`max_process_bytes` is validated against the per-shard budget at startup. Zero
disables a budget.

## Active Expiration

A bounded active-expiration cycle removes a small number of already-due keys
on each command and on the WAL sync interval. Passive expiration still treats
due keys as absent on lookup. Physical charges remain until the entry is
removed.
