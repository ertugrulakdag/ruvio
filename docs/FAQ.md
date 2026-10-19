# Frequently asked questions

Short answers about shards and slots. Details are in the
[connection model](CONNECTION_MODEL.md) and the
[compatibility notes](COMPATIBILITY.md#sharding-semantics).

## What is a slot?

A slot is a bucket number from 0 to 16,383. Every key belongs to exactly one
slot, and the slot is computed from the key name alone:

1. If the key contains a non-empty `{...}` part, only the text inside the
   first braces is used (the hash tag). Otherwise the whole key is used.
2. The CRC16 checksum of those bytes is taken (the same CRC16 as Redis Cluster).
3. The checksum modulo 16,384 is the slot.

The same key gives the same slot on every server, every client and every run.

## What is a shard, and how does a key get to one?

A shard is one worker inside a Ruvio process with its own data and its own
port. The 16,384 slots are split into consecutive ranges, one per shard. With
2 shards, slots 0-8191 belong to the first and 8192-16383 to the second; the
shard of a slot is `slot * shards / 16384`. So a key's place is decided by its
name: key to slot to shard. Nothing is stored about where a key lives and
nothing moves a key between shards while the server runs.

## Who decides this, and where?

The rule is in `src/routing.rs` of the server (`hash_slot` and
`shard_for_slot`). Every client library has its own copy, checked against the
same test vectors. The client asks the server for the slot ranges with
`CLUSTER SLOTS` when it connects and sends each key to its owner. If a key
reaches the wrong shard anyway, the server forwards it to the owner and answers
on the same connection.

## How do I find out which shard holds a key?

Ask the server for the slot, then look it up in the slot ranges:

```
CLUSTER KEYSLOT user:1
CLUSTER SLOTS
```

You rarely need this. The clients do it for you.

## Why do `MGET`, `MSET`, `DEL` or `EVAL` return `CROSSSLOT`?

Their keys are in different slots. A command that reads or writes several keys
must see them all in one place to stay correct, so the server refuses it, as
Redis Cluster does, and changes nothing. This holds even when both slots
happen to be on the same shard.

## How do I make several keys work together?

Give them a common hash tag: `{order:7}:items` and `{order:7}:total` share the
slot of `order:7`, so they share a shard. Then `MGET`, `MULTI`/`EXEC`, `EVAL`
and the like work on them. Do not put one tag on all your keys, or one shard
gets all the load.

## Do the clients handle keys in several slots for me?

For `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH`, yes: the client
splits the command per slot and merges the replies. It is not atomic. Other
commands, including `EVAL`, fail with `CROSSSLOT` before anything is sent. See
[client behavior](CLIENT_BEHAVIOR.md).

## What may a script touch?

Only the keys declared with `EVAL`, `EVALSHA` or `FCALL`, and those keys must
be in one slot. A script that declares no keys runs on the shard of the
connection that sent it.
