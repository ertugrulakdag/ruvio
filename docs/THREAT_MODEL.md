# Threat Model

Ruvio is a Redis-protocol-compatible, memory-first key-value store. This
document is the Milestone 6 threat model for standalone operation. It covers
the RESP parser, persistence files, administrative commands, cluster
discovery, and denial-of-service paths.

Cluster membership, a cluster bus, and failover are not implemented. Those
surfaces are listed so they are not mistaken for existing controls. WAL tail
replication is a separate listener, described under Replication port.

Snapshot date: 2026-09-27. Revisit this file when a wire, persistence, or
administrative command changes.

## Trust boundaries

| Boundary | Inside | Outside |
| --- | --- | --- |
| RESP listener | Shard owners, WAL, snapshots, ACL rule copies | Any TCP/TLS client |
| Replication listener | Primary WAL shipper for up to 16 replicas | A replica that dials `replication.primary` |
| Metrics HTTP listener | Same process, scrape-only | Any HTTP client on that bind |
| Data directory | Process with `LOCK` | Other processes, operators, backups |
| Host | Kernel, filesystem, TLS PEM files, config | Network attackers |
| TLS terminator | Optional proxy in front of the listener | Clients that stop at the terminator |

There is no authenticated cluster bus and no trust relationship between Ruvio
processes. Consecutive-port multi-shard mode is one process advertising static
topology.

## Assets

1. **Keyspace** — in-memory values and collection members.
2. **Durable mutations** — WAL records, snapshots, `CURRENT`, `CLOCK`,
   manifests.
3. **Secrets** — `requirepass`, ACL user passwords, TLS private keys.
4. **Availability** — CPU, memory, file descriptors, disk, accept capacity.
5. **Operator metadata** — bind address, data-dir path, TLS cert path, shard
   layout.

## Attackers

- Unauthenticated network client on the RESP or metrics port.
- Authenticated client with a restricted ACL user.
- Local process that can read or write the data directory but does not hold
  `LOCK`.
- Operator who restores an untrusted backup or disables protected mode.
- Future cluster peer (out of scope; no bus exists to attack).

The default bind is loopback. A public bind without a password keeps
protected mode on and rejects non-loopback peers.

## Parser

The RESP2 parser is the first untrusted-data path. It must not panic, must
not allocate from attacker-controlled lengths before checking limits, and must
not consume past a truncated frame.

| Threat | Control | Tests |
| --- | --- | --- |
| Bulk-string or array length bombs | `ProtocolLimits` (`max_command_bytes`, `max_key_bytes`, `max_value_bytes`, `max_arguments`) checked before allocation | `protocol::rejects_oversized_bulk_strings_before_allocating`, `enforces_key_and_value_limits_in_every_parser` |
| Pipeline / connection buffer growth | `max_pipeline_commands`, `max_connection_buffer_bytes`, idle and read timeouts | `server::applies_configured_protocol_limits`, `limits_inactive_partial_frame_reads` |
| Truncated or garbage frames | Incremental parse returns incomplete; consume is bounded; a framing error closes the connection | `every_truncated_*_frame_is_incomplete`, `parsers_bound_consume_on_small_garbage_frames`, `parsers_never_panic_or_consume_past_arbitrary_input` |
| Unknown name, wrong arity, or unsupported subcommand | The frame is consumed and answered with `-ERR`. The socket stays open. `CONFIG SET`, `CLUSTER MEET`, `SHUTDOWN`, `DEBUG`, and `MONITOR` are in this class. `FLUSHDB`, `FLUSHALL`, and `SWAPDB` exist and are in the ACL `@dangerous` category. `ACL SETUSER` changes only the in-memory user list | `protocol::rejects_mutating_admin_and_cluster_verbs`, `server::rejected_admin_verbs_reply_and_keep_the_connection`, `evented::evented_rejected_admin_verbs_reply_and_keep_the_connection` |
| Binary keys and values | Values are length-prefixed bytes; they are never interpreted as paths or Lua source unless the command is `EVAL` | store binary round-trips |

The library crate forbids `unsafe`. Hot-path parse borrows the connection
buffer; a parsed view does not outlive the buffer.

Residual: a client that stays inside the configured limits can still send
large legal values. Memory admission and output-buffer caps bound the effect
after parse.

## Persistence files

On-disk state is operator-trusted at restore time and integrity-checked at
load. Version 1 is uncompressed and unencrypted.

| Threat | Control | Tests |
| --- | --- | --- |
| Bit flips, torn writes | WAL CRC32C per record; snapshot and manifest BLAKE3 plus CRC32C; magic and major version | `persistence::codec::detects_header_and_record_corruption`, `wal::rejects_mid_log_checksum_corruption`, `snapshot::rejects_snapshot_corruption_before_returning_entries` |
| Truncated last WAL record | Writable recovery truncates only a torn **final** record. `ruvio-ctl check`/`inspect` report it and do not truncate | `wal::truncates_only_a_torn_final_record`, `inspect_does_not_truncate_a_torn_final_record` |
| Path traversal from a hostile manifest | Snapshot names must be exactly `shard-NNNN.snap` inside the generation directory | `snapshot::rejects_snapshot_filenames_outside_the_checkpoint_directory` |
| Dual writers | Exclusive `LOCK` for the server, backup, and restore | `data_directory_lock_excludes_a_second_owner`, `backup_fails_while_the_data_directory_is_locked` |
| Oversize snapshot | `snapshot_max_bytes` before returning entries | `rejects_snapshots_over_the_configured_size_limit` |
| Clock rollback after restart | Persisted `CLOCK` floor; TTLs never jump backward | `persists_a_monotonic_clock_floor`, `recovered_clock_floor_is_the_persisted_last_seen_time` |
| WAL record larger than protocol limits | Decode uses the same `ProtocolLimits` as the wire | codec length tests |
| Hostile backup restore | `restore` validates the source with the same checkers as startup; `--force` is required to overwrite an existing destination | `tooling::checks_backups_and_restores_a_wal_directory` |

Residual:

- No encryption at rest. Anyone who can read the data directory reads every
  key. Protect the directory with host permissions and, if needed, volume
  encryption.
- Restore trusts a source that passes checksums. An attacker who can write a
  well-formed WAL can plant values. Treat backup provenance as an operational
  control.
- `always` still depends on the filesystem honoring `fdatasync`. That is a
  durability contract, not a confidentiality control.

## Administrative commands

These commands change process behavior, disclose configuration, or force
disk I/O. They sit in ACL categories `admin`, `slow`, and `dangerous`.

| Command | Effect | Control |
| --- | --- | --- |
| `AUTH` | Selects a user | Constant-time compare; `NOAUTH` until success; not allowed inside `MULTI` |
| `ACL WHOAMI/USERS/LIST/GETUSER/CAT/SETUSER` | Introspect or change users | `SETUSER` updates the in-memory directory only. `SAVE`/`LOAD` do not parse. Passwords stay redacted |
| `CONFIG GET` | Lists settings | `requirepass` is `***`. `CONFIG SET` is an error reply and the connection stays open |
| `INFO` / `INFO prometheus` | Counters and memory | No secrets. ACL `@admin` can deny it |
| `SAVE` / `BGSAVE` | Checkpoint + WAL compact | Persistence `MISCONF` on failure; `BGSAVE` is in-process, not a fork |
| `SCRIPT LOAD/EXISTS/FLUSH/KILL` | Script cache | Size and count caps. `SCRIPT KILL` returns `NOTBUSY` because eval is synchronous |
| `EVAL` / `EVALSHA` | Lua 5.1 | See Scripts below |
| Process shutdown | Drain accept and clients | `SIGINT`/`SIGTERM` only; `SIGHUP` is ignored. There is no Redis `SHUTDOWN` command |

Default ACL until `requirepass` or a `default` user with a password is
`on nopass ~* +@all`. A public listener in that state is an intentional
misconfiguration; protected mode rejects non-loopback peers when no password
is set.

`CONFIG GET` still returns filesystem paths (`data-dir`, `tls-cert`,
`tls-key`). Restrict `@admin` for tenants that must not see host layout.

Tests: `auth_rejects_commands_until_the_password_matches`,
`config_get_redacts_requirepass_over_tcp`,
`acl_named_user_limits_commands_and_keys`,
`security::protected_mode_denies_non_loopback_without_auth`,
`protocol::rejects_mutating_admin_and_cluster_verbs`.

## Cluster messages

There is no cluster bus, no gossip, no epoch, and no authenticated node
identity.

| Surface | Behavior |
| --- | --- |
| `CLUSTER SLOTS` / `CLUSTER NODES` | Portable one-shard: `-ERR cluster mode is disabled`. Linux multi-shard: static slot ranges, advertised host, consecutive ports. `CLUSTER NODES` prints a synthetic `@bus_port` (`port + 10000`) that nothing listens on |
| `MOVED` / `CROSSSLOT` | Hash-slot routing inside one process |
| `CLUSTER MEET`, migrate, failover | Not parsed |

A client that can run `CLUSTER` (`@admin`) learns the advertised host and
shard ports. That is experimental discovery, not a membership protocol. Do
not expose those ports beyond the same trust domain as the RESP listener.

Tests: `protocol::parses_cluster_discovery_commands`,
`protocol::rejects_mutating_admin_and_cluster_verbs`.

## Replication port

The replication port is a separate listener from RESP and from metrics. Role
`off` does not open it. A primary binds `replication.listen`, loopback by
default in the sample configuration (`127.0.0.1`). The handshake uses
`replication_password` when set, otherwise `requirepass`, compared in constant
time (`security::constant_time_eq`). A public replication listen without
either secret refuses to start. A failed handshake sends a status and no WAL
bytes. Failover is not implemented, and this port is not a cluster bus.

## Denial of service

Shared-nothing shards do not take a lock on the command path. Admission and
quotas run on the accept/control path or as a per-connection token bucket.

| Threat | Control | Tests |
| --- | --- | --- |
| Connection flood | `max_global`, `max_per_ip`, per-user cap after AUTH | `rejects_connections_over_the_per_ip_limit`, `caps_authenticated_connections_per_acl_user` |
| Command flood on one socket | Per-connection token bucket (`max_commands_per_second`) | `rate_limits_commands_on_one_connection` |
| Slowloris / idle sockets | Idle, read, and write timeouts | `closes_idle_connections_after_the_configured_timeout` |
| Slow parse of a partial frame | Inactive partial-frame read limit | `limits_inactive_partial_frame_reads` |
| Huge replies | `max_output_buffer_bytes` before allocating the full payload | `bounds_encoded_responses_before_allocating_the_full_payload` |
| Queue saturation | `max_queued_requests` → `TRYAGAIN` | `returns_try_again_when_the_store_queue_is_full` |
| Memory exhaustion | Shard/process budgets, `noeviction` or sampled eviction, reserved headroom | engine/store memory tests |
| Lua busy-loop | Instruction hook on every coroutine, with each coroutine charged the 1,000 instructions its start runs unseen; a wall-clock deadline (`scripts.max_time_ms`) checked in the hook; string and table library calls charged by the bytes or elements they handle, each also checking the deadline; the hook checks more often once the Lua heap holds large values; `ruvio.call` checks the deadline; once either limit is reached `pcall`, `xpcall` and `coroutine.resume` pass the error on and an `xpcall` handler is not run; pattern matching charged to the quota; `ruvio.call` quota; `os`/`io`/`debug`/`package` not loaded; `load`/`loadstring`/`loadfile`/`dofile`/`require`/`string.dump` removed | `script::sandbox_rejects_host_escape_and_admin_calls`, `script::eval_enforces_instruction_and_call_quotas`, `script::an_endless_loop_inside_a_coroutine_or_pcall_hits_the_quota`, `script::a_backtracking_pattern_hits_the_quota_instead_of_holding_the_shard`, `script::library_functions_that_loop_in_c_return_promptly`, `script::unpack_with_extreme_bounds_fails_instead_of_crashing`, `script::coroutines_are_charged_for_the_instructions_the_hook_misses`, `script::work_that_grows_with_data_is_stopped_by_the_quota_or_the_deadline`, `script::an_xpcall_handler_cannot_outlive_the_quota`, `script::large_values_cannot_hold_the_shard_past_the_deadline`, `script::a_table_length_far_past_its_entries_is_charged_before_it_is_read`, `lua_pattern::tests::malformed_patterns_never_panic` |
| Blocking lists | Bounded waiter count | `list_waiters` tests |
| Disk fill via WAL/snapshot | Segment size, snapshot cap, compaction after checkpoint. Full-disk crash behavior is a hardware P0, not claimed here | snapshot size tests |

Rate limits are **per connection**, not a global or per-IP command lock, so
they do not introduce a shard mutex. A botnet of many connections still needs
`max_global` / `max_per_ip`.

## Scripts

Lua 5.1 (`mlua`) loads only `table`, `string`, and `math`. `math.random` and
`math.randomseed` are removed. Scripts may `ruvio.call` / `ruvio.pcall` only
declared same-slot keys. Admin, AUTH, transaction, cluster, blocking-list,
and nested-eval commands are not scriptable. Failed scripts restore copied
entries before reply. Writes persist as one `Transaction` WAL batch, not by
replaying Lua.

Residual: a script holds its shard until the instruction quota, its
pattern-matching steps (8 per instruction of the quota) or the wall-clock
deadline (`scripts.max_time_ms`, 1,000 ms by default, at most 3,600,000) runs
out. The hook checks both every 1,000 VM instructions, in every coroutine, and
more often as the Lua heap grows: the interval shrinks in proportion above 1 MiB
of heap, down to 10 instructions at 100 MiB. Charged library calls check them
before they run, and `ruvio.call` checks the deadline. An `xpcall` handler runs
without the hook, so once a limit is reached the handler is skipped and the
error passed on. Size both limits for the deployment. Redis relies on
`SCRIPT KILL` for this; here `SCRIPT KILL` cannot interrupt an in-flight eval,
so these limits are the stop mechanism.
`load` and `loadstring` stay removed: the vendored Lua 5.1 parser lacks the
CVE-2025-49844 fix.

Known limit: a single `..`, comparison or library call cannot be interrupted,
so a script overruns the deadline by the work done since the last check: at
most 10 such operations on values near the 128 MiB memory limit, each a copy or
scan of that value. In testing, loops of `s < s` on a 32 MiB string and `s .. s`
on 16 MiB strings stopped about 50-80 ms past a 1,000 ms deadline in a release
build. Work the quota charges stops at the quota instead (about 0.1-0.5 s with
the default quota).

## TLS and metrics

- TLS (rustls 0.23) is supported. Handshake completes on the accept
  path. Certificate files reload on the next handshake when mtimes change.
  Plaintext clients are closed. `tls_client_ca` checks a presented client
  certificate. `tls_client_auth` requires one. A plaintext listener does not
  ask for a certificate.
- HTTP `/metrics` is optional and scrape-only. Loopback with an empty
  `metrics_token` is open. Any other bind requires `Authorization: Bearer`
  matching `metrics_token` (`RUVIO_METRICS_TOKEN`). Startup refuses a public
  metrics bind without that token.

## Residual risks (accepted for this milestone)

1. Hardware-only P0s: process-kill crash harness, full-disk and permission
   fault injection, 24-hour soak, published bare-metal performance. See
   `docs/RELEASE_EVIDENCE.md`.
2. No encryption at rest; no signed provenance on restore sources.
3. No signed SLSA provenance on release artifacts yet. Checksums and a
   CycloneDX SBOM are produced; see `docs/SUPPLY_CHAIN.md`.
4. Experimental static cluster discovery is not an authenticated cluster.
5. Unauthenticated metrics HTTP on loopback when no token is set. A public
   metrics bind requires `metrics_token`.
6. `CONFIG GET` path disclosure to anyone allowed `config` (`@admin`).

## Operator checklist

1. Keep the default loopback bind, or set `requirepass` / ACL passwords
   before publishing a port.
2. Do not disable protected mode on a public bind unless every client is
   otherwise authenticated (containers that publish ports often must; they
   still warn at startup).
3. Restrict `@admin` for application users; grant `~pattern` key globs.
4. Set connection, rate, output, script, and memory limits for the threat
   you actually have. Defaults of `0` on per-user and per-command-rate caps
   mean unlimited.
5. Protect the data directory and TLS key with host permissions. Treat
   backups as secret material.
6. Keep metrics and RESP binds off the public internet unless a scrape
   identity sits in front.
