// SPDX-License-Identifier: MIT
/**
 * Java client for Ruvio, a Redis-compatible server. It speaks RESP2 and RESP3 over one multiplexed
 * socket per node and has no dependencies.
 *
 * <p>Entry points:
 * <ul>
 * <li>{@link io.github.salihcantekin.ruvio.RuvioClient}: every command as a blocking method, plus
 *     helpers for locks, leases, semaphores, rate limits, cache-aside, idempotency, scan iterators,
 *     typed JSON and Bloom "seen before".</li>
 * <li>{@link io.github.salihcantekin.ruvio.RuvioAsync}: the same commands returning
 *     {@code CompletableFuture}.</li>
 * <li>{@link io.github.salihcantekin.ruvio.Pipeline}: buffer commands and send them in one write.</li>
 * <li>{@link io.github.salihcantekin.ruvio.Transaction}: {@code MULTI}/{@code EXEC} on a dedicated connection.</li>
 * <li>{@link io.github.salihcantekin.ruvio.RuvioPubSub}: channel, pattern and shard subscriptions.</li>
 * <li>{@link io.github.salihcantekin.ruvio.RuvioOptions}: host, port, credentials, timeouts and cluster mode.</li>
 * </ul>
 *
 * <p>Errors are unchecked: {@link io.github.salihcantekin.ruvio.RuvioServerException} for an error reply
 * ({@code code()} is the first word, such as {@code WRONGTYPE}),
 * {@link io.github.salihcantekin.ruvio.RuvioConnectionException} when the socket fails, and
 * {@link io.github.salihcantekin.ruvio.RuvioException} for timeouts and other client-side failures.
 *
 * <p>Ruvio differences that matter to callers: in a multi-shard deployment every shard is its own
 * server. With {@code cluster(true)} the client routes keyed commands to the owning shard, sends
 * keyless commands ({@code FLUSHDB}, {@code DBSIZE}, {@code KEYS}, {@code SCAN}) to one node only, and
 * splits {@code MGET}/{@code MSET}/{@code DEL} across shards without atomicity.
 *
 * <pre>{@code
 * try (RuvioClient<String, String> client = RuvioClient.connect("127.0.0.1", 6379)) {
 *     client.set("greeting", "hello");
 *     String value = client.get("greeting");    // "hello"
 *     long hits = client.async().incr("hits").join();
 * }
 * }</pre>
 */
package io.github.salihcantekin.ruvio;
