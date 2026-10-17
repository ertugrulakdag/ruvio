// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Section name of {@code INFO section}. The server compares names without case or spaces and answers an unknown name with an empty reply, so use these values.
 * {@link #PROMETHEUS} switches the output to the Prometheus text format of the shard.
 *
 * <p>Example: {@code client.info(InfoSection.MEMORY)}
 */
public enum InfoSection {
    /** Version, health, durability and shard identity. */
    SERVER("server"),
    /** Connection and traffic counters. */
    STATS("stats"),
    /** Buffer pool usage. */
    BUFFERS("buffers"),
    /** Memory use. */
    MEMORY("memory"),
    /** Memory use by data kind (sent as {@code memorybreakdown}). */
    MEMORY_BREAKDOWN("memorybreakdown"),
    /** Key counts. */
    KEYSPACE("keyspace"),
    /** Snapshot state. */
    SNAPSHOT("snapshot"),
    /** Cluster state. */
    CLUSTER("cluster"),
    /** Replication role and lag. */
    REPLICATION("replication"),
    /** Per-kind command counters (sent as {@code commandcounters}). */
    COMMAND_COUNTERS("commandcounters"),
    /** Command latency percentiles. */
    LATENCY_STATS("latencystats"),
    /** Log (WAL) pages and extents. */
    LOGS("logs"),
    /** Slab pages and records. */
    SLABS("slabs"),
    /** Every section. */
    ALL("all"),
    /** The default set of sections (same as {@code all}). */
    DEFAULT("default"),
    /** Prometheus text format of the shard. */
    PROMETHEUS("prometheus");

    private final String word;
    private final byte[] fragment;

    InfoSection(String word) {
        this.word = word;
        this.fragment = RespOut.fragment(word);
    }

    /**
     * Returns the name as sent to the server.
     *
     * @return the section name, for example {@code "memorybreakdown"}
     */
    public String word() {
        return word;
    }

    /**
     * Returns the name pre-encoded as a complete RESP bulk string ({@code $len\r\nword\r\n}).
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
