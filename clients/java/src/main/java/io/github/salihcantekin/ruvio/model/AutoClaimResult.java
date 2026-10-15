// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.List;

/** XAUTOCLAIM reply: the next start id ("0-0" when done), claimed entries (or ids with JUSTID), deleted ids. */
public final class AutoClaimResult<K, V> {
    private final String nextId;
    private final List<StreamEntry<K, V>> entries;
    private final List<String> claimedIds;
    private final List<String> deletedIds;

    public AutoClaimResult(String nextId, List<StreamEntry<K, V>> entries, List<String> claimedIds, List<String> deletedIds) {
        this.nextId = nextId;
        this.entries = entries;
        this.claimedIds = claimedIds;
        this.deletedIds = deletedIds;
    }

    public String nextId() {
        return nextId;
    }

    /** Claimed entries; empty with JUSTID. */
    public List<StreamEntry<K, V>> entries() {
        return entries;
    }

    /** Claimed ids (always filled). */
    public List<String> claimedIds() {
        return claimedIds;
    }

    public List<String> deletedIds() {
        return deletedIds;
    }
}
