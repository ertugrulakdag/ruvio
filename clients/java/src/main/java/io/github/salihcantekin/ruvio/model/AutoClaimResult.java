// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import java.util.List;

/**
 * Reply of {@code XAUTOCLAIM}: where to continue and what was claimed.
 *
 * @param <K> the field name type
 * @param <V> the field value type
 */
public final class AutoClaimResult<K, V> {
    private final String nextId;
    private final List<StreamEntry<K, V>> entries;
    private final List<String> claimedIds;
    private final List<String> deletedIds;

    /**
     * Creates a result.
     *
     * @param nextId the id to pass as the next start ("0-0" when the scan reached the end)
     * @param entries the claimed entries (empty with {@code JUSTID})
     * @param claimedIds the ids of the claimed entries
     * @param deletedIds the ids that were pending but no longer exist in the stream
     */
    public AutoClaimResult(String nextId, List<StreamEntry<K, V>> entries, List<String> claimedIds, List<String> deletedIds) {
        this.nextId = nextId;
        this.entries = entries;
        this.claimedIds = claimedIds;
        this.deletedIds = deletedIds;
    }

    /**
     * Returns the id to pass as the next {@code start} to continue the claim loop.
     *
     * @return the next start id, or {@code 0-0} when every pending entry was examined
     */
    public String nextId() {
        return nextId;
    }

    /**
     * Returns the claimed entries.
     *
     * @return the entries with their fields; empty with {@code JUSTID}
     */
    public List<StreamEntry<K, V>> entries() {
        return entries;
    }

    /**
     * Returns the ids of the claimed entries.
     *
     * @return the ids (always filled)
     */
    public List<String> claimedIds() {
        return claimedIds;
    }

    /**
     * Returns pending ids whose entries were deleted from the stream; the server removes them from the group's pending list.
     *
     * @return the deleted ids
     */
    public List<String> deletedIds() {
        return deletedIds;
    }
}
