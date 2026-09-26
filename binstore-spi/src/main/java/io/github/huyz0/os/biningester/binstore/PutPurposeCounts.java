// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

/**
 * PUT requests attributed to the bounded object-key purposes used by the M9 cost harness.
 *
 * @param dataPuts segment-data writes
 * @param commitPuts commit-log delta and seal writes
 * @param checkpointPuts checkpoint objects and the latest-checkpoint pointer
 * @param leasePuts lease acquisition, renewal, and release writes
 * @param otherPuts writes outside those four key classes
 */
public record PutPurposeCounts(long dataPuts, long commitPuts, long checkpointPuts,
        long leasePuts, long otherPuts) {

    /** The purpose counters must partition the aggregate PUT counter. */
    public long total() {
        return dataPuts + commitPuts + checkpointPuts + leasePuts + otherPuts;
    }
}
