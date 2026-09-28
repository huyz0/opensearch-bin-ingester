// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * How one data-segment request is charged to the indices it carried
 * (ADR-0077): by each index's run bytes, from the segment's own directory.
 */
final class SegmentCharges {

    private SegmentCharges() {
    }

    /** Each index's run bytes in {@code directory}. */
    static Map<UUID, Long> runBytes(List<RunEntry> directory) {
        Map<UUID, Long> runBytes = new HashMap<>();
        for (RunEntry entry : directory) {
            runBytes.merge(entry.key().indexId(), (long) entry.byteLen(), Long::sum);
        }
        return runBytes;
    }

    /**
     * Charges ONE data-segment GET whose whole bytes are {@code segment}, or
     * {@code null} if they were not held: split by the directory, or charged
     * whole to {@code unattributed} when there is none to read (ADR-0077
     * decision 4). Never a request of its own.
     */
    static void chargeGet(IndexCostLedger ledger, byte[] segment) {
        if (segment == null) {
            ledger.unattributed(IndexCostLedger.Charge.DATA_GET);
            return;
        }
        List<RunEntry> directory;
        try {
            directory = SegmentReader.open(segment).directory();
        } catch (IOException | RuntimeException undecodable) {
            ledger.unattributed(IndexCostLedger.Charge.DATA_GET);
            return;
        }
        ledger.apportion(IndexCostLedger.Charge.DATA_GET, runBytes(directory));
    }
}
