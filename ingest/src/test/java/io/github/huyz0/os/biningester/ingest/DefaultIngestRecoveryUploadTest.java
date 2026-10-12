// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * A takeover's recovery segments are uploaded by the ingest path's own
 * publisher (M13.33p; ADR-0081 §5 step 6): its key carries the pod's id and
 * the publisher's sequence, written with a plain PUT, so a second publisher
 * of the same pod could write a flush's key and overwrite it.
 */
class DefaultIngestRecoveryUploadTest {

    private static final RunKey S = new RunKey(IngestTestSupport.LOGS, 0);

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1, 2});
    }

    private static DefaultIngest ingest(MemoryBinStore store) throws Exception {
        // ⚠️ ONE INSTANT, so two segments' keys differ only by the sequence.
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(5_000_000L), ZoneOffset.UTC);
        return new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(), fixed,
                index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger());
    }

    @Test
    void aRECOVERYsRunsAreUploadedAsOneSegment() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = ingest(store)) {
            String key = ingest.publishRecovery(Map.of(S, List.of(record("a"), record("b"))));

            try (InputStream in = store.get(key)) {
                var directory = SegmentReader.open(in.readAllBytes()).directory();
                assertThat(directory).singleElement().satisfies(e -> {
                    assertThat(e.key()).isEqualTo(S);
                    assertThat(e.recordCount()).isEqualTo(2);
                });
            }
        }
    }

    @Test
    void twoUPLOADSAtOneInstantTakeTwoKeys() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = ingest(store)) {
            String first = ingest.publishRecovery(Map.of(S, List.of(record("a"))));
            String second = ingest.publishRecovery(Map.of(S, List.of(record("b"))));

            assertThat(second).as("the publisher's sequence, shared").isNotEqualTo(first);
        }
    }
}
