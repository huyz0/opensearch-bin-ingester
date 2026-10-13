// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * A takeover's recovery delivered as a flush's is (M13.33w; ADR-0081 §5 step
 * 6): once its entry commits, each of its segments pushed to this pod's
 * subscribers at the offsets committed, under the entry's chain sequence and
 * the term's epoch, and signalled durable -- each once, and a segment no
 * entry named never.
 */
class DefaultIngestRecoveryDeliveryTest {

    private static final RunKey S = new RunKey(IngestTestSupport.LOGS, 0);

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1});
    }

    private static DefaultIngest ingest(MemoryBinStore store, SubscriptionHub hub,
            List<String> durable) throws Exception {
        return new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), hub, Clock.systemUTC(),
                index -> IngestTestSupport.LOGS, durable::add, new IndexCostLedger());
    }

    @Test
    void eachSEGMENTOfACommittedRecoveryIsPushedOnceAtItsOffsetsAndSignalledDurable()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        List<String> durable = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(S, SubscriptionHub.assembling(pushed::add));
                DefaultIngest ingest = ingest(store, hub, durable)) {
            String first = ingest.publishRecovery(Map.of(S, List.of(record("a"), record("b"))));
            String second = ingest.publishRecovery(Map.of(S, List.of(record("c"))));
            Recovery recovery = new Recovery(7, List.of(
                    new SegmentCommit(first, List.of(new RunCommit(S, 2, 40))),
                    new SegmentCommit(second, List.of(new RunCommit(S, 1, 50)))), List.of());

            ingest.recovered(recovery, 3);
            ingest.recovered(recovery, 3);
            // ⚠️ A LATER RECOVERY AS A MARKER: pushed in order, so once it is
            // seen nothing of the first is still on its way.
            String marker = ingest.publishRecovery(Map.of(S, List.of(record("d"))));
            ingest.recovered(new Recovery(8, List.of(new SegmentCommit(marker,
                    List.of(new RunCommit(S, 1, 51)))), List.of()), 3);

            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (pushed.stream().noneMatch(p -> p.firstOffset() == 51)) {
                assertThat(System.nanoTime()).as("the marker pushed").isLessThan(deadline);
                Thread.onSpinWait();
            }
            assertThat(pushed).as("each once, in order").extracting(
                    SubscriptionHub.Push::segmentKey, SubscriptionHub.Push::firstOffset,
                    SubscriptionHub.Push::recordCount, SubscriptionHub.Push::sequencerEpoch,
                    SubscriptionHub.Push::chainSequence)
                    .containsExactly(org.assertj.core.groups.Tuple.tuple(first, 40L, 2, 3L, 7L),
                            org.assertj.core.groups.Tuple.tuple(second, 50L, 1, 3L, 7L),
                            org.assertj.core.groups.Tuple.tuple(marker, 51L, 1, 3L, 8L));
            assertThat(durable).containsExactly(first, second, marker);
        }
    }

    @Test
    void aSEGMENTNoEntryNamedIsNeverDelivered() throws Exception {
        // a recovery whose entry never landed: its segment dropped by the next one's.
        MemoryBinStore store = new MemoryBinStore();
        List<String> durable = new CopyOnWriteArrayList<>();
        try (DefaultIngest ingest = ingest(store, new SubscriptionHub(), durable)) {
            String orphan = ingest.publishRecovery(Map.of(S, List.of(record("a"))));
            String landed = ingest.publishRecovery(Map.of(S, List.of(record("a"))));

            ingest.recovered(new Recovery(7, List.of(new SegmentCommit(landed,
                    List.of(new RunCommit(S, 1, 40)))), List.of()), 3);
            ingest.recovered(new Recovery(8, List.of(new SegmentCommit(orphan,
                    List.of(new RunCommit(S, 1, 41)))), List.of()), 3);
            ingest.recovered(new Recovery(9, List.of(new SegmentCommit("other/key",
                    List.of(new RunCommit(S, 1, 42)))), List.of()), 3);

            assertThat(durable).containsExactly(landed);
        }
    }
}
