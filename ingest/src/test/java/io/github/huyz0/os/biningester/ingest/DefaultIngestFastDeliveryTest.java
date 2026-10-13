// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A fast upload's segment delivered as a flush's is (M13.30a; ADR-0081 §6):
 * once its delta is committed, pushed to this pod's subscribers at its
 * committed offsets and signalled durable to its peers.
 */
class DefaultIngestFastDeliveryTest {

    private static final RunKey S = new RunKey(IngestTestSupport.LOGS, 0);

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1});
    }

    @Test
    void aCOMMITTEDFastSegmentIsPushedAtItsOffsetsAndSignalledDurable() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        AtomicReference<String> durable = new AtomicReference<>();
        try (var sub = hub.subscribe(S, SubscriptionHub.assembling(pushed::add));
                DefaultIngest ingest = new DefaultIngest(
                        IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                        store, IngestTestSupport.PREFIX, "pod1",
                        IngestTestSupport.sequencer(store, "pod1"), hub, Clock.systemUTC(),
                        index -> IngestTestSupport.LOGS, durable::set, new IndexCostLedger())) {
            String key = ingest.publishFast(Map.of(S, List.of(record("a"), record("b"))));
            CommitDelta delta = new CommitDelta(5, List.of(new SegmentCommit(key,
                    List.of(new RunCommit(S, 2, 40)))));

            ingest.delivered(key, delta, 3);
            ingest.delivered(key, delta, 3);

            IngestTestSupport.awaitPush(pushed);
            assertThat(pushed).as("delivered once").singleElement().satisfies(p -> {
                assertThat(p.firstOffset()).isEqualTo(40);
                assertThat(p.recordCount()).isEqualTo(2);
                assertThat(p.sequencerEpoch()).isEqualTo(3);
            });
            assertThat(durable.get()).isEqualTo(key);
        }
    }

    @Test
    void aSEGMENTARetryReplacedIsNeverDelivered() throws Exception {
        // ⚠️ its review round 1, P1 and T1: one segment held, never a queue.
        MemoryBinStore store = new MemoryBinStore();
        AtomicReference<String> durable = new AtomicReference<>();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, durable::set,
                new IndexCostLedger())) {
            String first = ingest.publishFast(Map.of(S, List.of(record("a"))));
            ingest.publishFast(Map.of(S, List.of(record("a"))));

            ingest.delivered(first, new CommitDelta(5, List.of(new SegmentCommit(first,
                    List.of(new RunCommit(S, 1, 40))))), 3);

            assertThat(durable.get()).isNull();
        }
    }

    @Test
    void aSEGMENTThisPathDidNotPublishIsNotDelivered() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        AtomicReference<String> durable = new AtomicReference<>();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, durable::set,
                new IndexCostLedger())) {
            ingest.delivered("other/key", new CommitDelta(5, List.of(new SegmentCommit(
                    "other/key", List.of(new RunCommit(S, 2, 40))))), 3);

            assertThat(durable.get()).isNull();
        }
    }
}
