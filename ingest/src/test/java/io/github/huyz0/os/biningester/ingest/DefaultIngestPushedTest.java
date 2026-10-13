// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * A PUBLISHed fast entry delivered to this pod's subscribers before its upload
 * (M13.30j; ADR-0081 §6, ADR-0082 §6): at its offsets, under its fast key,
 * its records inline, with no chain epoch or sequence -- and never by a
 * {@code direct} grant, since its key names no object.
 */
class DefaultIngestPushedTest {

    private static final RunKey S = new RunKey(IngestTestSupport.LOGS, 0);

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1});
    }

    private static FastJournalRecord.Entry entry(long firstOffset, String... ids) {
        return new FastJournalRecord.Entry(7, S, firstOffset, 1, 3,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), firstOffset),
                java.util.Arrays.stream(ids).map(DefaultIngestPushedTest::record).toList());
    }

    @Test
    void aPUSHEDEntryReachesTheSubscribersAtItsOffsetsUnderItsFastKey() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        List<byte[]> bytes = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(S, SubscriptionHub.assembling(p -> {
                    pushed.add(p);
                    bytes.add(p.segment());
                }));
                DefaultIngest ingest = new DefaultIngest(
                        IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                        store, IngestTestSupport.PREFIX, "pod1",
                        IngestTestSupport.sequencer(store, "pod1"), hub, Clock.systemUTC(),
                        index -> IngestTestSupport.LOGS, key -> { }, new IndexCostLedger())) {

            ingest.deliverPushed(entry(40, "a", "b"));

            assertThat(pushed).singleElement().satisfies(p -> {
                assertThat(p.segmentKey()).isEqualTo(
                        FastKey.of(IngestTestSupport.PREFIX, 7, 3, S, 40));
                assertThat(p.firstOffset()).isEqualTo(40);
                assertThat(p.recordCount()).isEqualTo(2);
                assertThat(p.via()).isEqualTo(FetchMode.INLINE);
                assertThat(p.sequencerEpoch()).isEqualTo(SubscriptionHub.EPOCH_UNKNOWN);
                assertThat(p.chainSequence())
                        .isEqualTo(SubscriptionHub.CHAIN_SEQUENCE_UNKNOWN);
            });
            SegmentReader reader = SegmentReader.open(bytes.get(0));
            assertThat(reader.read(reader.find(S).orElseThrow()))
                    .extracting(SegmentRecord::id).containsExactly("a", "b");
        }
    }

    @Test
    void aPUSHEDEntrysCopyIsKeptUnderItsFastKey() throws Exception {
        // ⚠️ M13.30l: for a subscriber in another zone, pushed the key alone.
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, key -> { },
                new IndexCostLedger())) {
            ingest.deliverPushed(entry(40, "a", "b"));

            byte[] copy = ingest.pushedCopies()
                    .copy(FastKey.of(IngestTestSupport.PREFIX, 7, 3, S, 40)).orElseThrow();
            SegmentReader reader = SegmentReader.open(copy);
            assertThat(reader.read(reader.find(S).orElseThrow()))
                    .extracting(SegmentRecord::id).containsExactly("a", "b");
        }
    }

    @Test
    void aPUSHEDEntrysServingNeverPresigns() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, key -> { },
                new IndexCostLedger())) {
            SegmentServing base = ingest.serving();
            Capabilities c = base.capabilities();
            SegmentServing presigning = new SegmentServing(base.policy(), new Capabilities(
                    c.conditionalWrites(), c.batchDelete(), true, c.maxKeyBytes(),
                    c.minPartSize(), c.costs()), base.proxy());

            assertThat(PushedEntries.withoutDirect(presigning).capabilities().presignedUrls())
                    .isFalse();
        }
    }
}
