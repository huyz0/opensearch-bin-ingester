// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Catch-up delivers a void the chain committed, between the runs it sits
 * among, as M13.25e's v5 event (M13.25i, ADR-0082 §5).
 *
 * <p>⚠️ **THE REPLAY ANSWERED RUNS ONLY**, so a consumer catching up across a
 * takeover's void met the next run as records lost upstream, and the plugin's
 * repair over it waited for records no answer would ever carry.
 */
class CatchUpVoidsTest {

    private static final UUID REQUEST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final RunKey KEY = new RunKey(new UUID(1, 1), 7);
    private static final RunKey OTHER = new RunKey(new UUID(2, 2), 7);

    private static byte[] segment(RunKey key, int records) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (int i = 0; i < records; i++) {
            writer.add(key, new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.of(1),
                    ("r" + i).getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return writer.toByteArray(1L);
    }

    private static CommitDelta delta(long sequence, String segmentKey, RunKey key, int count,
            long first) {
        return new CommitDelta(sequence, segmentKey, List.of(new RunCommit(key, count, first)));
    }

    private static List<SubscriptionEvent> respond(List<ChainMemory.SequencedVoid> voids,
            boolean voidsComplete, CatchUpRequestFrame.Stream... streams) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put("seg/a", Body.ofBytes(segment(KEY, 2)));
        store.put("seg/b", Body.ofBytes(segment(KEY, 2)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(delta(1, "seg/a", KEY, 2, 0), delta(3, "seg/b", KEY, 2, 5)),
                true, 1, 1, 1, 3, false, voids, voidsComplete);
        var responder = new DurableCatchUpResponder(store,
                new SnapshotCommittedDeltaSource(() -> snapshot), () -> 9, new IndexCostLedger());

        List<byte[]> frames = responder.respond(new CatchUpRequestFrame(REQUEST, List.of(streams)));

        List<SubscriptionEvent> events = new ArrayList<>();
        for (int i = 0; i < frames.size() - 1; i++) {
            events.add(CatchUpEventFrame.decode(frames.get(i)).event());
        }
        assertThat(CatchUpEndFrame.decode(frames.get(frames.size() - 1)).requestId())
                .as("the end frame closes the answer").isEqualTo(REQUEST);
        return events;
    }

    /**
     * A responder over two streams: KEY's run at [10, 12) first in chain order,
     * then OTHER's runs at [0, 2) and [5, 7).
     */
    private static List<SubscriptionEvent> respondTwoStreams(
            List<ChainMemory.SequencedVoid> voids) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put("seg/a", Body.ofBytes(segment(KEY, 2)));
        store.put("seg/c", Body.ofBytes(segment(OTHER, 2)));
        store.put("seg/d", Body.ofBytes(segment(OTHER, 2)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(List.of(
                delta(1, "seg/a", KEY, 2, 10), delta(2, "seg/c", OTHER, 2, 0),
                delta(4, "seg/d", OTHER, 2, 5)),
                true, 1, 1, 1, 4, false, voids, true);
        var responder = new DurableCatchUpResponder(store,
                new SnapshotCommittedDeltaSource(() -> snapshot), () -> 9, new IndexCostLedger());
        List<byte[]> frames = responder.respond(new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 0), new CatchUpRequestFrame.Stream(OTHER, 0))));
        List<SubscriptionEvent> events = new ArrayList<>();
        for (int i = 0; i < frames.size() - 1; i++) {
            events.add(CatchUpEventFrame.decode(frames.get(i)).event());
        }
        return events;
    }

    private static List<String> sequenceOf(List<SubscriptionEvent> events, RunKey key) {
        return events.stream().filter(e -> e.key().equals(key))
                .map(e -> (e.voided() ? "void@" : "run@") + e.firstOffset()).toList();
    }

    private static ChainMemory.SequencedVoid gap(long sequence, RunKey key, long from, long to) {
        return new ChainMemory.SequencedVoid(1, sequence, new Recovery.VoidRange(key, from, to));
    }

    @Test
    void aVOIDBetweenTwoRunsIsDeliveredBetweenThem() throws Exception {
        List<SubscriptionEvent> events = respond(List.of(gap(2, KEY, 2, 5)), true,
                new CatchUpRequestFrame.Stream(KEY, 0));

        assertThat(events).extracting(SubscriptionEvent::firstOffset).containsExactly(0L, 2L, 5L);
        assertThat(events).extracting(SubscriptionEvent::voided)
                .containsExactly(false, true, false);
        assertThat(events.get(1).recordCount()).isEqualTo(3);
        assertThat(events.get(1).chainSequence()).as("the recovery's slot").isEqualTo(2);
        assertThat(events.get(1).sequencerEpoch()).isEqualTo(9);
    }

    @Test
    void aVOIDAfterTheLastRunIsDeliveredBeforeTheEnd() throws Exception {
        List<SubscriptionEvent> events = respond(List.of(gap(4, KEY, 7, 10)), true,
                new CatchUpRequestFrame.Stream(KEY, 0));

        assertThat(events).extracting(SubscriptionEvent::firstOffset).containsExactly(0L, 5L, 7L);
        assertThat(events.get(2).voided()).isTrue();
    }

    @Test
    void aVOIDIsCutAtTheResumeOffsetAndDroppedWhollyBeforeIt() throws Exception {
        List<SubscriptionEvent> cut = respond(List.of(gap(2, KEY, 2, 5)), true,
                new CatchUpRequestFrame.Stream(KEY, 3));
        assertThat(cut).extracting(SubscriptionEvent::firstOffset).containsExactly(3L, 5L);
        assertThat(cut.get(0).recordCount()).as("[3, 5)").isEqualTo(2);

        List<SubscriptionEvent> past = respond(List.of(gap(2, KEY, 2, 5)), true,
                new CatchUpRequestFrame.Stream(KEY, 5));
        assertThat(past).extracting(SubscriptionEvent::voided).containsExactly(false);
    }

    @Test
    void anUNREQUESTEDStreamsVoidIsNotSent() throws Exception {
        List<SubscriptionEvent> events = respond(List.of(gap(2, OTHER, 0, 3)), true,
                new CatchUpRequestFrame.Stream(KEY, 0));

        assertThat(events).extracting(SubscriptionEvent::voided).containsOnly(false);
    }

    @Test
    void TWOVoidsOfOneStreamArriveInOffsetOrder() throws Exception {
        // ⚠️ M13.25i review T1 (mutant M4): a recovery's voids reversed reach
        // the plugin as records skipped.
        List<SubscriptionEvent> events = respond(
                List.of(gap(2, KEY, 2, 3), gap(2, KEY, 3, 5)), true,
                new CatchUpRequestFrame.Stream(KEY, 0));

        assertThat(sequenceOf(events, KEY))
                .containsExactly("run@0", "void@2", "void@3", "run@5");
    }

    @Test
    void EACHStreamsVoidsWaitForThatStreamsRuns() throws Exception {
        // ⚠️ M13.25i review T1 (mutant M5): written before KEY's run at 10,
        // OTHER's void would precede OTHER's own run at 0 -- records skipped.
        List<SubscriptionEvent> events = respondTwoStreams(List.of(gap(3, OTHER, 2, 5)));

        assertThat(sequenceOf(events, KEY)).containsExactly("run@10");
        assertThat(sequenceOf(events, OTHER)).containsExactly("run@0", "void@2", "run@5");
    }

    @Test
    void voidsForAnswersEachStreamInOffsetOrder() {
        // ⚠️ M13.25i review T3: its javadoc promises the order the responder needs.
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(List.of(), true, 1, 1, 1, 1,
                false, List.of(gap(4, KEY, 10, 12), gap(2, OTHER, 0, 1), gap(2, KEY, 2, 5)), true);

        List<CommittedDeltaSource.ReplayVoid> voids =
                new SnapshotCommittedDeltaSource(() -> snapshot).voidsFor(List.of(
                        new CommittedDeltaSource.ReplayRequest(KEY, -1),
                        new CommittedDeltaSource.ReplayRequest(OTHER, -1)));

        assertThat(voids.stream().filter(v -> v.key().equals(KEY))
                .map(CommittedDeltaSource.ReplayVoid::firstOffset).toList())
                .containsExactly(2L, 10L);
    }

    @Test
    void aVOIDWiderThanAnEventCanCarryIsRefusedNotSplit() {
        // ⚠️ M13.25i review P3: split, it was billions of frames unbudgeted.
        assertThatThrownBy(() -> respond(List.of(gap(4, KEY, 7, 7L + Integer.MAX_VALUE + 1)),
                true, new CatchUpRequestFrame.Stream(KEY, 0)))
                .isInstanceOf(DurableCatchUpResponder.ResponseTooLargeException.class);
    }

    @Test
    void voidsTheChainCannotVouchForAreRefused() {
        // ⚠️ AS FOR AN INCOMPLETE CHAIN: a void evicted by the bound would reach
        // the consumer as an unexplained jump.
        assertThatThrownBy(() -> respond(List.of(gap(2, KEY, 2, 5)), false,
                new CatchUpRequestFrame.Stream(KEY, 0)))
                .isInstanceOf(IllegalStateException.class);
    }
}
