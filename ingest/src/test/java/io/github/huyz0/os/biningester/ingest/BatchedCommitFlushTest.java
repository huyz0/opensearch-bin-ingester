// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendOnce;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A pod can flush when the sequencer answers with a BATCHED delta (M5.60).
 *
 * <p>⚠️ THIS IS THE COMMIT-FORWARDING SHAPE, not a hypothetical one. A
 * follower's flush is committed by the LEASEHOLDER alongside other pods'
 * flushes, so the delta that comes back names several segments -- which is
 * exactly what M5's SPEC describes and what {@code Sequencer.commitAll} exists
 * to produce.
 *
 * <p>⚠️ AND {@code flushLocked} REFUSED IT OUTRIGHT. It built its offsets from
 * {@code delta.runs()}, which delegates to {@code CommitDelta.only()}, which
 * throws {@code IllegalStateException} on any delta naming more than one
 * segment. The throw is caught by {@code flushLocked}'s own handler and
 * completes every waiting append exceptionally, so the pod cannot flush at all
 * -- not a degraded flush, none.
 *
 * <p>⚠️ THE FIX IS ADR-0032's PAIRING, which {@code publishSegment} already
 * does and this path did not: each run is read against ITS OWN segment. That is
 * why the fixture puts the SAME {@link RunKey} under another pod's segment with
 * a different offset -- a fix that merely flattened every segment's runs into
 * one map would take whichever landed last and hand the caller another object's
 * offsets, which is the silent direction of ADR-0032's error.
 */
class BatchedCommitFlushTest {

    private static final RunKey LOGS_0 = new RunKey(IngestTestSupport.LOGS, 0);

    @Test
    void aBATCHEDDeltaIsFlushedAndTheOffsetsComeFromTHISPODSSegment() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Batching batching = new Batching(IngestTestSupport.sequencer(store, "pod1"));
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1", batching, new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {

            AppendResult result = appendOnce(ingest, "logs", 0, 2);

            assertThat(batching.widened)
                    .as("PREMISE: the real commit named ONE segment and the decorator made it "
                            + "THREE, so the batch is this fixture's doing and the case is not "
                            + "the single-segment one wearing a different name")
                    .isTrue();
            assertThat(result.firstOffset())
                    .as("the offsets come from THIS pod's segment -- the decoys carry the same "
                            + "stream under other pods' objects at 9999 and 8888, so picking the "
                            + "delta's FIRST segment reads 9999 and flattening every segment's "
                            + "runs into one map reads 8888")
                    .isEqualTo(0L);
            assertThat(result.lastOffset())
                    .as("and the range is the caller's own two records")
                    .isEqualTo(1L);
        }
    }

    /**
     * A sequencer that answers every commit with the real delta PLUS another
     * pod's segment, which is what a leaseholder's batched commit looks like to
     * a follower.
     *
     * <p>⚠️ ONE DECOY EITHER SIDE, BOTH CARRYING THE SAME {@link RunKey}, and
     * the sandwich is what makes both wrong answers wrong. A decoy BEFORE means
     * a fix reading {@code segments().get(0)} picks somebody else's; a decoy
     * AFTER means a fix FLATTENING every segment's runs into one map keyed by
     * {@code RunKey} overwrites the real offset with the last one it sees.
     *
     * <p>⚠️ THE FIRST DRAFT HAD ONLY THE LEADING DECOY and review MEASURED the
     * flatten mutation surviving the whole build: with the real segment last,
     * flattening puts the RIGHT offset in the map by accident. That is the
     * silent ADR-0032 direction -- the append succeeds carrying another pod's
     * offsets -- so it is the half worth buying, and it cost a second decoy.
     * The same three-segment shape M5.59 landed on the push path, for the same
     * reason.
     *
     * <p>⚠️ THE DECOY OFFSETS ARE FAR FROM ANY REAL ONE, so a wrong pairing is
     * a loud number rather than an off-by-one.
     */
    private static final class Batching implements Sequencer {
        private final Sequencer delegate;
        private boolean widened;

        Batching(Sequencer delegate) {
            this.delegate = delegate;
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            CommitDelta real = delegate.commitAll(requests);
            List<SegmentCommit> widenedSegments = new ArrayList<>();
            widenedSegments.add(new SegmentCommit("seg-other-pod-before",
                    List.of(new RunCommit(LOGS_0, 7, 9999))));
            widenedSegments.addAll(real.segments());
            widenedSegments.add(new SegmentCommit("seg-other-pod-after",
                    List.of(new RunCommit(LOGS_0, 4, 8888))));
            // ⚠️ ABOUT THE REAL DELTA, NOT THE WIDENED ONE. `widenedSegments.size() > 1`
            // is the constant `true` -- the fixture just added two segments
            // unconditionally -- so it could not fail and review measured
            // `widened = true` surviving. What is worth asserting is that the
            // REAL commit named exactly one segment, so the widening is what
            // makes the delta a batch rather than the sequencer already
            // batching behind this decorator.
            widened = real.segments().size() == 1 && widenedSegments.size() == 3;
            return new CommitDelta(real.sequence(), List.copyOf(widenedSegments));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
