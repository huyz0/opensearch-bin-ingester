// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.AmbiguousAppendException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A lost reply is resent on the PRODUCTION path, and the records commit ONCE
 * (M5.52a, SPEC criterion 4).
 *
 * <p>⚠️ THE SEQUENCER HALF ALREADY EXISTED AND HAD NO CALLER, which is the
 * whole of what this row adds. {@code IdempotencyWindow.split} routes a
 * repeated triple to {@code answer} rather than to a second apply, so the
 * offsets for a resent flush are read back from the pointed delta. Nothing in
 * {@code src/main} ever resent one: {@code DefaultIngest} failed every waiter
 * and rethrew.
 *
 * <p>⚠️ WHAT THIS FIXTURE DOES NOT REACH, because an earlier version of this
 * javadoc claimed it did: {@code LocalSequencer}'s {@code
 * reconcileAmbiguousAppend}. The fake below loses the reply ABOVE the
 * sequencer, so {@code ambiguousAppend} is never set and the reconcile is never
 * entered -- review MEASURED that commenting the call out leaves both cases
 * green. That half is pinned one tier down, in {@code
 * sequencer/AmbiguousCommitRetryTest}, which is the right tier for it: it needs
 * a store that loses a conditional PUT's response, and this tier has no seam
 * for one.
 *
 * <p>⚠️ THE FAKE APPLIES AND THEN LOSES THE REPLY, which is what makes this the
 * ambiguous case rather than a clean failure. A fake that threw WITHOUT
 * delegating would leave nothing committed, so a retry would simply commit for
 * the first time and this test would pass against a broken window -- it would
 * pin "a retry happens", not "a retry is ANSWERED". Measured: with a
 * non-delegating fake, the offsets below are the same whether the window
 * answers or commits again.
 */
class AmbiguousReplyResentTest {

    /** Applies the first commit, then loses its reply exactly once. */
    private static final class LostReplySequencer implements Sequencer {
        private final Sequencer delegate;
        private final List<CommitRequest> seen = new ArrayList<>();
        private int replyLossesLeft;

        LostReplySequencer(Sequencer delegate, int replyLossesLeft) {
            this.delegate = delegate;
            this.replyLossesLeft = replyLossesLeft;
        }

        @Override
        public CommitDelta commit(CommitRequest request) throws IOException {
            seen.add(request);
            CommitDelta applied = delegate.commit(request);
            if (replyLossesLeft > 0) {
                replyLossesLeft--;
                throw new AmbiguousAppendException(applied.sequence(), applied.sequence(),
                        new IOException("injected: the response was lost"));
            }
            return applied;
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            seen.addAll(requests);
            CommitDelta applied = delegate.commitAll(requests);
            if (replyLossesLeft > 0) {
                replyLossesLeft--;
                throw new AmbiguousAppendException(applied.sequence(), applied.sequence(),
                        new IOException("injected: the response was lost"));
            }
            return applied;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        List<CommitRequest> seen() {
            return List.copyOf(seen);
        }
    }

    @Test
    void aLOSTREPLYIsResentWithTheSameTripleAndTheRecordsCommitONCE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LostReplySequencer lossy =
                new LostReplySequencer(IngestTestSupport.sequencer(store, "pod1"), 1);

        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(store, lossy)) {
            AppendResult first = IngestTestSupport.appendOnce(ingest, "logs", 0, 3);
            assertThat(first.firstOffset())
                    .as("the resend is ANSWERED from the pointed delta, so it carries the "
                            + "offsets the lost reply would have carried")
                    .isZero();

            // ⚠️ THE SECOND APPEND REACHES A DUPLICATE THE FIRST ASSERTION
            // CANNOT, and an earlier comment here justified it WRONGLY: it said
            // an answered replay and a second commit both "hand back A first
            // offset", which is false for this fixture -- the chain cannot
            // reissue offset 0, so a plain duplicate reds `isZero()` above.
            // What reaches this line is the sharper shape review measured:
            // answering on the wire while ALSO appending the replays
            // (`log.commitAll(replays)` beside the pure-retry return in
            // `LocalSequencer`), which leaves the offset above at 0 and puts
            // this one at 6.
            AppendResult second = IngestTestSupport.appendOnce(ingest, "logs", 0, 1);
            assertThat(second.firstOffset())
                    .as("only THREE records exist -- a duplicate commit would put this at 6")
                    .isEqualTo(3L);
        }

        assertThat(lossy.seen())
                .as("THREE sends: the lost first attempt, its resend, and the second "
                        + "append's own flush")
                .hasSize(3);
        assertThat(lossy.seen().stream().map(CommitRequest::flushSeq).toList())
                .as("the resend carries the SAME flushSeq -- a fresh one is a different "
                        + "commit and the records land twice")
                .containsExactly(0L, 0L, 1L);
        // ⚠️ NOTHING COMPARES THE RESEND TO THE ORIGINAL HERE, DELIBERATELY. The
        // resend passes the SAME OBJECT -- review measured both entries at one
        // `identityHashCode` -- so asserting its podId, incarnationId, flushSeq
        // and segmentKey against the original's is `x.equals(x)` four times,
        // green under any rebuild. That is the unobservability M5.32 measured
        // for M5.2's hoist. What kills a value-changing rebuild is the offset
        // pair above.
    }

    /**
     * A SECOND lost reply is not resent again -- the bound is one (M5.52a).
     *
     * <p>⚠️ WITHOUT THIS CASE "ONCE, NOT A LOOP" IS UNPINNED. With a single
     * injected loss a loop and a single resend are indistinguishable: both
     * commit once and both return the same offsets. Two losses separate them --
     * a loop swallows the second and succeeds, the bound abandons the flush.
     *
     * <p>⚠️ AND THE ABANDONED FLUSH BURNS ITS NUMBER, which is the behaviour
     * {@code CommitRetryTripleTest} already pins and this must not contradict:
     * {@code flushSeq} advanced at the construction, so the NEXT flush carries
     * the next number rather than reusing one whose bytes are gone.
     */
    @Test
    void aSECONDLostReplyIsNOTResentAgainAndTheFlushIsAbandoned() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LostReplySequencer lossy =
                new LostReplySequencer(IngestTestSupport.sequencer(store, "pod1"), 2);

        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(store, lossy)) {
            // ⚠️ `assertThatExceptionOfType`, NOT `assertThatThrownBy`, and the
            // difference shows up in the one failure this case exists for: the
            // latter raises "Expecting code to raise a throwable" from INSIDE
            // itself, before `.as(...)` attaches, so raising the bound to two
            // printed that bare line and none of the explanation. Review
            // measured it.
            assertThatExceptionOfType(AmbiguousAppendException.class)
                    .as("the resend was also ambiguous, so the flush is abandoned rather "
                            + "than retried a third time")
                    .isThrownBy(() -> IngestTestSupport.appendOnce(ingest, "logs", 0, 3));

            assertThat(lossy.seen())
                    .as("sent exactly TWICE -- the original and ONE resend; a loop would "
                            + "have sent it a third time and succeeded")
                    .hasSize(2);

            // ⚠️ AND THE ABANDONED FLUSH BURNS ITS NUMBER, which this case
            // CLAIMED and nothing pinned until review measured the gap:
            // `flushSeq--` in the catch before the rethrow was green across the
            // WHOLE build. That is the wedge `CommitRetryTripleTest` records a
            // draft of M5.2 shipping -- `flushLocked` has already cleared the
            // batch and drained the accumulator, so a reused number carries
            // DIFFERENT bytes, is refused on the segment-key mismatch, and the
            // pod never commits again until it restarts.
            IngestTestSupport.appendOnce(ingest, "logs", 0, 1);
            assertThat(lossy.seen().stream().map(CommitRequest::flushSeq).toList())
                    .as("the flush after an abandoned one carries the NEXT number, never "
                            + "the abandoned one's")
                    .containsExactly(0L, 0L, 1L);
        }
    }
}
