// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.CommitDelta;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.Sequencer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The commit request is built ONCE, so a retry could carry the same triple (M5.2).
 *
 * <p>⚠️ THE GUARANTEE STOPPED AT THE SEAM. {@code Sequencer}'s contract says a
 * resubmission of an already-applied {@code (podId, incarnationId, flushSeq)}
 * is answered rather than committed twice, and that "a caller that mints a
 * fresh flushSeq for the retry is submitting a different commit, and the same
 * records are committed twice" — which it still says. ⚠️ IT USED TO CLOSE WITH
 * "THIS GUARANTEE STOPS AT THIS SEAM and no production caller yet reaches it",
 * and M5.23 replaced that clause; it is quoted here as the state this commit
 * was written against, not as a sentence to go and find.
 *
 * <p>⚠️ M5 WILL MAKE ONE REACH IT — but not yet, and this commit deliberately
 * stops short. {@code DefaultIngest} wrote {@code flushSeq++} inside the
 * {@code CommitRequest} constructor call, so the request could not be resent at
 * all. It is now a named local, which is the whole change.
 *
 * <p>⚠️ NO RETRY IS ADDED HERE, AND ADDING ONE WOULD STILL BE WRONG TODAY --
 * for a NARROWER reason than when this was written. {@code Sequencer.commit}'s
 * {@code @throws} used to read "Retrying with the same (podId, incarnationId,
 * flushSeq) IS NOT YET SAFE" — quoted here as history, since M5.23 replaced
 * it: a retry to the SAME sequencer is now reconciled against the chain and
 * answered. What is still unsafe is a retry that crosses a TAKEOVER, because a
 * successor does not inherit an ambiguously-landed flush (M5.25) — and a retry
 * loop here cannot know which it is doing. A first draft of this commit added
 * an automatic retry and would have duplicated records on every lost
 * response.
 *
 * <p>⚠️ AND {@code flushSeq} STILL ADVANCES UNCONDITIONALLY. The same draft
 * advanced it only on success, which wedges the pod: {@code flushLocked} has
 * already cleared the batch and drained the accumulator by then, so the next
 * flush carries DIFFERENT bytes under the same triple — refused forever by a
 * sequencer that knows the number, and the pod never commits again until it
 * restarts. An abandoned flush burns its number, which is correct.
 */
class CommitRetryTripleTest {

    /** Records every request it is handed, and fails the first N ambiguously. */
    private static final class FlakySequencer implements Sequencer {
        private final Sequencer delegate;
        private final List<CommitRequest> seen = new ArrayList<>();
        private int failuresLeft;

        FlakySequencer(Sequencer delegate, int failuresLeft) {
            this.delegate = delegate;
            this.failuresLeft = failuresLeft;
        }

        @Override
        public CommitDelta commit(CommitRequest request) throws IOException {
            seen.add(request);
            if (failuresLeft > 0) {
                failuresLeft--;
                // ⚠️ AN IOException IS THE AMBIGUOUS CASE, not a clean failure:
                // the store may have taken the write and lost the response.
                // That is precisely when a retry must carry the same triple.
                throw new IOException("injected: the response was lost");
            }
            return delegate.commit(request);
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            for (CommitRequest r : requests) {
                seen.add(r);
            }
            if (failuresLeft > 0) {
                failuresLeft--;
                throw new IOException("injected: the response was lost");
            }
            return delegate.commitAll(requests);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        List<CommitRequest> seen() {
            return List.copyOf(seen);
        }
    }

    private static binjava.binstore.CountingBinStore store;

    private static FlakySequencer flaky(int failures) throws IOException {
        store = new binjava.binstore.CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        return new FlakySequencer(IngestTestSupport.sequencer(store, "pod1"), failures);
    }

    private static DefaultIngest ingestWith(Sequencer s) throws IOException {
        return IngestTestSupport.ingestWithSequencer(store, s);
    }

    private static List<Long> flushSeqsIn(FlakySequencer s) {
        return s.seen().stream().map(CommitRequest::flushSeq).toList();
    }

    @Test
    void theRequestIsBUILTONCESoItCouldBeResentUnchanged() throws Exception {
        FlakySequencer flaky = flaky(0);
        try (DefaultIngest ingest = ingestWith(flaky)) {
            IngestTestSupport.appendOnce(ingest, "logs", 0, 3);
        }
        CommitRequest sent = flaky.seen().get(0);
        // ⚠️ THE WHOLE REQUEST, not just its flushSeq. `IdempotencyWindow`
        // answers a replay only when the triple AND the segment key match, so a
        // resend carrying a different key is never answered -- and a test that
        // projected down to the number alone would not notice. Measured: with
        // that projection, mutating the resent key to a literal left the suite
        // green.
        assertThat(sent.segmentKey()).as("carries the segment it published").isNotBlank();
        assertThat(sent.podId()).isEqualTo("pod1");
        assertThat(sent.flushSeq()).isZero();
        assertThat(flaky.seen()).as("one attempt -- no retry is added here, because a "
                + "retry loop cannot tell a resend to the SAME sequencer, which M5.23 "
                + "answers, from one that crosses a takeover, which M5.25 does not")
                .hasSize(1);
    }

    @Test
    void anABANDONEDFlushBURNSItsNumberRatherThanWedgingThePod() throws Exception {
        // ⚠️ THE WEDGE A FIRST DRAFT INTRODUCED. Advancing only on success looks
        // right and is not: `flushLocked` has already cleared the batch and
        // drained the accumulator before the commit, so a failed flush is gone.
        // Holding the number back means the NEXT flush -- different bytes,
        // different segment -- arrives under a triple the sequencer may already
        // know, is refused because the segment key does not match, and the pod
        // never commits again until it restarts with a new incarnation.
        FlakySequencer flaky = flaky(1);
        try (DefaultIngest ingest = ingestWith(flaky)) {
            try {
                IngestTestSupport.appendOnce(ingest, "logs", 0, 2);
            } catch (Exception abandoned) {
                // the flush failed and its records are gone
            }
            IngestTestSupport.appendOnce(ingest, "logs", 0, 2);
        }
        assertThat(flushSeqsIn(flaky))
                .as("the abandoned flush burned 0; the next flush is a NEW commit at 1, "
                        + "never a resend of bytes that no longer exist")
                .containsExactly(0L, 1L);
    }

    @Test
    void aSUCCESSFULFlushAdvancesTheNumberSoTheNextIsNotAnsweredAsAReplay()
            throws Exception {
        FlakySequencer flaky = flaky(0);
        try (DefaultIngest ingest = ingestWith(flaky)) {
            IngestTestSupport.appendOnce(ingest, "logs", 0, 2);
            IngestTestSupport.appendOnce(ingest, "logs", 0, 2);
        }

        // ⚠️ THE HALF THAT STOPS THE FIX BECOMING A WORSE BUG. If the number
        // did not advance on success, the second flush -- different bytes,
        // different segment -- would arrive under a triple the sequencer has
        // already applied, be answered with the FIRST flush's offsets, and its
        // records would be lost silently. ADR-0036 calls that suppression and
        // records it as more damaging than duplication.
        assertThat(flushSeqsIn(flaky))
                .as("two distinct flushes carry two distinct numbers")
                .containsExactly(0L, 1L);
    }
}
