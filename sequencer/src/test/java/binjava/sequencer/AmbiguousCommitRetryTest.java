// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static binjava.sequencer.DedupFixtures.firstOffsetOf;
import static binjava.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A retry after an AMBIGUOUS append is answered from the chain, not applied
 * twice (M5.23).
 *
 * <p>⚠️ THE HALF {@code SequencerDedupTest} COULD NOT REACH. That suite replays
 * a triple the sequencer WATCHED land, so its window already holds it. Here the
 * append landed while its response was lost -- never recorded as applied,
 * because the record happens after the write RETURNS -- so the window says
 * "fresh" about records that are already committed, and until this row the
 * retry gave them a second set of offsets. That is I2.
 *
 * <p>⚠️ THE SEQUENCER RECONCILES, NOT THE LOG. What the log reports is the SLOT
 * it wrote to; what turns that into an answer is reading the slot and seeding
 * the idempotency window from every flush the landed delta carries. Seeding
 * from the RESUBMITTED requests instead is not enough and the batch tests below
 * are why: the delta that landed carries the whole batch, while a retry
 * regrouped by {@code BatchingSequencer} may carry a subset, a superset, or
 * both at once.
 *
 * <p>⚠️ THE ASSERTION IS ON THE CHAIN, never on the offsets alone.
 * {@code ChainReplay} folds with {@code Math::max}, so a duplicate appended at
 * its ORIGINAL offsets moves no stream's next offset and every offset assertion
 * still passes.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AmbiguousCommitRetryTest {

    /**
     * A store whose FIRST delta append loses its response.
     *
     * <p>⚠️ ARMED AFTER THE START, because a leader's cold start writes the
     * lease and the chain's slot-0 CONTINUE with the same primitive, and
     * neither of those is the append under test.
     *
     * @param thenRefuseNextRead makes the store still unreachable for the read
     *     that would reconcile the append -- the COMPOUND failure, and the one
     *     that decides whether the mark survives to be used by a later commit
     */
    private static BinStore ambiguousOnFirstDeltaAppend(BinStore backing,
            AmbiguousPutStore.Mode mode, AtomicBoolean armed, boolean thenRefuseNextRead) {
        return new AmbiguousPutStore(backing, mode, AmbiguousPutStore.Target.PUT_IF_ABSENT,
                thenRefuseNextRead, key -> armed.get());
    }

    private static BinStore ambiguousOnFirstDeltaAppend(BinStore backing,
            AmbiguousPutStore.Mode mode, AtomicBoolean armed) {
        return ambiguousOnFirstDeltaAppend(backing, mode, armed, false);
    }

    /**
     * ⚠️ THE RENEWER IS PARKED on a latch, not on a duration. The renewer issues
     * a counted {@code putIfMatch} into the same store the request counts below
     * are read from, and a duration long enough today is a latent flake rather
     * than a safe one -- {@code BoundedRecoveryFixture.noRenew} says so, and
     * this uses that one rather than growing a second idiom for it.
     */
    private static LocalSequencer leaderOver(BinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    @Test
    void aRetryAfterAnAmbiguousAppendGetsTheOffsetsThatALREADYApply() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store =
                ambiguousOnFirstDeltaAppend(backing, AmbiguousPutStore.Mode.LANDED, armed);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .as("the append landed and the response did not")
                    .isInstanceOf(AmbiguousAppendException.class);

            // ⚠️ THE SAME TRIPLE, which is what M5.2 made a caller able to do.
            CommitDelta retried = sequencer.commitAll(List.of(flush));

            assertThat(firstOffsetOf(retried, "seg/0"))
                    .as("the offsets already assigned to these records")
                    .isZero();
            assertThat(deltasCarrying(backing, "seg/0"))
                    .as("ONE delta carries them -- the retry appended nothing")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void everyFlushInAnAmbiguousBATCHIsAnsweredEvenWhenTheyRetrySEPARATELY() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store =
                ambiguousOnFirstDeltaAppend(backing, AmbiguousPutStore.Mode.LANDED, armed);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest first = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            CommitRequest second = new CommitRequest("podb", "i1", 0, "seg/1", counts(2));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(first, second)))
                    .isInstanceOf(AmbiguousAppendException.class);

            // ⚠️ THE BATCH DOES NOT REFORM. `BatchingSequencer` groups whatever
            // is in flight when a window closes, and the two producers behind
            // these flushes retry independently -- so the delta that landed
            // carries BOTH while each retry submits ONE. Reconciling from the
            // resubmitted requests answers whichever retried first and lets the
            // other commit a second time.
            CommitDelta retriedFirst = sequencer.commitAll(List.of(first));
            CommitDelta retriedSecond = sequencer.commitAll(List.of(second));

            assertThat(firstOffsetOf(retriedFirst, "seg/0")).isZero();
            assertThat(firstOffsetOf(retriedSecond, "seg/1"))
                    .as("the offsets the LANDED batch gave it, behind the first flush's runs")
                    .isEqualTo(3);
            assertThat(deltasCarrying(backing, "seg/0"))
                    .as("ONE delta for the flush that retried first")
                    .isEqualTo(1);
            assertThat(deltasCarrying(backing, "seg/1"))
                    .as("and ONE for the flush that merely rode in its batch")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aRetryBATCHEDWithANewFlushCommitsONLYTheNewOne() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store =
                ambiguousOnFirstDeltaAppend(backing, AmbiguousPutStore.Mode.LANDED, armed);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest landed = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(landed)))
                    .isInstanceOf(AmbiguousAppendException.class);

            // ⚠️ THE OTHER DIRECTION OF THE SAME REGROUPING, and the one a
            // batch-shaped match gets WRONG: the retry is a strict SUPERSET of
            // what landed, so a reconciliation asking "does the landed delta
            // carry this whole batch" answers no, appends the lot, and commits
            // `landed` a second time at a second set of offsets.
            CommitRequest fresh = new CommitRequest("podb", "i1", 0, "seg/1", counts(2));
            CommitDelta mixed = sequencer.commitAll(List.of(landed, fresh));

            assertThat(firstOffsetOf(mixed, "seg/0"))
                    .as("the replayed flush keeps the offsets it already has")
                    .isZero();
            assertThat(firstOffsetOf(mixed, "seg/1"))
                    .as("and the genuinely new flush is committed after them")
                    .isEqualTo(3);
            assertThat(deltasCarrying(backing, "seg/0"))
                    .as("ONE delta carries the replayed flush -- it was NOT re-appended")
                    .isEqualTo(1);
            assertThat(deltasCarrying(backing, "seg/1"))
                    .as("and the new flush is committed exactly once")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anAmbiguousAppendTheStoreNEVERSAWIsCommittedByTheRetryExactlyOnce() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store =
                ambiguousOnFirstDeltaAppend(backing, AmbiguousPutStore.Mode.LOST, armed);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .isInstanceOf(AmbiguousAppendException.class);

            // ⚠️ THE HARMLESS HALF, AND IT MUST NOT WEDGE. The slot named by the
            // ambiguity is EMPTY, and a reconciliation that cannot tell an empty
            // slot from an unreachable store refuses every commit from here on:
            // the slot stays empty precisely because nothing may commit.
            CommitDelta retried = sequencer.commitAll(List.of(flush));

            assertThat(firstOffsetOf(retried, "seg/0"))
                    .as("committed for the first time, at the first offsets")
                    .isZero();
            assertThat(deltasCarrying(backing, "seg/0"))
                    .as("exactly once -- not suppressed as a replay of nothing")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void reconcilingAnAmbiguousAppendReadsTheONESlotItNamesAndReadsItONCE() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CountingBinStore meter = new CountingBinStore(backing);
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store =
                ambiguousOnFirstDeltaAppend(meter, AmbiguousPutStore.Mode.LANDED, armed);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .isInstanceOf(AmbiguousAppendException.class);

            // ⚠️ COUNTED ACROSS THE RETRY ALONE, so the cold start's own
            // requests are not attributed to reconciliation.
            long listsBefore = meter.counts().lists();
            long putsBefore = meter.counts().puts();
            long statsBefore = meter.counts().stats();
            long getsBefore = meter.counts().gets();
            sequencer.commitAll(List.of(flush));

            assertThat(meter.counts().lists() - listsBefore)
                    .as("NO LIST -- the slot is computed from (epoch, sequence), never searched"
                            + " for (cost.md R2)")
                    .isZero();
            assertThat(meter.counts().puts() - putsBefore)
                    .as("and NO write: an answered retry appends nothing")
                    .isZero();
            assertThat(meter.counts().stats() - statsBefore)
                    .as("ONE stat, on the ONE slot named -- a probe of neighbouring slots is a"
                            + " scan wearing a computed address's clothes")
                    .isEqualTo(1);
            assertThat(meter.counts().gets() - getsBefore)
                    .as("TWO gets: the slot being reconciled, and the delta the answer comes"
                            + " from -- both addressed, neither searched for")
                    .isEqualTo(2);

            // ⚠️ AND IT HAPPENS ONCE. A mark that is never cleared re-reads the
            // same slot on EVERY commit for the life of the process, which is a
            // read rate scaling with the commit rate -- with flushes and so with
            // records, which is non-negotiable 6.
            long statsAfterRetry = meter.counts().stats();
            long getsAfterRetry = meter.counts().gets();
            sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 1, "seg/1", counts(2))));

            assertThat(meter.counts().stats() - statsAfterRetry)
                    .as("the reconciliation is SPENT -- the next commit stats nothing")
                    .isZero();
            // ⚠️ ONE GET, AND IT IS NOT A RECONCILIATION. The ambiguous append
            // never applied, so the log still believes its slot is free; the
            // next real commit loses that slot to its own landed bytes, reads
            // them once to fold their offsets in, and moves on. That is the
            // ordinary lost-race path and it happens ONCE, not per commit --
            // which is the whole difference from a mark that is never cleared.
            assertThat(meter.counts().gets() - getsAfterRetry)
                    .as("and reads the slot it lost exactly once, never the reconciled one again")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aMarkSurvivesAReconcilingReadThatFAILSAndAnswersTheCommitAfterIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        // ⚠️ THE COMPOUND FAILURE: the same outage that lost the append's
        // response is still there when the next commit tries to look at the
        // slot. This is the case that decides whether the mark is state or
        // decoration -- dropped, the commit AFTER this one sees no mark, calls
        // an already-durable flush fresh, and appends it a second time.
        BinStore store =
                ambiguousOnFirstDeltaAppend(backing, AmbiguousPutStore.Mode.LANDED, armed, true);
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .isInstanceOf(AmbiguousAppendException.class);
            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .as("the reconciling read failed, so the outcome is STILL unknown")
                    .isInstanceOf(IOException.class);

            CommitDelta retried = sequencer.commitAll(List.of(flush));

            assertThat(firstOffsetOf(retried, "seg/0"))
                    .as("the mark outlived the failed read and answered the third attempt")
                    .isZero();
            assertThat(deltasCarrying(backing, "seg/0"))
                    .as("ONE delta -- neither retry appended anything")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }
}
