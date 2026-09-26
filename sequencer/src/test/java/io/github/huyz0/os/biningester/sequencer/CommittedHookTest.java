// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The leaseholder's committed-delta hook fires once per delta it makes
 * durable, and never for one it only replays (M10.19, ADR-0075).
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommittedHookTest {

    private record Fired(long epoch, long sequence) {
    }

    private static LocalSequencer leaderOver(BinStore store, String pod) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, pod), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    @Test
    void eachFreshCommitFiresOnceWithTheTermsEpochAndAnAnsweredReplayDoesNot() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = leaderOver(store, "poda");
        List<Fired> fired = new CopyOnWriteArrayList<>();
        try {
            sequencer.onCommitted((delta, epoch) -> fired.add(new Fired(epoch, delta.sequence())));
            CommitRequest first = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            CommitDelta one = sequencer.commitAll(List.of(first));
            CommitDelta two = sequencer.commitAll(List.of(
                    new CommitRequest("podb", "i1", 0, "seg/1", counts(2))));
            sequencer.commitAll(List.of(first)); // answered from the window

            assertThat(fired).containsExactly(
                    new Fired(sequencer.epoch(), one.sequence()),
                    new Fired(sequencer.epoch(), two.sequence()));
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anAmbiguousAppendThatLandedFiresOnceWhenItIsReconciled() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_ABSENT, false, key -> armed.get());
        LocalSequencer sequencer = leaderOver(store, "poda");
        List<Fired> fired = new CopyOnWriteArrayList<>();
        try {
            sequencer.onCommitted((delta, epoch) -> fired.add(new Fired(epoch, delta.sequence())));
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .isInstanceOf(AmbiguousAppendException.class);
            assertThat(fired).as("the outcome is not known yet").isEmpty();

            CommitDelta retried = sequencer.commitAll(List.of(flush));

            assertThat(fired).as("the landed delta, reported by the reconciliation")
                    .containsExactly(new Fired(sequencer.epoch(), retried.sequence()));
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aLandedAppendMetAgainAsATakenSlotIsNotReportedTwice() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_ABSENT, false, key -> armed.get());
        LocalSequencer sequencer = leaderOver(store, "poda");
        List<Fired> fired = new CopyOnWriteArrayList<>();
        try {
            sequencer.onCommitted((delta, epoch) -> fired.add(new Fired(epoch, delta.sequence())));
            armed.set(true);
            CommitRequest landed = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            assertThatThrownBy(() -> sequencer.commitAll(List.of(landed)))
                    .isInstanceOf(AmbiguousAppendException.class);
            armed.set(false);

            // ⚠️ The log still believes the landed slot is free, so this append
            // meets it TAKEN and folds it in before writing the next one. That
            // fold is a read of a delta already reported, not a new one.
            CommitDelta mixed = sequencer.commitAll(List.of(landed,
                    new CommitRequest("podb", "i1", 0, "seg/1", counts(2))));

            assertThat(fired).extracting(Fired::sequence)
                    .as("each durable delta once, in chain order: the landed one first")
                    .containsExactly(mixed.sequence() - 1, mixed.sequence());
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anAmbiguousAppendThatNeverLandedFiresOnlyForTheRetrysOwnAppend() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LOST,
                AmbiguousPutStore.Target.PUT_IF_ABSENT, false, key -> armed.get());
        LocalSequencer sequencer = leaderOver(store, "poda");
        List<Fired> fired = new CopyOnWriteArrayList<>();
        try {
            sequencer.onCommitted((delta, epoch) -> fired.add(new Fired(epoch, delta.sequence())));
            armed.set(true);
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            assertThatThrownBy(() -> sequencer.commitAll(List.of(flush)))
                    .isInstanceOf(AmbiguousAppendException.class);
            armed.set(false);

            CommitDelta retried = sequencer.commitAll(List.of(flush));

            assertThat(fired).containsExactly(new Fired(sequencer.epoch(), retried.sequence()));
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ The listener can only be installed on a started term, after its
     * replay; what this pins is that nothing replayed is reported later, and
     * that the hook is the one the term's own commits reach.
     */
    @Test
    void aTakeoverReplaysTheChainWithoutFiringAndFiresForItsOwnCommits() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = leaderOver(store, "poda");
        try {
            first.commitAll(List.of(new CommitRequest("poda", "i1", 0, "seg/0", counts(3))));
            first.commitAll(List.of(new CommitRequest("poda", "i1", 1, "seg/1", counts(3))));
        } finally {
            first.close();
        }

        LocalSequencer successor = leaderOver(store, "podc");
        List<Fired> fired = new CopyOnWriteArrayList<>();
        try {
            successor.onCommitted((delta, epoch) -> fired.add(new Fired(epoch, delta.sequence())));
            CommitDelta own = successor.commitAll(List.of(
                    new CommitRequest("podc", "i1", 0, "seg/2", counts(1))));

            assertThat(successor.epoch()).as("the premise: a new term").isGreaterThan(1);
            assertThat(fired).as("nothing from the replay, one for its own commit")
                    .containsExactly(new Fired(successor.epoch(), own.sequence()));
        } finally {
            successor.close();
        }
    }

    @Test
    void aListenerThatThrowsDoesNotFailTheDurableCommit() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = leaderOver(store, "poda");
        try {
            sequencer.onCommitted((delta, epoch) -> {
                throw new IllegalStateException("the queue behind the listener broke");
            });
            CommitDelta delta = sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3))));

            assertThat(delta.segments()).hasSize(1);
        } finally {
            sequencer.close();
        }
    }
}
