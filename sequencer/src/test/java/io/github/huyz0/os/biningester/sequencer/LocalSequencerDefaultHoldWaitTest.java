// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A held default-path commit waits for its stream's decision outside the
 * sequencer's monitor, then commits (M13.33n review round 1, P1); a fast
 * upload's own commit is never held (T1); and a held run in any request of
 * a call refuses the whole call (T2).
 */
@Timeout(30)
class LocalSequencerDefaultHoldWaitTest {

    private record Held(LocalSequencer sequencer, CommitDesk desk) {
    }

    /** A sequencer whose fast term walked A's index, its takeover holding RA undecided. */
    private static Held held(MemoryBinStore store) throws Exception {
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"),
                8, BoundedRecoveryFixture.noRenew()).orElseThrow();
        MemoryBinStore rosters = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermDefaultHoldTest.walking(rosters, rosters, A);
        sequencer.attach(term);
        CommitDesk desk = term.commitDesk(
                FastJournal.recover(new MemoryJournalFile(), 1 << 20), SELF);
        desk.leader().undecided(List.of(RA));
        desk.leader().everyMemberHeard();
        return new Held(sequencer, desk);
    }

    @Test
    void aHELDCommitWaitsForItsDecisionAndThenCommits() throws Exception {
        Held h = held(new MemoryBinStore());
        try {
            CompletableFuture<Long> committed = CompletableFuture.supplyAsync(() -> {
                try {
                    h.sequencer().commitAll(List.of(
                            new CommitRequest("poda", "i1", 0, "seg/0", counts(3))));
                    return h.sequencer().committedNext(RA);
                } catch (IOException failed) {
                    throw new IllegalStateException(failed);
                }
            });
            Thread.sleep(300);
            assertThat(committed).as("waiting for the decision").isNotDone();

            h.desk().leader().decided(RA, 0);

            assertThat(committed.get(DefaultHold.WAIT.toMillis(), TimeUnit.MILLISECONDS))
                    .isEqualTo(3);
        } finally {
            h.sequencer().close();
        }
    }

    @Test
    void aFASTUploadsCommitIsNeverHeld() throws Exception {
        Held h = held(new MemoryBinStore());
        try {
            h.sequencer().commitAt(new CommitRequest("poda", "fast3", 0, "seg/f", counts(2)),
                    Map.of(RA, 0L));

            assertThat(h.sequencer().committedNext(RA)).isEqualTo(2);
        } finally {
            h.sequencer().close();
        }
    }

    @Test
    void aHELDRunInALaterRequestRefusesTheWholeCall() throws Exception {
        Held h = held(new MemoryBinStore());
        RunKey other = new RunKey(new UUID(7, 7), 0);
        try {
            long started = System.nanoTime();
            assertThatThrownBy(() -> h.sequencer().commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", Map.of(other, 2)),
                    new CommitRequest("poda", "i1", 1, "seg/1", counts(3)))))
                    .isInstanceOf(IOException.class).hasMessageContaining("awaits");

            assertThat(h.sequencer().committedNext(other)).as("nothing committed").isZero();
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .as("refused after its wait").isGreaterThanOrEqualTo(DefaultHold.WAIT);
        } finally {
            h.sequencer().close();
        }
    }
}
