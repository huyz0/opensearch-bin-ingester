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
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A default-path commit with a run of a stream the leader has in fast mode is
 * refused whole, at once, before anything is written (ADR-0082 §7; ADR-0081
 * §8; M13.34c): the stream's offsets are the fast cursor's. The fast
 * upload's own commit of it is not refused, and an index the catalog turns
 * back to the default path is committed again.
 */
@Timeout(30)
class LocalSequencerFastHereTest {

    /** A first term, its fast indices at the start {@code starting}. */
    private static FastLeaderTerm term(Supplier<Map<UUID, Integer>> starting) throws Exception {
        MemoryBinStore rosters = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(rosters, "p"),
                new EmptyTermCloser(rosters, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, starting)
                .open(1, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(rosters, "p", 1, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, rosters, "p", new Mono(), Duration.ofMillis(250));
    }

    private static LocalSequencer sequencer(MemoryBinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    @Test
    void aDEFAULTCommitOfAFastStreamIsRefusedWholeAtOnce() throws Exception {
        LocalSequencer sequencer = sequencer(new MemoryBinStore());
        RunKey other = new RunKey(new UUID(7, 7), 0);
        try {
            FastLeaderTerm fast = term(Map::of);
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);
            long started = System.nanoTime();

            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", Map.of(other, 2)),
                    new CommitRequest("poda", "i1", 1, "seg/1", counts(3)))))
                    .isInstanceOf(IOException.class).hasMessageContaining("fast at the leader");

            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .as("refused at once, not after a takeover's wait")
                    .isLessThan(DefaultHold.WAIT);
            assertThat(sequencer.committedNext(RA)).isZero();
            assertThat(sequencer.committedNext(other)).as("the whole request").isZero();
        } finally {
            sequencer.close();
        }
    }

    @Test
    void theFASTUploadsOwnCommitIsNotRefused() throws Exception {
        LocalSequencer sequencer = sequencer(new MemoryBinStore());
        try {
            FastLeaderTerm fast = term(Map::of);
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);

            sequencer.commitAt(new CommitRequest("poda", "fast1", 0, "seg/f", counts(2)),
                    Map.of(RA, 0L));

            assertThat(sequencer.committedNext(RA)).isEqualTo(2);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aSTREAMFastAtTheTermsStartIsRefusedBeforeTheCatalogSpeaks() throws Exception {
        LocalSequencer sequencer = sequencer(new MemoryBinStore());
        try {
            sequencer.attach(term(() -> Map.of(A, 2)));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(IOException.class);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anINDEXTheCatalogTurnsBackToTheDefaultPathIsCommitted() throws Exception {
        LocalSequencer sequencer = sequencer(new MemoryBinStore());
        try {
            FastLeaderTerm fast = term(() -> Map.of(A, 2));
            sequencer.attach(fast);

            fast.catalogSays(Map.of());
            sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3))));

            assertThat(sequencer.committedNext(RA)).isEqualTo(3);
        } finally {
            sequencer.close();
        }
    }
}
