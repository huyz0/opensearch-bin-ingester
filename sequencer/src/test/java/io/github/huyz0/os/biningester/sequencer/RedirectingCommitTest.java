// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The redirecting commit answer at a fast term's leader (M13.34c; ADR-0081
 * §8, ADR-0082 §7): a run of a stream the catalog has in fast mode is
 * redirected and the rest committed; a retry is answered per run -- from the
 * deltas that committed it, and a run redirected before, its stream back on
 * the default path, committed now.
 */
@Timeout(30)
class RedirectingCommitTest {

    private static final RunKey OTHER = new RunKey(new UUID(7, 7), 0);

    private static FastLeaderTerm term() throws Exception {
        MemoryBinStore rosters = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(rosters, "p"),
                new EmptyTermCloser(rosters, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(1, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(rosters, "p", 1, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, rosters, "p", new Mono(), Duration.ofMillis(250));
    }

    private static LocalSequencer sequencer() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    private static final CommitRequest BOTH =
            new CommitRequest("poda", "i1", 0, "seg/0", Map.of(RA, 3, OTHER, 2));

    @Test
    void aRUNOfAFastStreamIsRedirectedAndTheRestCommitted() throws Exception {
        LocalSequencer sequencer = sequencer();
        try {
            FastLeaderTerm fast = term();
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);

            CommitAnswer answer = sequencer.commitAnswering(BOTH);

            assertThat(answer.redirected()).containsExactly(RA);
            assertThat(answer.deltas()).singleElement().satisfies(d -> assertThat(d.segments())
                    .singleElement().satisfies(s -> assertThat(s.runs())
                            .containsExactly(new RunCommit(OTHER, 2, 0))));
            assertThat(sequencer.committedNext(RA)).as("never over fast offsets").isZero();
            assertThat(sequencer.committedNext(OTHER)).isEqualTo(2);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aREQUESTWhollyOfFastStreamsIsRedirectedWithNothingWritten() throws Exception {
        LocalSequencer sequencer = sequencer();
        try {
            FastLeaderTerm fast = term();
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);

            CommitAnswer answer = sequencer.commitAnswering(
                    new CommitRequest("poda", "i1", 0, "seg/0", Map.of(RA, 3)));

            assertThat(answer).isEqualTo(new CommitAnswer(List.of(), List.of(RA)));
            assertThat(sequencer.committedNext(RA)).isZero();
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aRETRYAfterTheStreamTurnedBackToTheDefaultPathCommitsItsRedirectedRunNow()
            throws Exception {
        LocalSequencer sequencer = sequencer();
        try {
            FastLeaderTerm fast = term();
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);
            CommitAnswer first = sequencer.commitAnswering(BOTH);
            fast.catalogSays(Map.of());

            CommitAnswer retry = sequencer.commitAnswering(BOTH);

            assertThat(retry.redirected()).isEmpty();
            assertThat(retry.deltas()).extracting(CommitDelta::sequence).as("both deltas")
                    .containsExactly(first.deltas().get(0).sequence(),
                            first.deltas().get(0).sequence() + 1);
            assertThat(retry.deltas().get(1).segments().get(0).runs())
                    .containsExactly(new RunCommit(RA, 3, 0));
            assertThat(sequencer.committedNext(RA)).isEqualTo(3);
            assertThat(sequencer.committedNext(OTHER)).as("not committed twice").isEqualTo(2);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aRETRYWhileTheStreamIsStillFastIsRedirectedAgainNothingCommitted() throws Exception {
        // ⚠️ its review round 1, T1.
        LocalSequencer sequencer = sequencer();
        try {
            FastLeaderTerm fast = term();
            fast.catalogSays(Map.of(A, 1));
            sequencer.attach(fast);
            CommitAnswer first = sequencer.commitAnswering(BOTH);

            CommitAnswer retry = sequencer.commitAnswering(BOTH);

            assertThat(retry.redirected()).containsExactly(RA);
            assertThat(retry.deltas()).extracting(CommitDelta::sequence)
                    .containsExactly(first.deltas().get(0).sequence());
            assertThat(sequencer.committedNext(RA)).as("never over fast offsets").isZero();
            assertThat(sequencer.committedNext(OTHER)).isEqualTo(2);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aCOMMITTEDRunWhoseStreamTurnedFastIsAnsweredNotRedirected() throws Exception {
        // ⚠️ its review round 1, P1: redirected, its records would land twice.
        LocalSequencer sequencer = sequencer();
        try {
            FastLeaderTerm fast = term();
            fast.catalogSays(Map.of());
            sequencer.attach(fast);
            CommitAnswer first = sequencer.commitAnswering(BOTH);
            fast.catalogSays(Map.of(A, 1));

            CommitAnswer retry = sequencer.commitAnswering(BOTH);

            assertThat(retry.redirected()).isEmpty();
            assertThat(retry.deltas()).singleElement().satisfies(d -> {
                assertThat(d.sequence()).isEqualTo(first.deltas().get(0).sequence());
                assertThat(d.segments().get(0).runs()).containsExactly(
                        new RunCommit(RA, 3, 0), new RunCommit(OTHER, 2, 0));
            });
            assertThat(sequencer.committedNext(RA)).isEqualTo(3);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anOLDERRetryIsAnsweredWithTheDeltaThatCommittedIt() throws Exception {
        // ⚠️ M13.34b review round 1, P1: the bare answer named the pod's
        // newest delta, which does not carry this segment.
        LocalSequencer sequencer = sequencer();
        try {
            CommitAnswer first = sequencer.commitAnswering(BOTH);
            sequencer.commitAnswering(
                    new CommitRequest("poda", "i1", 1, "seg/1", Map.of(OTHER, 1)));

            CommitAnswer retry = sequencer.commitAnswering(BOTH);

            assertThat(retry.deltas()).extracting(CommitDelta::sequence)
                    .containsExactly(first.deltas().get(0).sequence());
            assertThat(sequencer.committedNext(OTHER)).as("nothing committed again")
                    .isEqualTo(3);
        } finally {
            sequencer.close();
        }
    }
}
