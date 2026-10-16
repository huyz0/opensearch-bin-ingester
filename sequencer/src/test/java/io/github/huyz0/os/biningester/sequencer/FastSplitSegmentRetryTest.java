// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A split segment between its two deltas (SPEC criterion 15; ADR-0081 §8;
 * M13.34z): its other stream committed now, its held run not yet -- then a
 * writer retry, and separately a leader crash. The retry is answered only
 * once every run is committed, the held run lands in the same segment, and
 * every record is committed exactly once.
 *
 * <p>⚠️ BY IDENTITY, AT THE LAYER THAT HAS IT: a committed run names no
 * record range -- it is its stream's whole run in its segment, so a record is
 * named by {@code (segment, stream)} and its place in that run. Every record
 * committed exactly once is every run of the segment committed in exactly one
 * delta, and no other segment committing a record of those streams: what
 * {@link #eachRecordOnce} reads off every delta the chain holds.
 */
@Timeout(60)
class FastSplitSegmentRetryTest {

    private static final RunKey OTHER = new RunKey(new UUID(7, 7), 0);
    private static final CommitRequest FLUSH =
            new CommitRequest("podb", "i1", 4, "seg/4", Map.of(RA, 3, OTHER, 2));

    private static LocalSequencer holdingA(MemoryBinStore store, FastLeaderTerm[] term)
            throws Exception {
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"),
                8, BoundedRecoveryFixture.noRenew()).orElseThrow();
        MemoryBinStore rosters = new MemoryBinStore();
        term[0] = FastLeaderTermDefaultHoldTest.walking(rosters, rosters, A);
        sequencer.attach(term[0]);
        return sequencer;
    }

    /**
     * Each record of the segment committed exactly once: of every delta the
     * chain holds, each stream's run of {@code seg/4} in exactly one, at its
     * full count, and no other segment committing either stream.
     */
    private static void eachRecordOnce(LocalSequencer sequencer) {
        Map<RunKey, List<String>> where = new java.util.HashMap<>();
        for (var delta : sequencer.chain().snapshot().deltas()) {
            for (SegmentCommit segment : delta.segments()) {
                for (var run : segment.runs()) {
                    where.computeIfAbsent(run.key(), k -> new java.util.ArrayList<>())
                            .add(segment.segmentKey() + "/" + run.recordCount());
                }
            }
        }
        assertThat(where).as("each stream's records in one run, of seg/4, once")
                .isEqualTo(Map.of(RA, List.of("seg/4/3"), OTHER, List.of("seg/4/2")));
    }

    /** Every run of the segment, by stream: the record count each delta committed. */
    private static Map<RunKey, Integer> runsOf(CommitAnswer answer) {
        Map<RunKey, Integer> runs = new java.util.HashMap<>();
        answer.deltas().forEach(d -> d.segments().stream()
                .filter(s -> s.segmentKey().equals("seg/4"))
                .flatMap(s -> s.runs().stream())
                .forEach(run -> runs.merge(run.key(), run.recordCount(), Integer::sum)));
        return runs;
    }

    @Test
    void aRETRYBetweenTheDeltasIsRefusedUntilTheHeldRunCommitsThenAnsweredOnce()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm[] term = new FastLeaderTerm[1];
        LocalSequencer sequencer = holdingA(store, term);
        try {
            assertThatThrownBy(() -> sequencer.commitAnswering(FLUSH))
                    .as("the held run outlasts its wait").isInstanceOf(IOException.class);
            assertThat(sequencer.committedNext(OTHER)).as("the first delta: the other stream")
                    .isEqualTo(2);

            assertThatThrownBy(() -> sequencer.commitAnswering(FLUSH))
                    .as("a retry between the deltas is never answered in part")
                    .isInstanceOf(IOException.class);
            assertThat(sequencer.committedNext(RA)).isZero();

            term[0].noTakeover();
            CommitAnswer answer = sequencer.commitAnswering(FLUSH);

            assertThat(answer.deltas()).as("answered from both deltas").hasSize(2);
            assertThat(runsOf(answer)).isEqualTo(Map.of(RA, 3, OTHER, 2));
            assertThat(sequencer.committedNext(RA)).isEqualTo(3);
            assertThat(sequencer.committedNext(OTHER)).as("never committed again").isEqualTo(2);
            assertThat(deltasCarrying(store, "seg/4")).as("one segment, two deltas").isEqualTo(2);
            eachRecordOnce(sequencer);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aLEADERCrashBetweenTheDeltasLeavesTheHeldRunToTheSuccessorOnce() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer crashed = holdingA(store, new FastLeaderTerm[1]);
        assertThatThrownBy(() -> crashed.commitAnswering(FLUSH)).isInstanceOf(IOException.class);
        assertThat(crashed.committedNext(OTHER)).isEqualTo(2);

        LocalSequencer successor = LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                new LeaseConfig(PREFIX, "podc", "", Duration.ofSeconds(10),
                        Duration.ofSeconds(3)),
                Clock.fixed(Instant.ofEpochMilli(1_000_000L + 60_000L), ZoneOffset.UTC)), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            CommitAnswer answer = successor.commitAnswering(FLUSH);

            assertThat(answer.deltas()).as("across two chains, one delta (M13.34ae)")
                    .hasSize(1);
            assertThat(runsOf(answer)).isEqualTo(Map.of(RA, 3, OTHER, 2));
            assertThat(successor.committedNext(RA)).isEqualTo(3);
            assertThat(successor.committedNext(OTHER)).isEqualTo(2);
            assertThat(deltasCarrying(store, "seg/4"))
                    .as("the held run in the same segment, never moved; nothing twice")
                    .isEqualTo(2);
            eachRecordOnce(successor);
        } finally {
            successor.close();
            try {
                crashed.close();
            } catch (IOException fenced) {
                // the crashed term was taken over: its close is refused
            }
        }
    }
}
