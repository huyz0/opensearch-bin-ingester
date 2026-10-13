// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A term's switches, one drain tick at a time (M13.34r; ADR-0081 §8): every
 * paused stream's decision in one roster write at most once per
 * {@code min_upload_interval}, then its tail discarded; the run held until the
 * chain reaches the frontier; the stream unpaused once its default run
 * committed past it, or once it is fast again.
 */
class SwitchPauseTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);
    private static final RunKey U = new RunKey(new UUID(3, 3), 0);
    private static final long INTERVAL = Duration.ofMillis(250).toNanos();

    private final long[] nanos = {0};
    private final List<String> calls = new ArrayList<>();
    private final Map<RunKey, Long> chain = new HashMap<>();
    private final SwitchPause.Leader leader = new SwitchPause.Leader() {
        @Override
        public Set<FastJournalRecord.IdempotencyKey> discardFrom(RunKey stream, long from) {
            calls.add("discard " + stream + "@" + from);
            return Set.of();
        }

        @Override
        public void unpause(RunKey stream, long chainNext) {
            calls.add("unpause " + stream + "@" + chainNext);
        }
    };

    private static MemoryBinStore roster() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(3, 2, "self", false, 0, 0, false));
        return store;
    }

    private boolean tick(SwitchPause pause, MemoryBinStore store) throws Exception {
        return tick(pause, store, leader, s -> false);
    }

    private boolean tick(SwitchPause pause, MemoryBinStore store, SwitchPause.Leader with,
            java.util.function.Predicate<RunKey> fastNow) throws Exception {
        return pause.tick(with, new RosterDecisions(store, "p", 3),
                () -> calls.add("rosterWritten"), s -> chain.getOrDefault(s, 0L), fastNow);
    }

    @Test
    void everyPAUSEDStreamIsDecidedInOneWriteAtMostOncePerInterval() throws Exception {
        MemoryBinStore store = roster();
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, 10, 0);
        pause.paused(T, 5, 0);

        tick(pause, store);
        pause.paused(U, 7, 2);
        tick(pause, store);

        assertThat(FastTermStartTest.read(store, 3).decisions()).as("S and T, one write")
                .containsExactly(new Roster.Decision(0, S, 10), new Roster.Decision(1, T, 5));
        assertThat(calls).containsExactly("rosterWritten", "discard " + S + "@10",
                "discard " + T + "@5");
        assertThat(pause.holds(U)).as("U waits for the next interval").isTrue();

        nanos[0] += INTERVAL;
        tick(pause, store);

        assertThat(FastTermStartTest.read(store, 3).decisions()).last()
                .isEqualTo(new Roster.Decision(2, U, 7));
    }

    @Test
    void aRUNIsHeldUntilTheChainReachesTheFrontierAndTheStreamUnpausedAfterIt()
            throws Exception {
        MemoryBinStore store = roster();
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, 10, 0);
        chain.put(S, 8L);

        tick(pause, store);
        assertThat(pause.holds(S)).as("decided, its tail not yet uploaded").isTrue();
        chain.put(S, 10L);
        tick(pause, store);
        assertThat(pause.holds(S)).as("the chain at the frontier: the run admitted").isFalse();
        tick(pause, store);
        assertThat(calls).as("not before its run commits").doesNotContain("unpause " + S + "@10");
        chain.put(S, 12L);
        tick(pause, store);

        assertThat(calls).endsWith("unpause " + S + "@12");
    }

    @Test
    void aDEPOSEDTermDecidesAndDiscardsNothing() throws Exception {
        MemoryBinStore store = roster();
        FastTermStartTest.put(store, FastTermStartTest.read(store, 3).fencedBy(5));
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, 10, 0);

        assertThat(tick(pause, store)).isTrue();
        assertThat(calls).isEmpty();
    }

    @Test
    void aSTREAMPausedLaterIsNotMatchedByAnOlderDecisionOfItsOwn() throws Exception {
        // ⚠️ its review round 1, P1/T1: one shared since, the earliest, let T's
        // takeover decision stand in for its switch's -- superseding nothing.
        MemoryBinStore store = new MemoryBinStore();
        Roster r = FastTermStartTest.roster(3, 2, "self", false, 0, 0, false);
        FastTermStartTest.put(store, new Roster(r.epoch(), r.predecessor(), r.leader(),
                r.members(), r.termRecord(), List.of(new Roster.Decision(0, T, 5)),
                r.notBefore(), r.fencedBy(), r.closed()));
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, 10, 0);
        pause.paused(T, 5, 1);

        tick(pause, store);

        assertThat(FastTermStartTest.read(store, 3).decisions()).containsExactly(
                new Roster.Decision(0, T, 5), new Roster.Decision(1, S, 10),
                new Roster.Decision(2, T, 5));
    }

    @Test
    void aREADYStreamTheCatalogHasFastAgainIsUnpaused() throws Exception {
        // ⚠️ its review round 1, T2: its default run may never come.
        MemoryBinStore store = roster();
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, 10, 0);
        chain.put(S, 10L);
        tick(pause, store);
        assertThat(pause.holds(S)).isFalse();

        tick(pause, store, leader, s -> true);

        assertThat(calls).endsWith("unpause " + S + "@10");
    }

    @Test
    void aFAILEDDiscardLeavesItsStreamPausedAndTheOthersGoOn() throws Exception {
        // ⚠️ its review round 1, P2/T2.
        MemoryBinStore store = roster();
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        boolean[] failing = {true};
        SwitchPause.Leader flaky = new SwitchPause.Leader() {
            @Override
            public Set<FastJournalRecord.IdempotencyKey> discardFrom(RunKey stream, long from)
                    throws java.io.IOException {
                if (stream.equals(S) && failing[0]) {
                    throw new java.io.IOException("the journal refused the drop");
                }
                return leader.discardFrom(stream, from);
            }

            @Override
            public void unpause(RunKey stream, long chainNext) {
                leader.unpause(stream, chainNext);
            }
        };
        pause.paused(S, 10, 0);
        pause.paused(T, 5, 0);
        chain.put(T, 5L);

        tick(pause, store, flaky, s -> false);
        assertThat(pause.holds(T)).as("T decided, and ready").isFalse();
        assertThat(pause.holds(S)).as("S still paused").isTrue();
        failing[0] = false;
        nanos[0] += INTERVAL;
        tick(pause, store, flaky, s -> false);

        assertThat(calls).as("retried, its decision not written twice")
                .containsSubsequence("discard " + T + "@5", "discard " + S + "@10");
        assertThat(FastTermStartTest.read(store, 3).decisions()).hasSize(2);
    }
}
