// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A leader's refusal of a default-path run of a fast stream is DEFINITE
 * (M13.34g review round 1, P1, T1, P4): a forwarding pod answers it and
 * never defers it to the inbox, where every drain would refuse it again and
 * stall that pod's inbox; it is refused inside the sequencer's monitor too;
 * and a stream both held for a takeover and fast is refused at once.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastRefusalNotDeferredTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint, Duration.ofSeconds(10),
                Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(MemoryBinStore store, String podId, String endpoint,
            InProcessTransport transport) throws Exception {
        LeaseManager manager = new LeaseManager(store, config(podId, endpoint), CLOCK);
        FleetSequencer fleet = new FleetSequencer(store, config(podId, endpoint), transport,
                () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        BoundedRecoveryFixture.noRenew()));
        if (fleet.leading()) {
            transport.at(endpoint, fleet);
        }
        return fleet;
    }

    /** A first term whose catalog has A's index fast. */
    private static FastLeaderTerm fastA() throws Exception {
        MemoryBinStore rosters = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(rosters, "p"),
                new EmptyTermCloser(rosters, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, () -> Map.of(A, 1))
                .open(1, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(rosters, "p", 1, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, rosters, "p", new Mono(), Duration.ofMillis(250));
    }

    @Test
    void aFORWARDTheLeaderRefusedIsAnsweredAndNeverDeferredToTheInbox() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        FleetSequencer leader = pod(store, "poda", "pod-a:9000", transport);
        FleetSequencer follower = pod(store, "podb", "pod-b:9000", transport);
        try {
            assertThat(leader.leading()).as("the premise").isTrue();
            LocalSequencer.underneath(leader.heldTerm()).orElseThrow().attach(fastA());

            assertThatThrownBy(() -> follower.commitAll(List.of(
                    new CommitRequest("podb", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(CommitRefusedException.class);

            assertThat(store.list(PREFIX + "/ctl/inbox/", null, 100).objects())
                    .as("no intent every drain would refuse").isEmpty();
        } finally {
            follower.close();
            leader.close();
        }
    }

    @Test
    void theMONITORsOwnCheckRefusesAFastStream() throws Exception {
        assertThatThrownBy(() -> DefaultHold.refuseHeld(fastA(), List.of(
                new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                .isInstanceOf(CommitRefusedException.class);
    }

    @Test
    void aSTREAMHeldForATakeoverAndFastIsRefusedAtOnce() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX,
                DedupFixtures.manager(store, "poda"), 8, BoundedRecoveryFixture.noRenew())
                .orElseThrow();
        try {
            MemoryBinStore rosters = new MemoryBinStore();
            FastLeaderTerm term = FastLeaderTermDefaultHoldTest.walking(rosters, rosters, A);
            term.catalogSays(Map.of(A, 1));
            sequencer.attach(term);
            assertThat(term.holdsDefault(RA)).as("the premise: held").isTrue();
            long started = System.nanoTime();

            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(CommitRefusedException.class);

            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .as("never waited on as a hold").isLessThan(DefaultHold.WAIT);
        } finally {
            sequencer.close();
        }
    }
}
