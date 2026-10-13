// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFence;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.FastTermStart;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.PodGone;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A leader holding no journal runs no takeover, so holds nothing (M13.33n
 * review round 1, P2): a walked index's default path commits at once rather
 * than for the rest of its term never.
 */
@Timeout(30)
class FastTakeoverDisklessTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    private static final Roster.Incarnation X =
            new Roster.Incarnation("x", "uid-x", "az-x", "x:1");

    @Test
    void aDISKLESSLeaderStartsNoTakeoverAndHoldsNoDefaultCommit() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                .orElseThrow().close();
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        try (FastDisk disk = FastDisk.open(store, leases.leaseKey(), Optional.empty(),
                new FastDiskTest.Files())) {
            Roster one = new Roster(1, -1, X, List.of(new Roster.Member(X,
                    Roster.State.ROSTERED)), List.of(new Roster.TermRecord(0,
                    new TreeMap<>(Map.of(INDEX, 1)))), List.of(), 0, 0, false);
            store.put(Roster.key("p", 1), Body.ofBytes(one.encode()));
            store.put(Roster.latestKey("p"), Body.ofBytes(Roster.encodeLatest(1)));
            FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                    new EmptyTermCloser(store, "p")::close,
                    new FastLeaseFence(Duration.ofSeconds(10), WALL, MONO), SELF, Map::of)
                    .open(2, term::close).orElseThrow();
            FastLeaderTerm fast = new FastLeaderTerm(opened,
                    new JoinDesk(new RosterJoins(store, "p", 2, MONO, Duration.ofMillis(250)),
                            nanos -> { }),
                    term::committedNext, store, "p", MONO, Duration.ofMillis(250));
            term.attach(fast);
            assertThat(fast.holdsDefault(S)).as("the premise: a walked index").isTrue();

            boolean started = FastTakeover.start(term, disk, SELF, (endpoint, frame) -> {
                throw new IOException("nothing is sent");
            }, PodGone.NEVER, runs -> "seg", store, "p", WALL, Runnable::run);

            assertThat(started).isFalse();
            assertThat(fast.holdsDefault(S)).isFalse();
            term.commitAll(List.of(new CommitRequest("self", "i1", 0, "seg/0", Map.of(S, 2))));
            assertThat(term.committedNext(S)).isEqualTo(2);
        } finally {
            term.close();
        }
    }
}
