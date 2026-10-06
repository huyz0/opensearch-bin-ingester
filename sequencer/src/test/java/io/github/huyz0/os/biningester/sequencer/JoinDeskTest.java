// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The leader answers each JOIN only once a roster write lists it, joins that
 * arrive together sharing one write (ADR-0081 §1; M13.27j).
 */
class JoinDeskTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private static MemoryBinStore startedAt7() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new FastTermStart(store, "p").start(7, FastTermStartTest.SELF, Map.of(), 0,
                Optional.empty());
        return store;
    }

    private static Roster.Incarnation pod(String name) {
        return FastTermStartTest.incarnation(name);
    }

    @Test
    void aJOINIsAnsweredOnceTheRosterListsIt() throws Exception {
        MemoryBinStore store = startedAt7();
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(), INTERVAL),
                nanos -> { });

        JoinDesk.Outcome outcome = desk.join(pod("a"));

        assertThat(outcome).isInstanceOf(JoinDesk.Rostered.class);
        assertThat(FastTermStartTest.read(store, 7).member("uid-a")).isPresent();
        assertThat(((JoinDesk.Rostered) outcome).roster().member("uid-a")).isPresent();
    }

    @Test
    void JOINSWaitingTogetherShareOneRosterWrite() throws Exception {
        CountingBinStore store = new CountingBinStore(startedAt7());
        AtomicLong now = new AtomicLong(5_000_000_000L);
        CountDownLatch bothWaiting = new CountDownLatch(2);
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, now::get, INTERVAL), nanos -> {
            bothWaiting.countDown();
            assertThat(bothWaiting.await(10, TimeUnit.SECONDS)).isTrue();
            now.addAndGet(nanos);
        });
        desk.join(pod("a"));
        long putsAfterFirst = store.counts().puts();

        CompletableFuture<JoinDesk.Outcome> b = CompletableFuture.supplyAsync(() -> join(desk, "b"));
        CompletableFuture<JoinDesk.Outcome> c = CompletableFuture.supplyAsync(() -> join(desk, "c"));

        assertThat(b.get(10, TimeUnit.SECONDS)).isInstanceOf(JoinDesk.Rostered.class);
        assertThat(c.get(10, TimeUnit.SECONDS)).isInstanceOf(JoinDesk.Rostered.class);
        assertThat(store.counts().puts() - putsAfterFirst)
                .as("b and c, waiting out one interval together, in one write").isEqualTo(1);
    }

    private static JoinDesk.Outcome join(JoinDesk desk, String name) {
        try {
            return desk.join(pod(name));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void aDEPARTEDIncarnationIsAnsweredDeparted() throws Exception {
        MemoryBinStore store = startedAt7();
        Roster r = FastTermStartTest.read(store, 7);
        FastTermStartTest.put(store, new Roster(7, r.predecessor(), r.leader(),
                List.of(r.members().get(0), new Roster.Member(pod("a"), Roster.State.DEPARTED)),
                r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(), INTERVAL),
                nanos -> { });

        assertThat(desk.join(pod("a"))).isInstanceOf(JoinDesk.Departed.class);
    }

    @Test
    void aFENCEDRosterDeposesTheDesk() throws Exception {
        MemoryBinStore store = startedAt7();
        FastTermStartTest.put(store, FastTermStartTest.read(store, 7).fencedBy(9));
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(), INTERVAL),
                nanos -> { });

        assertThat(desk.join(pod("a"))).isEqualTo(new JoinDesk.Deposed(9));
        assertThat(desk.join(pod("b"))).as("and stays deposed").isEqualTo(new JoinDesk.Deposed(9));
    }

    @Test
    void aSTOREFailureIsThrownAndTheJoinIsAnsweredOnTheRetry() throws Exception {
        MemoryBinStore backing = startedAt7();
        HookedStore store = new HookedStore(backing, Roster.key("p", 7), b -> {
            throw new IOException("store down");
        });
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(), INTERVAL),
                nanos -> { });

        assertThatThrownBy(() -> desk.join(pod("a"))).isInstanceOf(IOException.class);

        assertThat(desk.join(pod("a"))).isInstanceOf(JoinDesk.Rostered.class);
    }
}
