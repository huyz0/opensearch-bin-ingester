// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A deposed desk asks the store nothing more, and a handler whose answer a
 * second JOIN of the same pod took still answers (M13.27j review round 1,
 * T4, T5).
 */
class JoinDeskWaitersTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private static MemoryBinStore startedAt7() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new FastTermStart(store, "p").start(7, FastTermStartTest.SELF, Map.of(), 0,
                Optional.empty());
        return store;
    }

    @Test
    void aDEPOSEDDeskAsksTheStoreNothingMore() throws Exception {
        MemoryBinStore backing = startedAt7();
        FastTermStartTest.put(backing, FastTermStartTest.read(backing, 7).fencedBy(9));
        CountingBinStore store = new CountingBinStore(backing);
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(), INTERVAL),
                nanos -> { });
        desk.join(FastTermStartTest.incarnation("a"));
        long before = store.counts().total();

        assertThat(desk.join(FastTermStartTest.incarnation("b")))
                .isEqualTo(new JoinDesk.Deposed(9));

        assertThat(store.counts().total() - before).isZero();
    }

    @Test
    void aWAITERWhoseAnswerASecondJoinTookStillAnswers() throws Exception {
        MemoryBinStore store = startedAt7();
        AtomicLong now = new AtomicLong(5_000_000_000L);
        Roster.Incarnation a = FastTermStartTest.incarnation("a");
        Roster.Incarnation b = FastTermStartTest.incarnation("b");
        CountDownLatch firstWaiting = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, now::get, INTERVAL),
                nanos -> {
                    if (Thread.currentThread().getName().equals("first")
                            && firstWaiting.getCount() > 0) {
                        firstWaiting.countDown();
                        assertThat(secondDone.await(10, TimeUnit.SECONDS)).isTrue();
                    }
                    now.addAndGet(nanos);
                });
        desk.join(b);
        CompletableFuture<JoinDesk.Outcome> first = new CompletableFuture<>();
        Thread.ofVirtual().name("first").start(() -> first.complete(join(desk, a)));
        assertThat(firstWaiting.await(10, TimeUnit.SECONDS)).as("the first JOIN waits").isTrue();

        JoinDesk.Outcome second = join(desk, a);
        secondDone.countDown();

        assertThat(second).isInstanceOf(JoinDesk.Rostered.class);
        assertThat(first.get(10, TimeUnit.SECONDS))
                .as("the first handler answers, never spinning on an empty flush")
                .isInstanceOf(JoinDesk.Rostered.class);
    }

    private static JoinDesk.Outcome join(JoinDesk desk, Roster.Incarnation pod) {
        try {
            return desk.join(pod);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
