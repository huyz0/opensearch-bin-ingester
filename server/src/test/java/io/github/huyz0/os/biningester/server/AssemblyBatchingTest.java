// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The elected term BATCHES: commits arriving while one is in the store share
 * the next delta (M8.50, NFR-3, R6).
 *
 * <p>⚠️ **ONE PUT PER COMMIT IS A RATE SCALING WITH PODS**, and it is also a
 * throughput ceiling: a leader writing one delta at a time commits about six
 * a second against a real store, and every follower's flush queues behind
 * that. M5 built {@code BatchingSequencer} for this; M8.49 left it unwired.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AssemblyBatchingTest {

    private static final int COMMITS = 8;

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    /** Holds the first delta PUT until {@code release}, and counts every delta PUT. */
    private static BinStore holdingFirstDelta(BinStore real, CountDownLatch held,
            CountDownLatch release, AtomicInteger deltas) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("putIfAbsent")
                            && ((String) args[0]).endsWith(".delta")) {
                        if (deltas.incrementAndGet() == 2) {
                            // the first is the term's CONTINUE; hold the first COMMIT
                            held.countDown();
                            release.await(20, TimeUnit.SECONDS);
                        }
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException thrown) {
                        throw thrown.getCause();
                    }
                });
    }

    @Test
    void commitsQUEUEDBehindAnInFlightDeltaSHAREThENextOne() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger deltas = new AtomicInteger();
        try (BinStore real = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            BinStore store = holdingFirstDelta(real, held, release, deltas);
            try (Assembly assembly = Assembly.open(config(), store, noPeers(),
                    Clock.systemUTC())) {
                Sequencer term = assembly.heldTerm();
                assertThat(term).as("the premise: this pod leads").isNotNull();
                int before = deltas.get();
                RunKey stream = new RunKey(UUID.randomUUID(), 0);
                List<Throwable> failed = new java.util.concurrent.CopyOnWriteArrayList<>();
                List<Thread> commits = new ArrayList<>();
                for (int p = 0; p < COMMITS; p++) {
                    String pod = "p" + p;
                    // ⚠️ PLATFORM THREADS, so their state is observable: the
                    // later ones are started only once the first is in the
                    // store, and the window is released only once every one of
                    // them is PARKED behind it -- no sleep decides it.
                    Thread t = Thread.ofPlatform().start(() -> {
                        try {
                            term.commit(new CommitRequest(pod, "i", 0, "seg/" + pod,
                                    Map.of(stream, 1)));
                        } catch (Throwable e) {
                            failed.add(e);
                        }
                    });
                    commits.add(t);
                    if (p == 0) {
                        assertThat(held.await(10, TimeUnit.SECONDS))
                                .as("the premise: the first commit is in the store").isTrue();
                    }
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (commits.stream().skip(1).anyMatch(t -> t.getState() == Thread.State.RUNNABLE
                        || t.getState() == Thread.State.NEW)) {
                    assertThat(System.nanoTime()).as("every later commit parks").isLessThan(deadline);
                    Thread.onSpinWait();
                }
                release.countDown();
                for (Thread commit : commits) {
                    commit.join(TimeUnit.SECONDS.toMillis(10));
                }
                assertThat(failed).as("every commit succeeded").isEmpty();

                assertThat(deltas.get() - before)
                        .as("⚠️ %d COMMITS, THE LAST %d QUEUED BEHIND THE FIRST, ARE TWO DELTAS: "
                                + "one PUT per commit is a rate scaling with pods",
                                COMMITS, COMMITS - 1)
                        .isEqualTo(2);
            }
        }
    }

    @Test
    void retentionREADSTheBatchedTerm() throws Exception {
        // ⚠️ THE HAZARD THE ROW NAMES: retention unwrapped the held term with
        // `instanceof LocalSequencer`, which is false for a batched one, and
        // "no term" there means GC silently stops.
        try (Assembly assembly = Assembly.open(config(), noPeers(), Clock.systemUTC())) {
            RunKey stream = new RunKey(UUID.randomUUID(), 0);
            assembly.heldTerm().commit(
                    new CommitRequest("p0", "i", 0, "seg/p0", Map.of(stream, 1)));

            assembly.retention().tick();

            assertThat(assembly.retention().termTicks())
                    .as("⚠️ THE PASS READ A TERM: GC runs on a batched leader").isEqualTo(1);
        }
    }
}
