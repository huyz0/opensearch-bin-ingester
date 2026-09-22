// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CatchUpCoordinatorTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    @Test
    void servicesLiveWorkBetweenBoundedCatchUpTurns() {
        var delivered = new CopyOnWriteArrayList<String>();
        CommittedDeltaSource source = (key, offset, limit) -> offset < 12
                ? List.of(new CommittedDeltaSource.CommittedRun(key, "seg-a", 1, offset + 1))
                : List.of();
        var coordinator = new CatchUpCoordinator(source, 1, 8,
                run -> delivered.add("catch-up:" + run.segmentKey()));

        assertThat(coordinator.enqueueCatchUp(KEY, 10)).isTrue();
        coordinator.enqueueLive(() -> delivered.add("live"));
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isFalse();
        assertThat(delivered).containsExactly("live", "catch-up:seg-a", "catch-up:seg-a");
    }

    @Test
    void refusesCatchUpWhenItsBoundCannotHoldTheWholeReplay() {
        CommittedDeltaSource source = (key, offset, limit) -> List.of(
                new CommittedDeltaSource.CommittedRun(key, "seg-a", 1, offset + 1),
                new CommittedDeltaSource.CommittedRun(key, "seg-b", 1, offset + 2));
        var coordinator = new CatchUpCoordinator(source, 1, 1, run -> { });

        assertThat(coordinator.enqueueCatchUp(List.of(
                new CommittedDeltaSource.ReplayRequest(KEY, 10),
                new CommittedDeltaSource.ReplayRequest(new RunKey(UUID.randomUUID(), 0), 10))))
                .isFalse();
        assertThat(coordinator.runNext()).isFalse();
    }

    @Test
    void exactFitAdmitsMultipleStreamsAndRoundRobinsThem() {
        RunKey other = new RunKey(UUID.randomUUID(), 0);
        CommittedDeltaSource source = (key, offset, limit) -> List.of(
                new CommittedDeltaSource.CommittedRun(key, key.equals(KEY) ? "a" : "b",
                        1, offset + 1));
        var delivered = new CopyOnWriteArrayList<String>();
        var coordinator = new CatchUpCoordinator(source, 1, 2,
                run -> delivered.add(run.key().equals(KEY) ? "a" : "b"));

        assertThat(coordinator.enqueueCatchUp(List.of(
                new CommittedDeltaSource.ReplayRequest(KEY, 10),
                new CommittedDeltaSource.ReplayRequest(other, 20)))).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(delivered).containsExactly("a", "b");
    }

    @Test
    void catchUpGetsAServiceTurnWhileLiveWorkKeepsArriving() {
        var delivered = new CopyOnWriteArrayList<String>();
        CommittedDeltaSource source = (key, offset, limit) -> offset < 13
                ? List.of(new CommittedDeltaSource.CommittedRun(key, "catch-up", 1, offset + 1))
                : List.of();
        var coordinator = new CatchUpCoordinator(source, 1, 8,
                run -> delivered.add("catch-up"));
        assertThat(coordinator.enqueueCatchUp(KEY, 10)).isTrue();

        for (int i = 0; i < 3; i++) {
            assertThat(coordinator.enqueueLive(() -> delivered.add("live"))).isTrue();
            assertThat(coordinator.runNext()).isTrue();
            assertThat(coordinator.runNext()).isTrue();
        }

        assertThat(delivered).containsExactly(
                "live", "catch-up", "live", "catch-up", "live", "catch-up");
    }

    @Test
    void anInFlightCursorKeepsItsCapacityReservationFromLiveAndReplayAdmissions() {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CommittedDeltaSource source = (key, offset, limit) -> List.of(
                new CommittedDeltaSource.CommittedRun(key, "seg", 1, offset + 1));
        var coordinator = new CatchUpCoordinator(source, 1, 1, run -> {
            started.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(coordinator.enqueueCatchUp(KEY, 10)).isTrue();
        Thread worker = Thread.ofVirtual().start(coordinator::runNext);
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(coordinator.enqueueLive(() -> { })).isFalse();
            assertThat(coordinator.enqueueCatchUp(new RunKey(UUID.randomUUID(), 0), 0))
                    .isFalse();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        } finally {
            release.countDown();
            try {
                worker.join(5_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        }
    }

    @Test
    void aFailureOpeningAStreamLeavesTheWholeRequestUnqueued() {
        var opens = new java.util.concurrent.atomic.AtomicInteger();
        CommittedDeltaSource source = new CommittedDeltaSource() {
            @Override
            public List<CommittedRun> replay(RunKey key, long offset, int limit) {
                return List.of();
            }

            @Override
            public ReplayCursor open(RunKey key, long offset) {
                if (opens.getAndIncrement() == 1) {
                    throw new IllegalStateException("second stream failed to open");
                }
                return new ReplayCursor() {
                    @Override
                    public java.util.Optional<CommittedRun> next() {
                        return java.util.Optional.empty();
                    }

                    @Override
                    public void retry() {
                        throw new IllegalStateException("nothing to retry");
                    }
                };
            }
        };
        var coordinator = new CatchUpCoordinator(source, 1, 4, run -> { });

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> coordinator.enqueueCatchUp(List.of(
                new CommittedDeltaSource.ReplayRequest(KEY, 0),
                new CommittedDeltaSource.ReplayRequest(new RunKey(UUID.randomUUID(), 0), 0))))
                .isInstanceOf(IllegalStateException.class);
        assertThat(coordinator.runNext()).isFalse();
    }

    @Test
    void rejectsAnOversizedRequestBeforeOpeningAnyStream() {
        AtomicInteger opens = new AtomicInteger();
        CommittedDeltaSource source = new CommittedDeltaSource() {
            @Override
            public List<CommittedRun> replay(RunKey key, long offset, int limit) {
                return List.of();
            }

            @Override
            public ReplayCursor open(RunKey key, long offset) {
                opens.incrementAndGet();
                return new ReplayCursor() {
                    @Override
                    public java.util.Optional<CommittedRun> next() {
                        return java.util.Optional.empty();
                    }

                    @Override
                    public void retry() {
                        throw new IllegalStateException("nothing to retry");
                    }
                };
            }
        };
        var coordinator = new CatchUpCoordinator(source, 1, 2, run -> { });

        assertThat(coordinator.enqueueCatchUp(List.of(
                new CommittedDeltaSource.ReplayRequest(KEY, 0),
                new CommittedDeltaSource.ReplayRequest(new RunKey(UUID.randomUUID(), 0), 0),
                new CommittedDeltaSource.ReplayRequest(new RunKey(UUID.randomUUID(), 0), 0))))
                .isFalse();
        assertThat(opens).hasValue(0);
    }

    @Test
    void retriesTheSameRunWhenTheSinkRejectsIt() {
        AtomicInteger deliveries = new AtomicInteger();
        CommittedDeltaSource source = (key, offset, limit) -> offset < 0
                ? List.of(new CommittedDeltaSource.CommittedRun(key, "seg", 1, 0))
                : List.of();
        var coordinator = new CatchUpCoordinator(source, 1, 2, run -> {
            if (deliveries.getAndIncrement() == 0) {
                throw new IllegalStateException("sink unavailable");
            }
        });

        assertThat(coordinator.enqueueCatchUp(KEY, -1)).isTrue();
        org.assertj.core.api.Assertions.assertThatThrownBy(coordinator::runNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("sink unavailable");
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isFalse();
        assertThat(deliveries).hasValue(2);
    }
}
