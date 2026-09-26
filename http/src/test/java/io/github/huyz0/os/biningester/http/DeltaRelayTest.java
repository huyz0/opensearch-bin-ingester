// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/** An AZ's relay: one read per hint, in order, retried before the next (M10.20). */
class DeltaRelayTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    private record Relayed(long epoch, long sequence) {
    }

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "bins/cluster-a/data/s" + sequence + ".bseg",
                List.of(new RunCommit(KEY, 2, 2 * sequence)));
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void eachHintIsReadOnceAndHandedOnWithinTheAzInArrivalOrder() throws Exception {
        Map<Long, AtomicInteger> readsOf = new ConcurrentHashMap<>();
        List<Relayed> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    readsOf.computeIfAbsent(sequence, s -> new AtomicInteger()).incrementAndGet();
                    return Optional.of(delta(sequence));
                },
                (delta, epoch) -> relayed.add(new Relayed(epoch, delta.sequence())),
                Duration.ofMillis(1))) {
            for (long sequence = 0; sequence < 5; sequence++) {
                relay.offer(new DeltaHintFrame(3, sequence));
            }
            await(() -> relayed.size() == 5, "all five");

            assertThat(relayed).extracting(Relayed::sequence).containsExactly(0L, 1L, 2L, 3L, 4L);
            assertThat(relayed).extracting(Relayed::epoch).containsOnly(3L);
            assertThat(readsOf.values()).as("one read per (delta, AZ)")
                    .allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
            assertThat(relay.reads()).isEqualTo(5);
            assertThat(relay.relayed()).isEqualTo(5);
        }
    }

    @Test
    void anUncheckedReadFailureIsRetriedAndTheRelayLivesOn() throws Exception {
        AtomicInteger failures = new AtomicInteger(1);
        List<Long> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    if (sequence == 0 && failures.getAndDecrement() > 0) {
                        throw new java.io.UncheckedIOException(new IOException("adapter threw"));
                    }
                    return Optional.of(delta(sequence));
                },
                (delta, epoch) -> relayed.add(delta.sequence()),
                Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(1, 0));
            relay.offer(new DeltaHintFrame(1, 1));
            await(() -> relayed.size() == 2, "both, the first after its retry");

            assertThat(relayed).containsExactly(0L, 1L);
            assertThat(relay.reads()).isEqualTo(3);
        }
    }

    @Test
    void aHintAlreadyRelayedIsSkippedWithoutARead() throws Exception {
        List<Long> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay((epoch, sequence) -> Optional.of(delta(sequence)),
                (delta, epoch) -> relayed.add(delta.sequence()), Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(2, 4));
            relay.offer(new DeltaHintFrame(2, 4)); // resent after a lost answer
            relay.offer(new DeltaHintFrame(1, 9)); // an older term's
            relay.offer(new DeltaHintFrame(2, 5));
            await(() -> relayed.size() == 2, "the two new ones");

            assertThat(relayed).containsExactly(4L, 5L);
            assertThat(relay.skipped()).isEqualTo(2);
            assertThat(relay.reads()).isEqualTo(2);
        }
    }

    @Test
    void closeCountsWhatItAbandonsAndRefusesAfterwards() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    entered.countDown();
                    try {
                        new CountDownLatch(1).await(); // until interrupted
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    return Optional.empty();
                },
                (delta, epoch) -> { }, Duration.ofMillis(1));
        relay.offer(new DeltaHintFrame(1, 0));
        assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
        relay.offer(new DeltaHintFrame(1, 1));
        relay.offer(new DeltaHintFrame(1, 2));
        relay.close();

        assertThat(relay.lost()).as("the two still queued").isEqualTo(2);
        assertThat(relay.offer(new DeltaHintFrame(1, 3))).isFalse();
        assertThat(relay.overflowed()).isEqualTo(1);
    }

    @Test
    void theProductionBudgetIsAboutThirtySeconds() {
        long total = 0;
        long wait = DeltaRelay.FIRST_BACKOFF.toMillis();
        for (int attempt = 1; attempt < DeltaRelay.READ_ATTEMPTS; attempt++) {
            total += wait;
            wait = Math.min(wait * 2, DeltaRelay.MAX_BACKOFF.toMillis());
        }
        assertThat(total).isBetween(25_000L, 35_000L);
    }


    @Test
    void aFailedReadIsRetriedBeforeTheNextHintIsLookedAt() throws Exception {
        List<Long> readOrder = new CopyOnWriteArrayList<>();
        AtomicInteger firstFailures = new AtomicInteger(3);
        List<Long> relayed = new CopyOnWriteArrayList<>();
        CountDownLatch bothQueued = new CountDownLatch(1);
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    try {
                        bothQueued.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    readOrder.add(sequence);
                    if (sequence == 0 && firstFailures.getAndDecrement() > 0) {
                        throw new IOException("the store answered 503");
                    }
                    return Optional.of(delta(sequence));
                },
                (delta, epoch) -> relayed.add(delta.sequence()),
                Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(1, 0));
            relay.offer(new DeltaHintFrame(1, 1));
            bothQueued.countDown();
            await(() -> relayed.size() == 2, "both, in order");

            assertThat(readOrder).as("0 is retried to success before 1 is read")
                    .containsExactly(0L, 0L, 0L, 0L, 1L);
            assertThat(relayed).containsExactly(0L, 1L);
            assertThat(relay.lost()).isZero();
        }
    }

    @Test
    void aReadThatNeverSucceedsIsCountedLostWithinItsBudgetAndTheNextStillGoes() throws Exception {
        List<Long> pauses = new CopyOnWriteArrayList<>();
        AtomicInteger readsOfZero = new AtomicInteger();
        List<Long> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    if (sequence == 0) {
                        readsOfZero.incrementAndGet();
                        throw new IOException("the store is down for this one");
                    }
                    return Optional.of(delta(sequence));
                },
                (delta, epoch) -> relayed.add(delta.sequence()),
                Duration.ofMillis(100), pauses::add)) {
            relay.offer(new DeltaHintFrame(1, 0));
            relay.offer(new DeltaHintFrame(1, 1));
            await(() -> relayed.size() == 1, "the next hint");

            assertThat(readsOfZero.get()).isEqualTo(DeltaRelay.READ_ATTEMPTS);
            assertThat(relay.lost()).isEqualTo(1);
            assertThat(relayed).containsExactly(1L);
            assertThat(pauses).as("doubling to the cap, about 30 s in all")
                    .containsExactly(100L, 200L, 400L, 800L, 1600L, 3200L,
                            5000L, 5000L, 5000L, 5000L, 5000L);
            assertThat(pauses.stream().mapToLong(Long::longValue).sum())
                    .isBetween(25_000L, 35_000L);
        }
    }

    @Test
    void aHintForADeltaThatDoesNotExistAdvancesNothing() throws Exception {
        List<Long> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> sequence == 9 ? Optional.empty() : Optional.of(delta(sequence)),
                (delta, epoch) -> relayed.add(delta.sequence()),
                Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(1, 9)); // stale or forged
            relay.offer(new DeltaHintFrame(1, 2));
            await(() -> relayed.size() == 1, "the real one");

            assertThat(relayed).containsExactly(2L);
            assertThat(relay.missing()).isEqualTo(1);
            assertThat(relay.lost()).isZero();
            assertThat(relay.reads()).as("a missing delta is not retried").isEqualTo(2);
        }
    }

    @Test
    void aFullQueueDropsAndCountsRatherThanBlockingTheRoute() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    return Optional.empty();
                },
                (delta, epoch) -> { },
                Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(1, 0));
            assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
            int accepted = 0;
            for (int i = 1; i <= DeltaRelay.QUEUE_DEPTH + 3; i++) {
                if (relay.offer(new DeltaHintFrame(1, i))) {
                    accepted++;
                }
            }
            assertThat(accepted).isEqualTo(DeltaRelay.QUEUE_DEPTH);
            assertThat(relay.overflowed()).isEqualTo(3);
            release.countDown();
        }
    }

    @Test
    void aHandOnThatThrowsIsCountedAndTheRelayGoesOn() throws Exception {
        List<Long> relayed = new CopyOnWriteArrayList<>();
        try (DeltaRelay relay = new DeltaRelay(
                (epoch, sequence) -> Optional.of(delta(sequence)),
                (delta, epoch) -> {
                    if (delta.sequence() == 0) {
                        throw new IllegalStateException("the publisher is closing");
                    }
                    relayed.add(delta.sequence());
                },
                Duration.ofMillis(1))) {
            relay.offer(new DeltaHintFrame(1, 0));
            relay.offer(new DeltaHintFrame(1, 1));
            await(() -> relayed.size() == 1, "the next");

            assertThat(relay.lost()).isEqualTo(1);
            assertThat(relayed).containsExactly(1L);
        }
    }

    @Test
    void aBackoffMustBePositive() {
        for (Duration bad : List.of(Duration.ZERO, Duration.ofMillis(-1))) {
            assertThatThrownBy(() -> new DeltaRelay((e, s) -> Optional.empty(), (d, e) -> { }, bad))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
