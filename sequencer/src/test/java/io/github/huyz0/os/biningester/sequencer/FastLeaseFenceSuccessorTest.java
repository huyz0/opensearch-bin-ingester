// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The successor's half of ADR-0081 §3's lease-time fence (M13.26): a pod that
 * took the lease over assigns no fast offset before its wall clock passes the
 * replaced lease's expiry plus the margin AND its monotonic clock passes the
 * instant it first read the replaced VERSION plus the TTL plus the margin --
 * reset at every newer version it reads, since each renewal is one.
 *
 * <p>⚠️ THE MONOTONIC HALF is the load-bearing one: the wall expiry was stamped
 * on the old leader's clock, so a stopped or backwards clock there would let
 * the successor in early.
 */
class FastLeaseFenceSuccessorTest {

    private static final Duration TTL = Duration.ofSeconds(10);

    private static Pod pod(MemoryBinStore store, String podId) {
        return FastLeaseFenceHolderTest.pod(store, podId);
    }

    @Test
    void theFIRSTTermEverWaitsForNothing() throws Exception {
        Pod a = pod(new MemoryBinStore(), "a");
        a.leases().tryAcquire();

        assertThat(a.fence().successorMayAssign()).isTrue();
    }

    @Test
    void aTAKEOVERAtExpiryWaitsOnBothClocks() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        Pod b = pod(store, "b");
        assertThat(b.leases().tryAcquire()).as("b reads a's unexpired lease").isEmpty();
        b.advanceBoth(TTL);
        assertThat(b.leases().tryAcquire()).as("expired on b's clock: taken").isPresent();
        assertThat(b.fence().successorMayAssign()).isFalse();

        b.advanceBoth(Duration.ofMillis(999));
        assertThat(b.fence().successorMayAssign())
                .as("TTL + 999 ms after b first read that version").isFalse();
        b.advanceBoth(Duration.ofMillis(1));

        assertThat(b.fence().successorMayAssign()).isTrue();
    }

    @Test
    void theMONOTONICWaitHoldsWhenTheWallClockHasLongPassed() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        Pod b = pod(store, "b");
        b.leases().tryAcquire();
        b.wall().advance(Duration.ofHours(1));
        b.leases().tryAcquire();

        b.mono().advance(Duration.ofMillis(10_999));
        assertThat(b.fence().successorMayAssign())
                .as("the replaced expiry is long past on b's wall clock; the monotonic wait is not")
                .isFalse();
        b.mono().advance(Duration.ofMillis(1));

        assertThat(b.fence().successorMayAssign()).isTrue();
    }

    @Test
    void theWAITCountsFromTheLastVersionTheSuccessorRead() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        Pod b = pod(store, "b");
        b.leases().tryAcquire();
        for (int i = 0; i < 3; i++) {
            a.advanceBoth(Duration.ofSeconds(3));
            b.advanceBoth(Duration.ofSeconds(3));
            a.leases().renew();
            b.leases().tryAcquire();
        }
        Mono bMono = b.mono();
        long lastRead = bMono.nanos();
        b.advanceBoth(TTL);
        assertThat(b.leases().tryAcquire()).as("a stopped renewing; its last lease expired")
                .isPresent();
        b.wall().advance(Duration.ofHours(1));

        bMono.nanos = lastRead + TTL.toNanos() + Duration.ofMillis(999).toNanos();
        assertThat(b.fence().successorMayAssign())
                .as("the first read was 9 s earlier; only the last version's read counts")
                .isFalse();
        bMono.nanos = lastRead + TTL.toNanos() + Duration.ofSeconds(1).toNanos();

        assertThat(b.fence().successorMayAssign()).isTrue();
    }

    @Test
    void anEARLYChallengeWaitsAsLong() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        SimulatedClock wall = new SimulatedClock(1_000_000L);
        Mono mono = new Mono();
        FastLeaseFence fence = new FastLeaseFence(TTL, wall, mono);
        LeaseManager b = new LeaseManager(store,
                new LeaseConfig("p", "b", "", "uid-b", TTL, Duration.ofSeconds(3)), wall,
                lease -> true, fence);

        assertThat(b.tryAcquire()).as("the holder is gone by the challenge's evidence").isPresent();
        mono.advance(Duration.ofMillis(10_999));
        wall.advance(Duration.ofMillis(10_999));
        assertThat(fence.successorMayAssign()).as("the challenge shortens the lease, not the wait")
                .isFalse();
        mono.advance(Duration.ofMillis(1));
        wall.advance(Duration.ofMillis(1));

        assertThat(fence.successorMayAssign()).isTrue();
    }

    @Test
    void theWALLWaitHoldsWhenTheMonotonicOneHasPassed() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        SimulatedClock wall = new SimulatedClock(1_000_000L);
        Mono mono = new Mono();
        FastLeaseFence fence = new FastLeaseFence(TTL, wall, mono);
        LeaseManager b = new LeaseManager(store,
                new LeaseConfig("p", "b", "", "uid-b", TTL, Duration.ofSeconds(3)), wall,
                lease -> true, fence);
        b.tryAcquire();

        mono.advance(Duration.ofHours(1));
        wall.advance(Duration.ofMillis(10_999));
        assertThat(fence.successorMayAssign())
                .as("the replaced lease expires at +10 s; the margin ends at +11 s").isFalse();
        wall.advance(Duration.ofMillis(1));

        assertThat(fence.successorMayAssign()).isTrue();
    }
}
