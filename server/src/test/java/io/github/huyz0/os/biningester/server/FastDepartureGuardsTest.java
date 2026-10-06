// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.FastDepartureTest.LEASE;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.config;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.journaledDisk;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.leaseHeldBy;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.read;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Lease;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A departure's bounds and its leader (M13.27p review round 1, T1, T2, T4):
 * the whole grace is waited, never less; a lease naming this pod or no one
 * departs nothing -- unless a leader was already asked, when it waits; and
 * the pause between asks is the stated one.
 */
class FastDepartureGuardsTest {

    private static final Duration GRACE = Duration.ofSeconds(10);

    @Test
    void aPENDINGCopyKeepsThePodForTheWholeGraceAndAtTheStatedPause() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        leader.pending = true;
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        long started = time.nanos.get();
        List<Duration> paused = new ArrayList<>();
        try (FastDisk disk = journaledDisk(store)) {
            FastDeparture.depart(config(), () -> read(store), disk, leader, false, GRACE,
                    time.nanos::get, interval -> {
                        paused.add(interval);
                        time.nanos.addAndGet(interval.toNanos());
                    });
        }

        assertThat(time.nanos.get() - started).as("never giving up before the grace")
                .isGreaterThanOrEqualTo(GRACE.toNanos());
        assertThat(paused).as("each pause the stated 250 ms").isNotEmpty()
                .containsOnly(FastDeparture.PAUSE);
    }

    @Test
    void aLEASENamingThisPodOrNoOneDepartsNothing() throws Exception {
        for (Lease lease : List.of(new Lease(1, "pod2", "uid-pod2", "http://pod2:1", 1_000L),
                new Lease(1, "l", "", "http://l:1", 1_000L))) {
            FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
            try (FastDisk disk = FastDisk.open(leaseHeldBy("uid-l"), LEASE, Optional.empty(),
                    new FastDiskTest.Files())) {
                assertThat(FastDeparture.depart(config(), () -> lease, disk, leader, false,
                        GRACE, new FastDepartureTest.Time().nanos::get, interval -> { }))
                        .as("a lease held by %s", lease.holderPodUid())
                        .isEqualTo(FastDeparture.Result.NOT_A_FOLLOWER);
            }
            assertThat(leader.asked).isEmpty();
        }
    }

    @Test
    void aLEADERAskedOnceAndThenLostIsWaitedForUntilTheGraceEnds() throws Exception {
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        leader.down = true;
        AtomicInteger looks = new AtomicInteger();
        Lease other = new Lease(1, "l", "uid-l", "http://l:1", 1_000L);
        Lease self = new Lease(2, "pod2", "uid-pod2", "http://pod2:1", 1_000L);
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        try (FastDisk disk = FastDisk.open(leaseHeldBy("uid-l"), LEASE, Optional.empty(),
                new FastDiskTest.Files())) {
            assertThat(FastDeparture.depart(config(),
                    () -> looks.getAndIncrement() == 0 ? other : self, disk, leader, false,
                    GRACE, time.nanos::get,
                    interval -> time.nanos.addAndGet(interval.toNanos())))
                    .as("it asked a leader once: it waits, never claiming no leader")
                    .isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        }
        assertThat(looks.get()).isGreaterThan(2);
    }
}
