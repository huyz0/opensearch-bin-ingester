// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.FastDepartureTest.config;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.journaledDisk;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.leaseHeldBy;
import static io.github.huyz0.os.biningester.server.FastDepartureTest.read;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A departure fitted to the shutdown budget (M13.35e; M13.27p review round 2,
 * P5, T5, T6): no ask begun that cannot finish inside the grace, no leave past
 * it, the step inside §7's 30 s beside the others; the interrupt path stops
 * undeparted with the flag clear, and a failed first lease read counts as an
 * ask.
 */
class FastDepartureBoundsTest {

    /** A leader each exchange with which takes a whole exchange timeout. */
    private static TermJoiner.Transport slow(FastDepartureTest.Leader leader,
            FastDepartureTest.Time time) {
        return (endpoint, frame) -> {
            time.nanos.addAndGet(FastDeparture.EXCHANGE_TIMEOUT.toNanos());
            return leader.exchange(endpoint, frame);
        };
    }

    private static FastDeparture.Result depart(MemoryBinStore store, FastDisk disk,
            TermJoiner.Transport transport, Duration grace, FastDepartureTest.Time time) {
        return FastDeparture.depart(config(), () -> read(store), disk, transport, false, grace,
                time.nanos::get, interval -> time.nanos.addAndGet(interval.toNanos()));
    }

    @Test
    void anASKIsBegunOnlyWhereItFinishesInsideTheGrace() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        leader.pending = true;
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        long started = time.nanos.get();
        Duration grace = Duration.ofSeconds(4);
        try (FastDisk disk = journaledDisk(store)) {
            depart(store, disk, slow(leader, time), grace, time);
        }

        assertThat(time.nanos.get() - started).as("the grace and one pause at most")
                .isLessThanOrEqualTo(grace.plus(FastDeparture.PAUSE).toNanos());
    }

    @Test
    void aLEAVEThatCannotFinishInsideTheGraceIsNotAsked() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        FastDeparture.Result result;
        try (FastDisk disk = journaledDisk(store)) {
            // a read of 0.6 s and a report of 1 s fit 2.2 s; a leave after them does not
            result = FastDeparture.depart(config(), () -> {
                time.nanos.addAndGet(Duration.ofMillis(600).toNanos());
                return read(store);
            }, disk, slow(leader, time), false, Duration.ofMillis(2_200), time.nanos::get,
                    interval -> time.nanos.addAndGet(interval.toNanos()));
        }

        assertThat(result).isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        assertThat(leader.asked).as("the report fitted; the leave did not")
                .containsExactly("DEPART1");
    }

    @Test
    void anINTERRUPTStopsItUndepartedWithTheFlagClear() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        leader.pending = true;
        FastDeparture.Result result;
        try (FastDisk disk = journaledDisk(store)) {
            result = FastDeparture.depart(config(), () -> read(store), disk, leader, false,
                    Duration.ofSeconds(10), new FastDepartureTest.Time().nanos::get,
                    interval -> {
                        throw new InterruptedException("the shutdown is cut short");
                    });
        }
        boolean flag = Thread.interrupted();

        assertThat(result).isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        assertThat(flag).as("not set again, so the lease release is not failed by it")
                .isFalse();
        assertThat(leader.asked).as("never departed").doesNotContain("DEPART2");
    }

    @Test
    void aFAILEDFirstLeaseReadCountsAsAnAsk() throws Exception {
        MemoryBinStore store = leaseHeldBy(config().podUid());
        AtomicInteger reads = new AtomicInteger();
        FastDepartureTest.Leader leader = new FastDepartureTest.Leader();
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        FastDeparture.Result result;
        try (FastDisk disk = journaledDisk(store)) {
            result = FastDeparture.depart(config(), () -> {
                if (reads.getAndIncrement() == 0) {
                    throw new IOException("the store did not answer");
                }
                return read(store);
            }, disk, leader, false, Duration.ofSeconds(3), time.nanos::get,
                    interval -> time.nanos.addAndGet(interval.toNanos()));
        }

        assertThat(result).as("asked once, a lease naming this pod is waited out")
                .isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
    }

    @Test
    void theDEPARTUREFitsTheShutdownBudgetBesideTheOtherSteps() {
        Duration steps = IngesterNode.SUBSCRIBER_GRACE.plus(IngesterNode.READINESS_PROPAGATION)
                .plus(IngesterNode.IN_FLIGHT_BOUND).plus(FastDeparture.STEP_BOUND)
                .plus(IngesterNode.FLUSH_AND_RELEASE);

        assertThat(steps).as("§7's 30 s").isLessThanOrEqualTo(IngesterNode.SHUTDOWN_BUDGET);
        assertThat(FastDeparture.EXCHANGE_TIMEOUT.multipliedBy(3))
                .as("a lease read, a report and a leave fit inside the grace")
                .isLessThanOrEqualTo(FastDeparture.GRACE);
    }

    @Test
    @Timeout(20)
    void aLEADERThatNeverAnswersIsGivenUpAtTheExchangeBound() throws Exception {
        // ⚠️ review round 1, T1: the bound is the departure's own, not its caller's.
        MemoryBinStore store = leaseHeldBy("uid-l");
        CountDownLatch never = new CountDownLatch(1);
        long started = System.nanoTime();
        FastDeparture.Result result;
        try (FastDisk disk = journaledDisk(store)) {
            result = depart(store, disk, (endpoint, frame) -> {
                try {
                    never.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return new byte[0];
            }, Duration.ofSeconds(2), new FastDepartureTest.Time());
        }

        assertThat(result).isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(FastDeparture.EXCHANGE_TIMEOUT.multipliedBy(5));
    }

    @Test
    @Timeout(20)
    void aLEASEReadThatNeverAnswersIsGivenUpAtItsBound() throws Exception {
        // ⚠️ review round 1, P1: a lease GET has no timeout of its own.
        MemoryBinStore store = leaseHeldBy("uid-l");
        CountDownLatch never = new CountDownLatch(1);
        FastDepartureTest.Time time = new FastDepartureTest.Time();
        long started = System.nanoTime();
        FastDeparture.Result result;
        try (FastDisk disk = journaledDisk(store)) {
            result = FastDeparture.depart(config(), () -> {
                try {
                    never.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return read(store);
            }, disk, new FastDepartureTest.Leader(), false, Duration.ofSeconds(2),
                    time.nanos::get, interval -> time.nanos.addAndGet(interval.toNanos()));
        }

        assertThat(result).isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(FastDeparture.EXCHANGE_TIMEOUT.multipliedBy(5));
    }
}
