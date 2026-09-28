// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link LaneAdmission#acquire} waits for room under the same rule
 * {@code tryAcquire} refuses by, and is woken by a release (M11.7).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LaneAdmissionAcquireTest {

    @Test
    void aSaturatedAcquireWaitsAndIsAdmittedWhenAPermitIsReleased() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        LaneAdmission.Permit held = admission.tryAcquire((byte) 0).orElseThrow();
        CountDownLatch started = new CountDownLatch(1);

        CompletableFuture<LaneAdmission.Permit> waiting = CompletableFuture.supplyAsync(() -> {
            started.countDown();
            try {
                return admission.acquire((byte) 0);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        started.await();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (admission.waiting() < 1 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(admission.waiting()).as("⚠️ PARKED IN THE WAIT").isEqualTo(1);
        assertThat(waiting).as("NOT REFUSED AND NOT ADMITTED PAST THE BUDGET").isNotDone();
        assertThat(admission.inFlight()).isEqualTo(1);

        held.release();

        LaneAdmission.Permit admitted = waiting.get(10, TimeUnit.SECONDS);
        assertThat(admission.inFlight()).as("the released permit, taken by the waiter")
                .isEqualTo(1);
        admitted.release();
        assertThat(admission.inFlight()).isZero();
    }

    @Test
    void anAcquireWithRoomReturnsAtOnceAndAnInterruptedOneThrows() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));

        LaneAdmission.Permit first = admission.acquire((byte) 0);
        assertThat(admission.inFlight()).isEqualTo(1);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                admission.acquire((byte) 0);
            } catch (InterruptedException e) {
                thrown.set(e);
            }
        });
        // ⚠️ INTERRUPTED ONLY ONCE IT IS PARKED IN THE WAIT: an interrupt that
        // raced ahead of the acquire would be thrown by the lock alone.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiter.getState() != Thread.State.WAITING && waiter.isAlive()
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(waiter.getState()).as("the premise: the waiter is parked, not admitted")
                .isEqualTo(Thread.State.WAITING);
        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        assertThat(admission.inFlight()).as("an interrupted waiter took nothing").isEqualTo(1);
        first.release();
        assertThatThrownBy(() -> {
            Thread.currentThread().interrupt();
            admission.acquire((byte) 0);
        }).isInstanceOf(InterruptedException.class);
        Thread.interrupted();
    }
}
