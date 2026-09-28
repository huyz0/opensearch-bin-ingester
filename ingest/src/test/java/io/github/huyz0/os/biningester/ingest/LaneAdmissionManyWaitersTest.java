// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * One release admits ONE waiter, however many are parked: {@code acquire}
 * re-checks the budget after every wake (M11.7 review T2).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LaneAdmissionManyWaitersTest {

    private static void awaitWaiting(LaneAdmission admission, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (admission.waiting() != n && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(admission.waiting()).isEqualTo(n);
    }

    @Test
    void oneReleaseAdmitsExactlyOneOfThreeWaitersAndTheBudgetHolds() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        LaneAdmission.Permit held = admission.tryAcquire((byte) 0).orElseThrow();
        List<CompletableFuture<LaneAdmission.Permit>> waiters = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            waiters.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return admission.acquire((byte) 0);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        awaitWaiting(admission, 3);

        held.release();
        awaitWaiting(admission, 2);

        assertThat(admission.inFlight())
                .as("⚠️ ONE WOKEN WAITER TOOK IT; THE OTHERS RE-CHECKED AND WAIT AGAIN")
                .isEqualTo(1);
        assertThat(waiters.stream().filter(CompletableFuture::isDone)).hasSize(1);
        for (int i = 0; i < 3; i++) {
            LaneAdmission.Permit next = waiters.stream().filter(CompletableFuture::isDone)
                    .map(CompletableFuture::join).findFirst().orElseThrow();
            waiters.removeIf(w -> w.isDone() && w.join() == next);
            next.release();
            if (i < 2) {
                awaitWaiting(admission, 1 - i);
                assertThat(admission.inFlight()).isEqualTo(1);
            }
        }
        assertThat(admission.inFlight()).isZero();
    }
}
