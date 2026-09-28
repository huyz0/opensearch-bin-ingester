// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link LaneAdmission#acquire} waits under {@code tryAcquire}'s own rule: a
 * lane below its floor is admitted on a saturated pod, and an inactive lane is
 * admitted only under the budget (M11.7).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LaneAdmissionAcquireLaneTest {

    @Test
    void aLaneBelowItsFloorIsAdmittedAtOnceAndAnInactiveLaneWaitsForTheBudget()
            throws Exception {
        // Budget 2 over {0, 1}: both floors are 1.
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0, (byte) 1));
        LaneAdmission.Permit a = admission.tryAcquire((byte) 0).orElseThrow();
        LaneAdmission.Permit b = admission.tryAcquire((byte) 0).orElseThrow();

        LaneAdmission.Permit below = admission.acquire((byte) 1);
        assertThat(admission.inFlight())
                .as("⚠️ LANE 1 HOLDS NOTHING OF ITS FLOOR: admitted past the budget, no wait")
                .isEqualTo(3);

        CompletableFuture<LaneAdmission.Permit> inactive = CompletableFuture.supplyAsync(() -> {
            try {
                return admission.acquire((byte) 5);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (admission.waiting() < 1 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(admission.waiting()).as("an inactive lane has no floor and waits for the "
                + "budget").isEqualTo(1);
        assertThat(inactive).isNotDone();

        below.release();
        a.release();
        inactive.get(10, TimeUnit.SECONDS).release();
        b.release();
        assertThat(admission.inFlight()).isZero();
    }
}
