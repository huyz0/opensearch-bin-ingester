// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Weighted fair-share admission over the pod's in-flight {@code _bulk} budget
 * (M10.8, ADR-0074 decision 6, M10 criterion 11).
 *
 * <p>⚠️ THE BUDGET IS 62 OVER {@code -2..2} SO THE ARITHMETIC IS EXACT:
 * {@code Σ 2^k} is {@code 31/4}, so lane {@code l}'s share is {@code 2^(l+3)}
 * and its floor {@code 2^(l+2)} -- 1, 2, 4, 8, 16 -- with no rounding to hide a
 * dropped factor behind.
 *
 * <p>The two mutations this must kill (SPEC test plan): STRICT PRIORITY, a
 * saturated pod that still admits the highest lane (or refuses a lower one
 * below its floor), and FLOOR IGNORED, a saturated pod that refuses every lane.
 */
class LaneAdmissionTest {

    private static final int BUDGET = 62;

    private static List<LaneAdmission.Permit> fill(LaneAdmission admission, byte lane, int n) {
        List<LaneAdmission.Permit> held = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            held.add(admission.tryAcquire(lane).orElseThrow(
                    () -> new AssertionError("refused while filling lane " + lane)));
        }
        return held;
    }

    /** How many more requests of {@code lane} are admitted before the first refusal. */
    private static int admittedUntilRefused(LaneAdmission admission, byte lane) {
        int admitted = 0;
        while (admission.tryAcquire(lane).isPresent()) {
            admitted++;
            if (admitted > 10 * BUDGET) {
                throw new AssertionError("lane " + lane + " was never refused");
            }
        }
        return admitted;
    }

    @Test
    void withTheBudgetFreeEveryLaneIsAdmittedWhateverItsFloor() {
        LaneAdmission admission = new LaneAdmission(BUDGET, LaneSet.defaults());

        for (byte lane = -2; lane <= 2; lane++) {
            assertThat(admission.tryAcquire(lane)).as("lane %d on an idle pod", lane).isPresent();
        }
        // ⚠️ Lane -2's floor is 1, and it is admitted far past it while the
        // POD is under budget: the floor is a guarantee, not a cap.
        fill(admission, (byte) -2, BUDGET - 5);
        assertThat(admission.inFlight()).isEqualTo(BUDGET);
    }

    @Test
    void withTheBudgetSaturatedALaneIsAdmittedOnlyWhileBelowItsFloor() {
        LaneAdmission admission = new LaneAdmission(BUDGET, LaneSet.defaults());
        fill(admission, (byte) -2, BUDGET);

        assertThat(admission.tryAcquire((byte) -2))
                .as("lane -2 is saturating the pod and far above its floor of 1").isEmpty();
        // ⚠️ Strict priority admits lane +2 without end; a floor-ignoring
        // admission refuses it at once. Its floor is 16.
        assertThat(admittedUntilRefused(admission, (byte) 2)).isEqualTo(16);
        assertThat(admittedUntilRefused(admission, (byte) -1))
                .as("a LOW lane below its floor is still admitted on a saturated pod")
                .isEqualTo(2);
    }

    @Test
    void aLanesFloorIsProportionalToTwoToTheLane() {
        LaneAdmission admission = new LaneAdmission(BUDGET, LaneSet.defaults());
        fill(admission, (byte) 0, BUDGET);

        // Lane 0 fills the pod, so it is above its own floor and gets nothing.
        int[] floors = new int[5];
        for (byte lane = -2; lane <= 2; lane++) {
            floors[lane + 2] = admittedUntilRefused(admission, lane);
        }
        assertThat(floors).containsExactly(1, 2, 0, 8, 16);

        LaneAdmission fresh = new LaneAdmission(BUDGET, LaneSet.defaults());
        fill(fresh, (byte) -2, BUDGET);
        assertThat(admittedUntilRefused(fresh, (byte) 0)).isEqualTo(4);
    }

    @Test
    void theFloorIsAtLeastOneAndTheWeightsAreTheActiveSetsOnly() {
        // Budget 1 over five lanes: every share is under 2, every floor still 1.
        LaneAdmission tiny = new LaneAdmission(1, LaneSet.defaults());
        fill(tiny, (byte) 0, 1);
        for (byte lane = -2; lane <= 2; lane++) {
            assertThat(admittedUntilRefused(tiny, lane)).as("lane %d", lane)
                    .isEqualTo(lane == 0 ? 0 : 1);
        }

        // {-1, 0, 1} over 28: Σ is 7/2, shares 4, 8, 16, floors 2, 4, 8 -- a
        // lane outside the set would change Σ, and with it every floor.
        LaneAdmission three = new LaneAdmission(28, LaneSet.of((byte) -1, (byte) 0, (byte) 1));
        fill(three, (byte) 0, 28);
        assertThat(admittedUntilRefused(three, (byte) -1)).isEqualTo(2);
        assertThat(admittedUntilRefused(three, (byte) 1)).isEqualTo(8);
    }

    @Test
    void aReleasedPermitIsReturnedOnceAndReadmitsItsLane() {
        LaneAdmission admission = new LaneAdmission(BUDGET, LaneSet.defaults());
        List<LaneAdmission.Permit> held = fill(admission, (byte) -2, BUDGET);
        assertThat(admission.tryAcquire((byte) -2)).isEmpty();

        LaneAdmission.Permit one = held.get(0);
        one.release();
        one.release();

        assertThat(admission.inFlight()).as("a second release returns nothing")
                .isEqualTo(BUDGET - 1);
        assertThat(admission.tryAcquire((byte) -2)).as("under budget again").isPresent();
        assertThat(admission.tryAcquire((byte) -2)).isEmpty();
        for (LaneAdmission.Permit p : held) {
            p.close();
        }
        assertThat(admission.inFlight()).isEqualTo(1);
    }

    @Test
    void aWideOrSparseActiveSetStillWeighsItsLanesByTwoToTheLane() {
        // ⚠️ {-128, 0} spans 2^128, and {-63, 0, 2} spans 2^65: a `long` weight
        // wraps there and inverted the priority (review round 1).
        LaneAdmission widest = new LaneAdmission(256, LaneSet.of((byte) -128, (byte) 0));
        fill(widest, (byte) -128, 256);
        assertThat(admittedUntilRefused(widest, (byte) 0))
                .as("lane 0 holds all but a 2^-128 sliver of the half-budget").isEqualTo(127);
        assertThat(admittedUntilRefused(widest, (byte) -128)).isZero();

        LaneAdmission lowFull = new LaneAdmission(256, LaneSet.of((byte) -128, (byte) 0));
        fill(lowFull, (byte) 0, 256);
        assertThat(admittedUntilRefused(lowFull, (byte) -128)).as("the floor of one").isEqualTo(1);

        // Σ = 2^-63 + 1 + 4: lane +2's floor ⌊128 × 4 ÷ 5.0…⌋ = 102, lane 0's 25.
        LaneAdmission sparse = new LaneAdmission(256,
                LaneSet.of((byte) -63, (byte) 0, (byte) 2));
        fill(sparse, (byte) -63, 256);
        assertThat(admittedUntilRefused(sparse, (byte) 2)).isEqualTo(102);
        assertThat(admittedUntilRefused(sparse, (byte) 0)).isEqualTo(25);
        assertThat(admittedUntilRefused(sparse, (byte) -63)).isZero();
    }

    @Test
    void aLaneAtItsFloorOnASaturatedPodIsReadmittedWhenItReleasesOne() {
        LaneAdmission admission = new LaneAdmission(BUDGET, LaneSet.defaults());
        fill(admission, (byte) -2, BUDGET);
        List<LaneAdmission.Permit> atFloor = fill(admission, (byte) 2, 16);
        assertThat(admission.tryAcquire((byte) 2)).as("at its floor of 16").isEmpty();

        atFloor.get(0).release();

        assertThat(admission.inFlight()).as("still saturated").isGreaterThan(BUDGET);
        assertThat(admission.tryAcquire((byte) 2))
                .as("the LANE's count came down, not only the pod's").isPresent();
        assertThat(admission.tryAcquire((byte) 2)).isEmpty();
    }

    @Test
    void anInactiveLaneHasNoFloorAndIsAdmittedOnlyUnderBudget() {
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0));

        assertThat(admission.tryAcquire((byte) 2)).as("the ingester answers it 400 later")
                .isPresent();
        admission.tryAcquire((byte) 0).orElseThrow();
        assertThat(admission.tryAcquire((byte) 2)).as("saturated, and it has no floor")
                .isEmpty();
    }

    @Test
    void aBudgetBelowOneIsRefused() {
        assertThatThrownBy(() -> new LaneAdmission(0, LaneSet.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightBulk");
    }
}
