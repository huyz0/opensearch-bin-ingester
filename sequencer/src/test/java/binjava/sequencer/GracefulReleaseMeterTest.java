// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import org.junit.jupiter.api.Test;

/**
 * That {@link GracefulReleaseMeter} can COUNT the thing it exists to detect
 * (M5.28).
 *
 * <p>⚠️ NO ASSERTION OVER THE UNMODIFIED SWEEP'S TOTALS CAN PIN THIS --
 * narrower than "no sweep assertion can", and the narrower claim is the true
 * one: {@code aBrokenReaderViewREACHESTheSweepsVerdict} is the counter-shape,
 * driving the real simulation through {@code run}'s injection seam with a
 * deliberately broken input. This is still the right answer because it needs
 * no new seam on {@code run}. On a
 * correct tree {@code refusedForAnotherPod} is legitimately ZERO, so deleting
 * its increment changes nothing the 1,000-seed sweep observes -- MEASURED: that
 * mutation survives green even with the refusals-seen floor added, which kills
 * the other two members of its family (a misspelt scan predicate and an emptied
 * window). A detector whose positive branch never fires in the suite is a
 * detector nothing constrains, and the only way to fire it is to build the
 * situation deliberately.
 */
class GracefulReleaseMeterTest {

    /**
     * A refusal blaming a pod OTHER than the releasing leader is counted.
     *
     * <p>⚠️ THE RELEASE ITSELF MOVES THE ACTOR, which is the one way to reach
     * this branch now that {@code observe} sets it. That is not artificial: it
     * is the shape of the defect M5.26 fixed, where the actor was left at
     * whoever ran last and the store judged the release against them --
     * {@code ForwardingPods} already moves the actor mid-window and restores it
     * in a {@code finally}, so this is the harness's own shape with the restore
     * missing. Here the situation is built rather than waited for.
     *
     * <p>⚠️ WHAT THIS CASE DOES NOT PIN, and what its SIBLINGS do. Deleting
     * {@code observe}'s {@code actingAs} leaves THIS case green, because the
     * meter reads only {@code injected()} and cannot tell "the actor moved
     * inside the window" from "the actor was never set". It reds
     * {@code theLEADERSOWNPartitionIsSEENButBlamedOnNOBODYElse} instead,
     * because with no actor set {@code isPartitioned()} reads null, nothing is
     * injected, and the refusal it asserts never happens.
     * {@code TWORefusalsInOneWindowAreCountedTWICE} reds for the same reason. ⚠️ THOSE TWO ARE THE DIRECT PINS ON {@code actingAs} HERE, and
     * deleting either costs more than it looks. ⚠️ NO COUNT IS GIVEN, on
     * purpose: this paragraph has said "every case is green", then "1 of 5",
     * and review falsified both -- each time because a case added beside it in
     * the same round was not re-measured. A number here goes stale on the next
     * case; the names do not. The driver-side call site is
     * what remains sweep-only, at 5 seeds in 1,000. ⚠️ AND THE EQUATION IS
     * APPROXIMATE: any non-leader-blamed refusal in the window is read as the
     * defect, so a legitimately forwarded release whose peer is partitioned
     * would look identical. Not reachable today -- {@code leader::close} closes
     * a {@code LocalSequencer}, which does not hop.
     */
    @Test
    void aRefusalBLAMINGANOTHERPODIsCountedAndTheReleaseIsNotCredited() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        faulty.partition("podb");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            faulty.actingAs("podb");
            faulty.stat("ctl/lease");
        });

        assertThat(meter.refusedForAnotherPod())
                .as("the store refused blaming podb while poda was releasing")
                .isEqualTo(1);
        assertThat(meter.refusalsSeen())
                .as("and the refusal was seen at all, which is what the sweep's floor rests on")
                .isEqualTo(1);
        assertThat(meter.released())
                .as("a release refused is not a release completed")
                .isZero();
    }

    /**
     * The LEADER's OWN partition is seen, and blamed on nobody else.
     *
     * <p>⚠️ THIS IS THE CARVE-OUT, and it is the case the sweep cannot reach:
     * a leader refused by its own partition is the ungraceful path working
     * correctly, so {@code refusedForAnotherPod} must stay 0 while
     * {@code refusalsSeen} counts it. Without this, deleting the carve-out is
     * caught only by a sweep total.
     */
    @Test
    void theLEADERSOWNPartitionIsSEENButBlamedOnNOBODYElse() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> faulty.stat("ctl/lease"));

        assertThat(meter.refusalsSeen())
                .as("the store refused, and the meter must see it")
                .isEqualTo(1);
        assertThat(meter.refusedForAnotherPod())
                .as("but it blamed the releasing leader itself, which is the ungraceful path "
                        + "rather than the defect")
                .isZero();
    }

    /**
     * A fault that is NOT a partition is not counted as a refusal.
     *
     * <p>⚠️ IT PINS THE COUNTER TO ITS PREDICATE. Review MEASURED that hoisting
     * {@code refusalsSeen++} above the kind guard leaves the sweep AND every
     * other case here green -- at 124 rather than 38, carrying 86 non-partition
     * faults -- and green even composed with a misspelt predicate. A control
     * that counts anything is not a control for anything.
     */
    @Test
    void aNONPARTITIONFaultInTheWindowIsNotCountedAsARefusal() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, new FaultInjectingStore.Faults(1, 0, 0, 0));
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> faulty.list("ctl/", null, 10));

        assertThat(faulty.injected())
                .as("the window must contain exactly the fault this case is named for, or it "
                        + "pins nothing -- the store is fresh, so its whole list IS the window")
                .extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("unreachable");
        assertThat(meter.refusalsSeen())
                .as("an injected `unreachable` is not a partition refusal")
                .isZero();
        assertThat(meter.refusedForAnotherPod()).isZero();
    }

    /**
     * TWO refusals in one window are counted TWICE (M5.28).
     *
     * <p>⚠️ IT PINS THE COUNTER AS A COUNTER. Review MEASURED two survivors
     * without it: {@code refusalsSeen++} rewritten to {@code refusalsSeen = 1},
     * and an unconditional {@code break} at the end of the loop. Both leave all
     * other cases AND the 1,000-seed sweep green, because every window in the
     * tree carries at most one fault -- the sweep's 38 refusals fall on 37
     * seeds -- so the counter is never observed above 1 and can silently become
     * "did this window see one" with nothing red.
     *
     * <p>⚠️ THE FIRST FAILURE IS SWALLOWED INSIDE THE LAMBDA, which is what
     * lets the window hold two: a refusal aborts the release, so a second call
     * only happens if the first throw is caught. That makes this the case the
     * meter's own comment describes as separating refusals from windows.
     */
    @Test
    void TWORefusalsInOneWindowAreCountedTWICE() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            try {
                faulty.stat("ctl/lease");
            } catch (java.io.IOException swallowed) {
                // deliberately: a release that keeps calling is what separates
                // "refusals" from "windows carrying one"
            }
            faulty.stat("ctl/lease");
        });

        assertThat(meter.refusalsSeen())
                .as("both refusals are counted, not the window they share")
                .isEqualTo(2);
        assertThat(meter.refusedForAnotherPod())
                .as("and both blamed the leader itself")
                .isZero();
    }

    /** A clean release is credited, and blames nobody. */
    @Test
    void aCLEANReleaseIsCreditedAndCountsNoRefusal() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> faulty.stat("ctl/lease"));

        assertThat(meter.released())
                .as("it reached the store and returned")
                .isEqualTo(1);
        assertThat(meter.refusalsSeen()).isZero();
        assertThat(meter.refusedForAnotherPod()).isZero();
    }

    /** A release that never touches the store is NOT credited. */
    @Test
    void aReleaseThatNEVERREACHESTheStoreIsNotCredited() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> { });

        assertThat(meter.released())
                .as("returning is not releasing -- an empty lambda returns perfectly well")
                .isZero();
    }
}
