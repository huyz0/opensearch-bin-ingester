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
     * A release the leader's OWN partition should have refused, and did not.
     *
     * <p>⚠️ THE OTHER DIRECTION OF M5.26's DEFECT, and nothing counted it.
     * A stale acting pod can make the store REFUSE a release it should have
     * allowed -- that is {@code refusedForAnotherPod}, and the sweep asserts it
     * is zero -- but it can equally make the store ALLOW one it should have
     * refused, because the pod it judges is un-partitioned while the LEADER is
     * cut off. The sweep sees that direction only in totals that move: review
     * MEASURED seed 323 runs 3 releases and 9 commits on a CORRECT tree and 4 and 13 UNDER THE DEFECT, with zero wrongly-blamed refusals either way,
     * so the outcomes moved and no assertion noticed.
     *
     * <p>⚠️ IT IS THE MORE DAMAGING DIRECTION. A wrongly REFUSED release
     * leaves the lease to expire, which is merely the ungraceful path the
     * cluster already tolerates. A wrongly ALLOWED one hands the lease back on
     * behalf of a leader that is PARTITIONED and therefore cannot know it has
     * been released -- and it is credited as a graceful release while doing so,
     * so `gracefulReleases`'s own floor is met by releases that should not
     * have happened.
     *
     * <p>⚠️ DISTINCT FROM M5.28, which is about the detector being
     * blindable. This is a direction the detector was never pointed at.
     */
    @Test
    void aReleaseTheLEADERSOwnPartitionShouldHaveREFUSEDIsCounted() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            // ⚠️ THE STALE ACTOR, which is M5.26's defect verbatim: the store
            // judges the release against whoever acted last, and podb is not
            // cut off, so the conditional write SUCCEEDS.
            faulty.actingAs("podb");
            faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held);
        });

        assertThat(meter.releasedDespiteOwnPartition())
                .as("poda was cut off; its release must not have gone through")
                .isEqualTo(1);
        assertThat(meter.released())
                .as("and the harm is that it was CREDITED -- the graceful-release floor is met "
                        + "by a release that should have been refused")
                .isEqualTo(1);
        assertThat(meter.refusedForAnotherPod())
                .as("the refusal direction saw nothing -- HELD BY CONSTRUCTION, not by the "
                        + "meter's logic: `Faults.none()` with podb un-partitioned leaves the "
                        + "injected window empty, so every mutant of the scan loop leaves this "
                        + "green. It is here to show the two directions are DIFFERENT, not as "
                        + "evidence that they are independent under mutation")
                .isZero();
    }

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
        assertThat(faulty.calls("stat"))
                .as("and a REFUSED call is still counted, which is what `calls(String)`'s "
                        + "javadoc says and what the pre-M5.29 `calls++` did -- moving `record` "
                        + "below `refuseIfPartitioned` was otherwise unconstrained")
                .isEqualTo(1);
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
        assertThat(faulty.calls("list"))
                .as("and the call is COUNTED even though its injected fault fired before the "
                        + "partition check -- `record` sits at the top of the verb, above the "
                        + "throw, which review measured as otherwise unconstrained")
                .isEqualTo(1);
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

    /**
     * A clean release is credited, and blames nobody.
     *
     * <p>⚠️ ITS STAND-IN FOR A RELEASE CHANGED FROM {@code stat} TO
     * {@code putIfMatch} (M5.29) AND THE ASSERTION DID NOT. Crediting any call
     * that reached the store is what let {@code leases.release()} be replaced
     * by another verb with the sweep's floor green at 1,626; a release is a
     * CONDITIONAL WRITE of the expired lease, which is what
     * {@code LeaseManager.releaseLocked} does. So this case now does what it
     * always claimed to.
     */
    @Test
    void aCLEANReleaseIsCreditedAndCountsNoRefusal() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda",
                () -> faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held));

        assertThat(meter.released())
                .as("it wrote the expired lease back and returned")
                .isEqualTo(1);
        assertThat(meter.refusalsSeen()).isZero();
        assertThat(meter.refusedForAnotherPod()).isZero();
    }

    /**
     * A release whose write LANDED but which then THREW is not credited.
     *
     * <p>⚠️ THE {@code completed} CONJUNCT WAS UNPINNED, AND NOT MERELY IN
     * THEORY. Review MEASURED dropping it from the credit guard surviving the
     * whole suite, and the 1,000-seed sweep then counts 1,732 instead of 1,608
     * -- so **124 windows per 1,000 seeds move the verb counter and then
     * throw**. ⚠️ NOT ONE ARM, AND NOT ALL OF THEM WROTE ANYTHING:
     * {@code record} runs at the TOP of {@code putIfMatch}, which then throws
     * from THREE places -- {@code refuseIfPartitioned} (nothing reaches the
     * delegate at all), the {@code ambiguousPut} arm (the write lands, the
     * response is lost) and the {@code withheldPut} arm (whose own comment says
     * the version does not move). The sweep enables all three, and its 38
     * refusal windows sit INSIDE the 124 with nothing written -- M5.28
     * independently measured 124 faults in release windows carrying 86
     * non-partition, and 124 = 38 + 86. ⚠️ WHICH MAKES THE CONJUNCT MORE
     * NECESSARY, NOT LESS: crediting these would credit releases where nothing
     * reached the store. Every {@code gracefulReleases} assertion is a FLOOR,
     * so 1,732 clears them all and nothing reds.
     *
     * <p>⚠️ IT IS UNCREDITED ON PURPOSE, not by omission. The lease really
     * is expired on the store, so a successor does not wait out the TTL -- but
     * the releasing pod cannot know that, {@code LocalSequencer.close}
     * propagates, and the pod dies reporting a failed release. What this meter
     * counts is the path COMPLETING, which is the claim the sweep's floor
     * carries. Crediting an ambiguous landing would be a different and larger
     * claim about what the caller can observe.
     */
    @Test
    void aReleaseWhoseWriteLANDEDButTHREWIsNOTCredited() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held);
            throw new java.io.IOException("the response was lost after the write landed");
        });

        assertThat(meter.released())
                .as("the write reached the store, so the verb counter moved -- only "
                        + "`completed` says the release finished, and it did not")
                .isZero();
        assertThat(faulty.calls("putIfMatch"))
                .as("and the premise: the write really did happen, or this pins nothing")
                .isEqualTo(1);
    }

    /**
     * A conditional write REFUSED by a partition is still counted.
     *
     * <p>⚠️ THE OTHER HALF OF THE POSITION PIN. Round 2 measured
     * {@code record} moved below {@code refuseIfPartitioned} surviving for BOTH
     * {@code stat} and {@code putIfMatch}; round 3 closed the {@code stat} half
     * in {@link #theLEADERSOWNPartitionIsSEENButBlamedOnNOBODYElse} and left
     * the verb the meter actually CREDITS open. Closing one of two is the
     * failure mode this commit has now recorded twice.
     *
     * <p>⚠️ "REFUSED ONES INCLUDED" IS {@code calls(String)}'s CONTRACT and
     * was the pre-M5.29 behaviour too, where {@code calls++} sat above the
     * {@code isPartitioned()} test. Nothing is credited here -- the throw makes
     * {@code completed} false -- so what this pins is the COUNTER, not the
     * verdict.
     */
    @Test
    void aConditionalWriteREFUSEDByAPartitionIsStillCOUNTED() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda",
                () -> faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held));

        assertThat(faulty.calls("putIfMatch"))
                .as("counted above `refuseIfPartitioned`, which is what \"refused ones "
                        + "included\" means and what `calls++` did before M5.29")
                .isEqualTo(1);
        assertThat(meter.released())
                .as("and refused is not released")
                .isZero();
        assertThat(meter.refusalsSeen()).isEqualTo(1);
    }

    /**
     * The store's OWN ambiguous write is counted, and still not credited.
     *
     * <p>⚠️ THE SIBLING OF THE CASE ABOVE, BUILT FROM THE INJECTOR RATHER
     * THAN THE LAMBDA. There the test threw; here {@code FaultInjectingStore}
     * does, from its {@code ambiguousPut} arm -- the write reaches the delegate
     * and the call reports failure anyway. That is ONE of the three arms behind
     * the sweep's 124 windows per 1,000 seeds (the others being
     * {@code refuseIfPartitioned} and {@code withheldPut}, neither of which
     * writes anything), and it is the only one that LANDS a write, so it is
     * worth exercising through the real arm and not only through a
     * hand-written throw.
     *
     * <p>⚠️ IT PINS WHERE {@code record} SITS. Review MEASURED
     * {@code record("putIfMatch")} moved BELOW the ambiguous throw surviving
     * the whole suite including the 1,000-seed sweep -- which would falsify the
     * 1,732-against-1,608 figure this commit's evidence rests on, because the
     * windows that throw would stop being counted at all.
     */
    @Test
    void theStoresOWNAmbiguousWriteIsCOUNTEDAndStillNotCredited() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(0, 1, 0, 0));
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda",
                () -> faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held));

        assertThat(faulty.injected())
                .as("the window must contain the fault this case is named for")
                .extracting(FaultInjectingStore.Injected::kind)
                .contains("ambiguousPut");
        assertThat(faulty.calls("putIfMatch"))
                .as("counted at the TOP of the verb, above the injected throw")
                .isEqualTo(1);
        assertThat(meter.released())
                .as("the write landed, the call failed, and the releasing pod cannot tell -- "
                        + "so the polite path did not COMPLETE")
                .isZero();
    }

    /**
     * A release that only READ the store is not a release (M5.29).
     *
     * <p>⚠️ "THE STORE DID WORK" IS NOT "THE LEASE WAS RELEASED", and the
     * gap is what review MEASURED: replacing {@code leases.release()} in
     * {@code LocalSequencer.close} with another store verb left the sweep's
     * {@code gracefulReleases} floor green at 1,626, because {@code stat}
     * reaches the store exactly as a release does. Only deleting the store
     * call outright redded it, at 0 -- so the COUNT did not generalise but the
     * blindness did.
     */
    @Test
    void aReleaseThatONLYREADTheStoreIsNOTCredited() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> faulty.stat("ctl/lease"));

        assertThat(meter.released())
                .as("a stat is store work and is not a release; crediting ANY call is what let "
                        + "the release be swapped for another verb with the sweep green")
                .isZero();
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

    /**
     * A PARTITION refusal AFTER a non-partition fault in the same window is
     * still counted (M5.35).
     *
     * <p>⚠️ THE KIND GUARD IS A SKIP, NOT A STOP, and nothing said so.
     * MEASURED: {@code continue;} -> {@code break;} in that guard is killed by
     * THIS CASE AND BY NOTHING ELSE in the sequencer module's 501 tests, the
     * 1,000-seed sweep included. It blinds the meter to a partition refusal
     * FOLLOWING a non-partition fault in one window -- M5.28's family, at a
     * decision point M5.28 itself created by splitting a compound {@code if}
     * into a guard plus an inner test.
     *
     * <p>⚠️ IT HAS TO BE A SIBLING RATHER THAN A LINE ADDED TO
     * {@code aNONPARTITIONFaultInTheWindowIsNotCountedAsARefusal}, which
     * already builds a window with an {@code unreachable} in it. {@code list}
     * draws its {@code unreachable} ABOVE {@code refuseIfPartitioned}, so a
     * partitioned actor calling {@code list} records no partition at all --
     * and adding one to that case falsifies both its
     * {@code containsExactly("unreachable")} and its {@code isZero()}.
     */
    @Test
    void aPARTITIONRefusalAFTERANonPartitionFaultIsSTILLCounted() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, new FaultInjectingStore.Faults(1, 0, 0, 0));
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            try {
                faulty.list("ctl/", null, 10);
            } catch (java.io.IOException swallowed) {
                // deliberately: `list` draws its `unreachable` ABOVE
                // `refuseIfPartitioned`, so a partitioned actor calling it
                // records no partition -- and a refusal aborts the release, so
                // the second call only happens if the first throw is caught
            }
            faulty.stat("ctl/lease");
        });

        assertThat(faulty.injected())
                .as("the window must hold the non-partition fault FIRST and the refusal "
                        + "AFTER it, or this pins nothing -- the store is fresh, so its whole "
                        + "list IS the window")
                .extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("unreachable", "partition");
        assertThat(meter.refusalsSeen())
                .as("the scan SKIPS the `unreachable` and goes on to the refusal behind it; "
                        + "stopping there instead counts 0")
                .isEqualTo(1);
        assertThat(meter.refusedForAnotherPod()).isZero();
    }

    /**
     * TWO refusals both blaming ANOTHER pod count the WINDOW once (M5.35).
     *
     * <p>⚠️ THE {@code break} AFTER {@code refusedForAnotherPod++} WAS
     * UNPINNED. MEASURED: deleting it is killed by THIS CASE AND BY NOTHING
     * ELSE in the sequencer module's 501 tests, the 1,000-seed sweep included,
     * and it turns that counter from WINDOWS into REFUSALS while
     * {@code observe}'s own inline comment beside it states the distinction as
     * fact. Not reachable on today's single-call release path, so this is
     * M5.29's staleness class rather than live loss.
     *
     * <p>⚠️ AND THE SAME {@code break} TRUNCATES {@code refusalsSeen}, which
     * is why this case asserts that too: the increment sits ABOVE the key
     * test, so the scan counts every refusal up to AND INCLUDING the first
     * that blames another pod, and then stops. A window holding two of them
     * reads 1, not 2. {@code TWORefusalsInOneWindowAreCountedTWICE} is the
     * other half -- two refusals blaming the LEADER, where nothing breaks and
     * the count is 2.
     */
    @Test
    void TWORefusalsBLAMINGANOTHERPODCountTheWINDOWOnce() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        faulty.partition("podb");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> {
            faulty.actingAs("podb");
            try {
                faulty.stat("ctl/lease");
            } catch (java.io.IOException swallowed) {
                // deliberately: a refusal aborts the release
            }
            faulty.stat("ctl/lease");
        });

        assertThat(faulty.injected())
                .as("the premise: the window really holds TWO refusals and both blame podb, "
                        + "not the releasing leader")
                .extracting(FaultInjectingStore.Injected::key)
                .containsExactly("pod:podb", "pod:podb");
        assertThat(meter.refusedForAnotherPod())
                .as("ONE WINDOW, however many refusals it carried -- without the break this "
                        + "is 2 and the counter silently changes unit")
                .isEqualTo(1);
        assertThat(meter.refusalsSeen())
                .as("and the same break stops this scan too, so the refusal counter is "
                        + "TRUNCATED here rather than reaching 2 -- stated because the "
                        + "accessor's javadoc contrasts the two units and this is where they "
                        + "coincide")
                .isEqualTo(1);
    }

    /**
     * Refusals ACCUMULATE across windows and are not RE-counted (M5.35).
     *
     * <p>⚠️ TWO MUTATIONS AT THE TOP OF {@code observe} SURVIVED EVERY CASE
     * IN THIS FILE, caught only by the 1,000-seed sweep and the second of them
     * by a margin of ONE (19 against a floor of 20). MEASURED, both: {@code
     * int before = 0} -- which makes every window start at the beginning of
     * the whole injected list, so window two counts window one again -- and
     * {@code refusalsSeen = 0} at the top, which turns the accumulator into a
     * per-window count. Each is killed by THIS CASE and by
     * {@code CommitProtocolSweepTest.theInvariantsHoldAcrossEverySeed}, and by
     * nothing else in the module.
     *
     * <p>⚠️ THE REASON THEY HID IS ONE LINE: every other case here runs a
     * SINGLE {@code observe} on a fresh store, so {@code before} is always 0
     * and the accumulator is never read across windows. Two windows is the
     * whole fixture.
     */
    @Test
    void refusalsACCUMULATEAcrossWindowsAndAreNotRECOUNTED() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        faulty.partition("poda");
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda", () -> faulty.stat("ctl/lease"));
        assertThat(meter.refusalsSeen())
                .as("the premise: window one saw exactly one refusal")
                .isEqualTo(1);

        meter.observe("poda", () -> faulty.stat("ctl/lease"));

        assertThat(meter.refusalsSeen())
                .as("one refusal per window, TWO windows: 3 if the second window rescans the "
                        + "first, 1 if the accumulator is reset at the top of `observe`")
                .isEqualTo(2);
    }

    /**
     * A SECOND window is not credited for the FIRST's write (M5.35).
     *
     * <p>⚠️ THE THIRD OF THE SAME FAMILY AND THE WORST OF THEM, BECAUSE THE
     * SWEEP DOES NOT CATCH IT. MEASURED: {@code int writesBefore = 0} at the
     * top of {@code observe} is killed by THIS CASE and by nothing else in the
     * sequencer module's 501 tests -- unlike {@code int before = 0} and
     * {@code refusalsSeen = 0}, which the sweep also reds. It degrades the
     * credit from "this window issued a conditional write" to "this store has
     * ever seen one", true by lease-acquire time.
     *
     * <p>⚠️ AND IT RESTORES EXACTLY THE BLINDNESS M5.29 REMOVED, measured
     * compound: the production swap alone ({@code leases.release()} ->
     * {@code store.stat(leases.key())}) REDS the {@code gracefulReleases}
     * floor, and the same swap PLUS {@code writesBefore = 0} leaves it GREEN.
     */
    @Test
    void aSECONDWindowIsNotCreditedForTheFIRSTSWrite() throws Exception {
        FaultInjectingStore faulty = new FaultInjectingStore(
                new MemoryBinStore(), 1L, FaultInjectingStore.Faults.none());
        binjava.binstore.Version held = faulty.put("ctl/lease", FaultFixtures.body("held"));
        GracefulReleaseMeter meter = new GracefulReleaseMeter(faulty);

        meter.observe("poda",
                () -> faulty.putIfMatch("ctl/lease", FaultFixtures.body("expired"), held));
        assertThat(meter.released())
                .as("the premise: window one wrote the expired lease back and is credited")
                .isEqualTo(1);

        meter.observe("poda", () -> { });

        assertThat(meter.released())
                .as("the second window touched nothing, so the credit stays at one -- a "
                        + "baseline of 0 credits it again for a write another window issued")
                .isEqualTo(1);
        assertThat(faulty.calls("putIfMatch"))
                .as("and the store really did see exactly one conditional write across both "
                        + "windows, or the assertion above is trivially true")
                .isEqualTo(1);
    }
}
