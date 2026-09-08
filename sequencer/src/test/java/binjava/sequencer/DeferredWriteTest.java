// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Delayed writes and reordered completions, the last two fault classes (M4.13b).
 *
 * <p>⚠️ THE INJECTOR COULD NOT MODEL TIME. Every other class resolves inside
 * the call that triggered it: the write fails, or lands, or lands twice, or is
 * withheld — all before the caller returns. A DELAYED write is different in
 * kind, because its effect happens after the call that caused it, and the
 * driver is a synchronous act-or-fail loop with nowhere to put a pending
 * operation. That is why M4's SPEC listed these two and the simulation shipped
 * without them.
 *
 * <p>⚠️ ONE MECHANISM GIVES BOTH. A write that is held and landed later is a
 * DELAYED write; several held writes landed in an order other than the one they
 * were issued in are REORDERED COMPLETIONS. Modelling them separately would be
 * two mechanisms for one fact about the world.
 *
 * <p>⚠️ AND IT IS OBSERVABLE PRECISELY BECAUSE OF M4.50, whose observer this
 * commit also wires into {@link CommitProtocolSimulation}. The acknowledgement
 * trace's CONFIRMED half now comes from the store, at the moment a write
 * actually lands, so a drained write confirms late and out of order — exactly
 * the input I5's ack clause exists to judge. Before that fix the trace was
 * synthesised from each {@code commit()} return, so a reordered completion
 * would have been invisible no matter how faithfully it was injected.
 */
class DeferredWriteTest {

    private static final String PREFIX = "bins/cluster-a";

    private static Body body(String s) {
        byte[] b = s.getBytes();
        return new Body(b.length, () -> new ByteArrayInputStream(b));
    }

    private static String key(long seq) {
        return new LogKeys(PREFIX, 1L).keyFor(seq);
    }

    private static FaultInjectingStore alwaysDeferring(MemoryBinStore backing) {
        return new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 1.0));
    }

    @Test
    void aDEFERREDWriteDoesNotLandWhenItIsIssued() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = alwaysDeferring(backing);

        assertThatThrownBy(() -> store.putIfAbsent(key(0), body("zero")))
                .as("the caller does not learn the outcome -- that is what a delay IS")
                .hasMessageContaining("deferred");

        assertThat(backing.stat(key(0)))
                .as("and nothing has landed yet")
                .isEmpty();
        assertThat(store.pendingWrites()).as("it is held, not lost").isEqualTo(1);
    }

    @Test
    void drainingLandsIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = alwaysDeferring(backing);
        try {
            store.putIfAbsent(key(0), body("zero"));
        } catch (Exception expected) {
            // held
        }

        store.drainPending();

        // ⚠️ THIS IS THE HALF THAT MAKES IT A DELAY RATHER THAN A LOSS. A write
        // that never lands is `withheldPut`, which the injector already models.
        // The defect a delay causes is that the world changes UNDER a caller
        // who has already decided what to do about the failure.
        assertThat(backing.stat(key(0))).as("the held write lands later").isPresent();
        assertThat(store.pendingWrites()).as("and the queue drains").isZero();
    }

    @Test
    void heldWritesLandInAnOrderOtherThanTheyWereIssuedIn() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        List<AckOrderInvariants.AckEvent> landed = new ArrayList<>();
        AckTraceStore observed = new AckTraceStore(backing, landed::add);
        FaultInjectingStore store = new FaultInjectingStore(observed, 7L,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 1.0));

        for (long seq = 0; seq < 6; seq++) {
            try {
                store.putIfAbsent(key(seq), body("v" + seq));
            } catch (Exception expected) {
                // all six are held
            }
        }
        store.drainPending();

        List<Long> order = landed.stream()
                .map(AckOrderInvariants.AckEvent::sequence).toList();
        assertThat(order).as("every held write eventually lands")
                .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L);
        // ⚠️ THE ASSERTION IS THAT THE ORDER CAN DIFFER, not that it always
        // does: a shuffle is allowed to return the identity permutation, and a
        // test demanding otherwise would be asserting against chance. The seed
        // is fixed, so this is a deterministic statement about THIS run.
        assertThat(order).as("and the completion order is not the issue order -- "
                        + "which is what 'reordered completions' means")
                .isNotEqualTo(List.of(0L, 1L, 2L, 3L, 4L, 5L));
    }

    @Test
    void theDrainOrderIsSEEDEDSoAFailingRunIsAPermanentRegression() throws Exception {
        // ⚠️ FOR THIS CLASS THE ORDER *IS* THE FAULT, so an unseeded shuffle
        // makes the one thing being injected unreproducible. Review MEASURED
        // the gap: replacing `new Random(scramble(seed, 6))` with
        // `new Random()` left the entire sequencer suite green, because the
        // only order assertion was "not the issue order", which a random
        // permutation of six satisfies 719 times in 720. A sweep seed failing
        // on one particular completion ordering would then not reproduce and
        // would be filed as a flake.
        assertThat(drainOrderFor(42L)).as("the same seed drains the same way, always")
                .isEqualTo(drainOrderFor(42L));
        // ⚠️ AND THE OTHER DIRECTION, which is what kills a fixed permutation:
        // replacing the shuffle with `Collections.reverse` also survived, and
        // one order explored across every seed is "a single scenario dressed
        // as a fault class".
        assertThat(drainOrderFor(42L)).as("and different seeds explore different orders")
                .isNotEqualTo(drainOrderFor(43L));
    }

    /** The order six held writes land in, under one seed. */
    private static List<Long> drainOrderFor(long seed) throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        List<AckOrderInvariants.AckEvent> landed = new ArrayList<>();
        AckTraceStore observed = new AckTraceStore(backing, landed::add);
        FaultInjectingStore store = new FaultInjectingStore(observed, seed,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 1.0));
        for (long seq = 0; seq < 6; seq++) {
            try {
                store.putIfAbsent(key(seq), body("v" + seq));
            } catch (Exception held) {
                // every one is held
            }
        }
        store.drainPending();
        return landed.stream().map(AckOrderInvariants.AckEvent::sequence).toList();
    }

    @Test
    void aFAILINGDrainPutsTheRestBackRatherThanLosingThem() throws Exception {
        // ⚠️ OTHERWISE A DELAY BECOMES A LOSS SILENTLY. `pending` is cleared
        // before anything lands, so a throw partway through would leave the
        // remaining writes in neither the queue nor the store, with
        // `pendingWrites()` reporting 0 -- `withheldPut` writes that were
        // never labelled as such.
        MemoryBinStore backing = new MemoryBinStore();
        var failing = new RefuseKeyStore(backing, key(3));
        FaultInjectingStore store = new FaultInjectingStore(failing, 11L,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 1.0));
        for (long seq = 0; seq < 6; seq++) {
            try {
                store.putIfAbsent(key(seq), body("v" + seq));
            } catch (Exception held) {
                // held
            }
        }
        try {
            store.drainPending();
        } catch (Exception thrown) {
            // the refusing key blew up the drain
        }
        assertThat(store.pendingWrites())
                .as("what could not land is still held, not silently dropped")
                .isGreaterThan(0);
    }

    @Test
    void heldWritesLandWHILETheRunIsStillGoingNotAllAtTheEnd() throws Exception {
        // ⚠️ THE DIFFERENCE BETWEEN A DELAY AND A DEFERRAL TO THE END, and
        // review MEASURED that nothing saw it: deleting the driver's
        // round-boundary drain left every test green, because the final drain
        // before judging still landed everything. The writes arrived, the
        // invariants held, and no assertion could tell that they had all
        // arrived at once instead of interleaved with the rounds after them.
        // Interleaving IS the fault -- a write issued against one world
        // landing in another -- so it has to be countable.
        var run = CommitProtocolSimulation.run(5L, 60, 3,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 0.30),
                new MemoryBinStore());
        assertThat(run.midRunDrained())
                .as("some held write landed at a round boundary, with rounds still to come")
                .isGreaterThan(0);
    }

    @Test
    void aDeferredWriteIsRecordedAsItsOwnFaultClass() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = alwaysDeferring(backing);
        try {
            store.putIfAbsent(key(0), body("zero"));
        } catch (Exception expected) {
            // held
        }
        assertThat(store.injected())
                .as("countable like every other class, or the evidence test cannot see it")
                .anySatisfy(i -> assertThat(i.kind()).isEqualTo("deferredPut"));
    }

    @Test
    void aDrainedWriteSTILLOBEYSWriteOnce() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = alwaysDeferring(backing);

        // Someone else wins the slot while the write is held. This is the
        // ⚠️ RACE A DELAY CREATES and the reason the class is worth modelling:
        // the held write is issued against one world and lands in another.
        backing.putIfAbsent(key(0), body("winner"));
        try {
            store.putIfAbsent(key(0), body("loser"));
        } catch (Exception expected) {
            // held
        }
        store.drainPending();

        try (var in = backing.get(key(0))) {
            assertThat(new String(in.readAllBytes()))
                    .as("putIfAbsent is write-once whenever the write lands -- I1 does "
                            + "not weaken because a write was slow")
                    .isEqualTo("winner");
        }
    }
}
