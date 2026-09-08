// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static binjava.sequencer.DedupFixtures.firstOffsetOf;
import static binjava.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * A replay that crosses a TAKEOVER is answered, not applied twice (M5.1).
 *
 * <p>⚠️ THIS IS THE HALF OF IDEMPOTENCY THAT DID NOT EXIST, and
 * {@link Sequencer}'s own javadoc says so: "A SUCCESSOR INHERITS NOTHING ... a
 * replay that crosses a takeover commits twice. Inheriting it is M4.10f."
 * M4.10f was in no build and was a backlog row nowhere, on this branch or on
 * the archive branch — an obligation named in a contract and owned by nobody.
 *
 * <p>⚠️ FORWARDING IS WHAT MAKES IT REACHABLE, which is why M5 owns it. Today
 * every pod that commits IS the leaseholder, so the window answering a retry is
 * always the one that applied it. Once a pod forwards, the retry after a lost
 * reply can arrive at a pod that never saw the original — which then assigns
 * fresh offsets and appends a second delta for the same segment: the same
 * records at two offsets, which I2 forbids.
 *
 * <p>⚠️ THE DATA WAS ALREADY ON THE CHAIN. M4.10c put the pod's incarnation and
 * a pointer into both the delta attribution and {@code Checkpoint.pods}. Nothing
 * read it back. This is a recovery-path change, not a format change.
 */
class DedupAcrossTakeoverTest {

    /** A fresh sequencer over the same store, as a takeover produces. */
    private static LocalSequencer takeOver(MemoryBinStore store, String pod) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, pod), 8).orElseThrow();
    }

    @Test
    void aReplayARRIVINGATTheSuccessorIsAnsweredFromTheChain() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitRequest flush = new CommitRequest("podb", "i1", 7, "seg/0", counts(3));

        long assigned;
        long sequence;
        LocalSequencer first = takeOver(store, "poda");
        try {
            CommitDelta applied = first.commitAll(List.of(flush));
            assigned = firstOffsetOf(applied, "seg/0");
            sequence = applied.sequence();
        } finally {
            first.close();
        }

        // ⚠️ A REAL SUCCESSOR: a different pod, its own window, over the same
        // store. Nothing in memory carries over -- that is the whole point.
        LocalSequencer successor = takeOver(store, "podc");
        try {
            CommitDelta replay = successor.commitAll(List.of(flush));

            assertThat(firstOffsetOf(replay, "seg/0"))
                    .as("the successor answers with the offsets that ALREADY apply, "
                            + "rebuilt from the chain rather than from memory it never had")
                    .isEqualTo(assigned);
            assertThat(replay.sequence())
                    .as("and from the delta that already holds them").isEqualTo(sequence);
        } finally {
            successor.close();
        }

        // ⚠️ THE CHAIN, NOT THE RETURN VALUE. Answering correctly AND appending
        // anyway satisfies every assertion above while committing the records
        // twice -- which is exactly the defect, so it is asserted separately.
        assertThat(deltasCarrying(store, "seg/0"))
                .as("and appends NOTHING -- one segment, one delta, across a takeover")
                .isEqualTo(1);
    }

    @Test
    void aReplayFromBELOWThePointerIsREFUSED_NotAppliedTwice() throws Exception {
        // ⚠️ THE BOUNDED-ANSWERING LIMIT, AND THE CODE PREDICTED IT.
        // `IdempotencyWindow.answer` says its refusal is "UNREACHABLE UNTIL THE
        // WINDOW IS INHERITED (M4.10f) ... where this refusal starts to matter
        // and where its test lives". A pod's slot holds ONE pointer, to the
        // delta that LAST applied for it, so an OLDER replay is DETECTED but
        // cannot be ANSWERED -- answering would mean the unbounded walk the
        // pointer exists to avoid.
        //
        // ⚠️ SO THE PROPERTY IS NOT "IT IS ANSWERED" but "IT IS NEVER APPLIED
        // TWICE", said out loud rather than by silently assigning fresh
        // offsets. A refused retry costs the caller an error; a duplicated one
        // costs two copies of the same records at two offsets, which I2 forbids.
        MemoryBinStore store = new MemoryBinStore();
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        CommitRequest early = new CommitRequest("podb", "i1", 0, "seg/0", counts(3));

        LocalSequencer first = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(java.time.Duration.ofSeconds(3)), 2, frozen)
                .orElseThrow();
        try {
            first.commitAll(List.of(early));
            first.commitAll(List.of(new CommitRequest("podb", "i1", 1, "seg/1", counts(2))));
            first.commitAll(List.of(new CommitRequest("podb", "i1", 2, "seg/2", counts(2))));
        } finally {
            first.close();
        }

        LocalSequencer successor = takeOver(store, "podc");
        try {
            assertThatThrownBy(() -> successor.commitAll(List.of(early)))
                    .as("an old replay the pointer cannot reach is REFUSED, explicitly")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("flushSeq 0")
                    .hasMessageContaining("does not carry");
        } finally {
            successor.close();
        }

        // ⚠️ THE HALF THAT ACTUALLY PROTECTS I2. Without the inherited window
        // the successor would have assigned fresh offsets here and this count
        // would be 2.
        assertThat(deltasCarrying(store, "seg/0"))
                .as("and above all it is NOT applied a second time")
                .isEqualTo(1);
    }

    @Test
    void theWATERMARKItselfCrossesTheCheckpoint() throws Exception {
        // ⚠️ WITHOUT `checkpoint.pods()` THE SEED SEES ONLY THE UNCHECKPOINTED
        // TAIL, so an old replay is not even DETECTED -- treated as fresh and
        // applied twice, silently. Measured surviving before this test.
        MemoryBinStore store = new MemoryBinStore();
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        LocalSequencer first = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(java.time.Duration.ofSeconds(3)), 2, frozen)
                .orElseThrow();
        try {
            for (int i = 0; i < 6; i++) {
                first.commitAll(List.of(
                        new CommitRequest("podb", "i1", i, "seg/" + i, counts(2))));
            }
        } finally {
            first.close();
        }

        LocalSequencer successor = takeOver(store, "podc");
        try {
            // flushSeq 0 is far below the newest checkpoint. Detected, so
            // refused -- rather than treated as new work.
            assertThatThrownBy(() -> successor.commitAll(
                    List.of(new CommitRequest("podb", "i1", 0, "seg/0", counts(2)))))
                    .as("the watermark inherited through the checkpoint still recognises it")
                    .isInstanceOf(IOException.class);
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/0")).isEqualTo(1);
    }

    @Test
    void theSeedNEVERLowersWhatTheRunningWindowAlreadyKnows() throws Exception {
        // ⚠️ A running window answering its OWN replay, then a successor
        // answering the same one from the seed -- the two halves agreeing.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = takeOver(store, "poda");
        long assigned;
        try {
            first.commitAll(List.of(new CommitRequest("podb", "i1", 0, "seg/0", counts(3))));
            assigned = firstOffsetOf(
                    first.commitAll(List.of(new CommitRequest("podb", "i1", 5, "seg/1",
                            counts(2)))), "seg/1");
            // A replay of the HIGHER flushSeq, answered by the running window.
            assertThat(firstOffsetOf(first.commitAll(
                    List.of(new CommitRequest("podb", "i1", 5, "seg/1", counts(2)))), "seg/1"))
                    .as("the running window answers its own replay").isEqualTo(assigned);
        } finally {
            first.close();
        }

        LocalSequencer successor = takeOver(store, "podc");
        try {
            // The seed carries flushSeq 5 too; a merge keeping the LOWER would
            // drop back to 0 and re-apply everything above it.
            assertThat(firstOffsetOf(successor.commitAll(
                    List.of(new CommitRequest("podb", "i1", 5, "seg/1", counts(2)))), "seg/1"))
                    .as("and so does the successor, from a seed that kept the HIGHEST")
                    .isEqualTo(assigned);
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/1")).isEqualTo(1);
    }

    @Test
    void seedNEVERLowersAWatermarkItAlreadyHolds() {
        // ⚠️ TESTED ON THE UNIT, because through `LocalSequencer` it is
        // unreachable: `start` seeds a window it has just constructed, so the
        // map is always empty and the merge direction cannot be observed.
        // Measured -- inverting it to keep the LOWER left the whole suite
        // green. It is not dead code: a second seed arrives the moment anything
        // re-crosses from a predecessor, and the chain's older view there would
        // re-admit a replay the running window had already answered.
        MemoryBinStore store = new MemoryBinStore();
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);

        // ⚠️ THE KEY IS A SLOT, `podId\0incarnationId`, which is what
        // `ChainReplay` produces and what this map is keyed by. Re-keying it
        // inside `seed` produced `podb\0i1\0i1` -- a key nothing can match, so
        // the window looked populated and every replay was treated as fresh.
        String slot = "podb\0i1";
        window.seed(java.util.Map.of(slot,
                new binjava.format.Checkpoint.PodState("i1", 9, 1, 4)));
        window.seed(java.util.Map.of(slot,
                new binjava.format.Checkpoint.PodState("i1", 2, 1, 1)));

        assertThat(window.replayOf(new CommitRequest("podb", "i1", 9, "seg/x", counts(1))))
                .as("the higher watermark survives a lower seed")
                .isPresent();
        assertThat(window.replayOf(new CommitRequest("podb", "i1", 10, "seg/x", counts(1))))
                .as("and work above it is still fresh")
                .isEmpty();
    }

    @Test
    void aGENUINELYNewFlushAfterATakeoverIsStillApplied() throws Exception {
        // ⚠️ THE NEGATIVE CONTROL, and it is doing real work: a window seeded
        // too eagerly -- one that treated any known incarnation as fully
        // replayed -- would SUPPRESS a real commit. ADR-0036 records that
        // shape as worse than the duplication it prevents, because a suppressed
        // commit loses records silently while a duplicate is at least visible.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = takeOver(store, "poda");
        try {
            first.commitAll(List.of(new CommitRequest("podb", "i1", 7, "seg/0", counts(3))));
        } finally {
            first.close();
        }

        LocalSequencer successor = takeOver(store, "podc");
        try {
            CommitDelta fresh = successor.commitAll(
                    List.of(new CommitRequest("podb", "i1", 8, "seg/1", counts(2))));
            assertThat(firstOffsetOf(fresh, "seg/1"))
                    .as("flushSeq 8 is NEW work and must land, not be answered as a replay")
                    .isEqualTo(3L);
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/1"))
                .as("and it really is on the chain").isEqualTo(1);
    }

    @Test
    void aRESTARTAfterATakeoverIsNotMistakenForAReplay() throws Exception {
        // ⚠️ THE PAIR ADR-0036 REJECTED, arriving through the new path. A pod
        // that restarts issues flushSeq 0 again under a NEW incarnation; a
        // window keyed on `(podId, flushSeq)` would answer it as a replay and
        // suppress a genuine commit. Seeding from the chain must key on the
        // incarnation too, or it reintroduces the defect it inherits from.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = takeOver(store, "poda");
        try {
            first.commitAll(List.of(new CommitRequest("podb", "i1", 0, "seg/0", counts(3))));
        } finally {
            first.close();
        }

        LocalSequencer successor = takeOver(store, "podc");
        try {
            CommitDelta restarted = successor.commitAll(
                    List.of(new CommitRequest("podb", "i2", 0, "seg/1", counts(2))));
            assertThat(firstOffsetOf(restarted, "seg/1"))
                    .as("a new incarnation reissuing flushSeq 0 is a RESTART, not a replay")
                    .isEqualTo(3L);
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/1")).isEqualTo(1);
    }
}
