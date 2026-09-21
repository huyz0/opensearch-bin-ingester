// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.firstOffsetOf;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * A replay that crosses a TAKEOVER is answered, not applied twice (M5.1).
 *
 * <p>⚠️ THIS IS THE HALF OF IDEMPOTENCY THAT DID NOT EXIST, and
 * {@link Sequencer}'s own javadoc USED TO say so: "A SUCCESSOR INHERITS NOTHING
 * ... a replay that crosses a takeover commits twice. Inheriting it is M4.10f."
 * ⚠️ THAT SENTENCE IS NO LONGER IN THE TREE — M5.1 made it false and M5.23
 * rewrote the paragraph — so it is quoted here as history, not as a citation to
 * follow. M4.10f was in no build and was a backlog row nowhere, on this branch
 * or on the archive branch: an obligation named in a contract and owned by
 * nobody.
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

    /**
     * An AMBIGUOUSLY-LANDED flush is inherited too (M5.25).
     *
     * <p>⚠️ THE HALF M5.23 LEFT OPEN, and it is invisible from either side.
     * {@code CheckpointWriter} learns only from a commit that RETURNED, so an
     * append whose response was lost reached it never — the flush was durable,
     * answered by the running sequencer's own window, and absent from the map a
     * SUCCESSOR treats as authoritative below the checkpoint bound.
     *
     * <p>⚠️ THE CHECKPOINT IS READ BACK, not inferred from the replay's answer.
     * Review MEASURED why: every assertion about the answer to a replay of
     * flushSeq 4 is satisfied by a watermark of FIVE just as well, so recording
     * one flush too high survived — and its effect is the opposite defect, a
     * genuinely new flush REFUSED after a takeover, which ADR-0036 calls the
     * more damaging direction. The negative control below is that case.
     *
     * <p>⚠️ AND THE SETUP IS THE TEST. Review MEASURED three one-token edits
     * that each make this pass against the defect: {@code K = 8} instead of 1
     * (the delta stays in the uncheckpointed tail), dropping the {@code
     * seg/after} commit (same), and {@code Mode.LOST} instead of {@code LANDED}
     * (nothing was there to reconcile). The checkpoint assertion is what stops
     * those being silent.
     */
    @Test
    void anAMBIGUOUSLYLandedFlushIsInheritedAcrossATakeover() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        java.util.concurrent.atomic.AtomicBoolean armed =
                new java.util.concurrent.atomic.AtomicBoolean();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_ABSENT, false, key -> armed.get());
        // ⚠️ TWO PODS IN ONE AMBIGUOUS BATCH, which is what a leaseholder
        // folding a window of forwarded flushes produces -- and without two
        // DIFFERENT pods, "records EVERY flush the landed delta carries" is
        // unconstrained: review measured a `break` after the first attributed
        // segment surviving, and two segments of the SAME pod under one
        // flushSeq did not kill it either, because one entry covers both.
        CommitRequest lostA = new CommitRequest("podb", "i1", 4, "seg/lost-a", counts(3));
        CommitRequest lostB = new CommitRequest("podd", "i1", 9, "seg/lost-b", counts(2));

        long assigned;
        long ambiguousSequence;
        LocalSequencer first = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(java.time.Duration.ofSeconds(3)), 1, frozen)
                .orElseThrow();
        try {
            // ⚠️ AN EARLIER RETURNED COMMIT FROM THE SAME POD, so the map
            // already holds an entry for it and the reconciled fact has to WIN
            // a merge rather than fill a hole -- review measured `putIfAbsent`
            // surviving without one. It also pushes the ambiguous delta's
            // sequence past the epoch, so swapping the two arguments is visible.
            first.commitAll(List.of(new CommitRequest("podb", "i1", 1, "seg/early", counts(1))));

            armed.set(true);
            assertThatThrownBy(() -> first.commitAll(List.of(lostA, lostB)))
                    .as("the append landed and the response did not")
                    .isInstanceOf(IOException.class);
            CommitDelta answered = first.commitAll(List.of(lostA, lostB));
            assigned = firstOffsetOf(answered, "seg/lost-a");
            ambiguousSequence = answered.sequence();
            // ⚠️ AND A LATER FLUSH, so a checkpoint is written whose sequence is
            // PAST the ambiguous delta.
            first.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/after", counts(2))));
        } finally {
            first.close();
        }

        // ⚠️ WHAT WAS WRITTEN, not what a replay happens to answer.
        Checkpoint checkpoint;
        try (var in = backing.get(new LogKeys(PREFIX, 1L).latestCheckpointKey())) {
            checkpoint = Checkpoint.decode(in.readAllBytes());
        }
        assertThat(checkpoint.sequence())
                .as("the checkpoint bounds PAST the ambiguous delta, so a successor cannot "
                        + "find it in the tail -- which is what makes this test about the "
                        + "checkpoint at all")
                .isGreaterThan(ambiguousSequence);
        assertThat(checkpoint.pods().get("podb"))
                .as("and it carries the flush whose commit never returned, at the delta that "
                        + "holds it -- one too high would REFUSE this pod's next real flush")
                .isEqualTo(new Checkpoint.PodState("i1", 4, 1L, ambiguousSequence));
        assertThat(checkpoint.pods().get("podd"))
                .as("and EVERY pod the landed batch carried, not just the first -- a "
                        + "leaseholder folds many pods' flushes into one delta")
                .isEqualTo(new Checkpoint.PodState("i1", 9, 1L, ambiguousSequence));

        LocalSequencer successor = takeOver(backing, "podc");
        try {
            CommitDelta replay = successor.commitAll(List.of(lostA, lostB));
            assertThat(firstOffsetOf(replay, "seg/lost-a"))
                    .as("the successor answers with the offsets that already apply, for a "
                            + "flush no commit ever RETURNED for")
                    .isEqualTo(assigned);

            // ⚠️ THE NEGATIVE CONTROL. A watermark one too high answers the
            // replay above just as well, and REFUSES this -- a live pod's real
            // flush rejected across a takeover, which ADR-0036 records as worse
            // than the duplication the row exists to stop.
            assertThat(successor.commitAll(List.of(
                    new CommitRequest("podb", "i1", 5, "seg/next", counts(2)))).segments())
                    .as("and a genuinely NEW flush from the same pod is still applied")
                    .isNotEmpty();
        } finally {
            successor.close();
        }

        assertThat(deltasCarrying(backing, "seg/lost-a"))
                .as("and appends NOTHING -- one delta, across a lost reply AND a takeover")
                .isEqualTo(1);
        // ⚠️ BOTH PODS OF THE BATCH. The I2 assertion is what actually shows
        // nothing was committed twice, and covering only the first pod would
        // leave the two-pod strengthening resting entirely on the white-box
        // checkpoint read.
        assertThat(deltasCarrying(backing, "seg/lost-b"))
                .as("for the second pod of the same landed batch as well")
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
                new io.github.huyz0.os.biningester.format.Checkpoint.PodState("i1", 9, 1, 4)));
        window.seed(java.util.Map.of(slot,
                new io.github.huyz0.os.biningester.format.Checkpoint.PodState("i1", 2, 1, 1)));

        assertThat(window.replayOf(new CommitRequest("podb", "i1", 9, "seg/x", counts(1))))
                .as("the higher watermark survives a lower seed")
                .isPresent();
        assertThat(window.replayOf(new CommitRequest("podb", "i1", 10, "seg/x", counts(1))))
                .as("and work above it is still fresh")
                .isEmpty();
    }

    @Test
    void aBAREV0CheckpointSlotLeavesItsPodUNPROTECTED_AndThatIsRecorded() throws Exception {
        // ⚠️ A STATED GAP, PINNED SO IT CANNOT DRIFT. `Checkpoint.decode`
        // yields `PodState.bare(watermark)` for a v0 object -- no incarnation,
        // no pointer -- and v0 objects keep decoding for the retention window,
        // so this is a supported rolling-upgrade state.
        //
        // Such a slot cannot be keyed or followed, and admitting it is strictly
        // WORSE: merged on flushSeq alone it evicts a real pointered slot and
        // seeds "pod\0null", which nothing matches. It is skipped -- and
        // because the checkpoint still bounds the walk, that pod's deltas are
        // never read either, so its replay is applied twice.
        //
        // ⚠️ THIS IS NOT A REGRESSION: before M5.1 every takeover replay
        // duplicated. It is the one case that does not improve, and the test
        // exists so nobody later reads the seeding code and assumes it does.
        // Closing it costs either bounded recovery for that chain (M4's
        // criterion 5) or a new unanswerable-watermark tier -- M5.22.
        MemoryBinStore store = new MemoryBinStore();
        CommitRequest flush = new CommitRequest("podb", "i1", 3, "seg/0", counts(3));
        LocalSequencer first = takeOver(store, "poda");
        try {
            first.commitAll(List.of(flush));
        } finally {
            first.close();
        }

        io.github.huyz0.os.biningester.format.Checkpoint bare = new io.github.huyz0.os.biningester.format.Checkpoint(2L,
                java.util.Map.of(),
                java.util.Map.of("podb", io.github.huyz0.os.biningester.format.Checkpoint.PodState.bare(100)));
        byte[] bytes = bare.encode();
        store.put(new LogKeys(PREFIX, 1L).latestCheckpointKey(),
                new io.github.huyz0.os.biningester.binstore.Body(bytes.length,
                        () -> new java.io.ByteArrayInputStream(bytes)));

        LocalSequencer successor = takeOver(store, "podc");
        try {
            successor.commitAll(List.of(flush));
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/0"))
                .as("the KNOWN gap: behind a bare v0 slot the replay is applied twice, "
                        + "as it was before M5.1. Pinned so the limit is visible rather "
                        + "than discovered")
                .isEqualTo(2);
    }

    /** Replays `early` behind a checkpoint whose podb slot names {@code inc}. */
    private static int deltasAfterReplayBehindSlotNaming(String inc) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitRequest early = new CommitRequest("podb", "i1", 0, "seg/0", counts(3));
        LocalSequencer first = takeOver(store, "poda");
        try {
            first.commitAll(List.of(early));
        } finally {
            first.close();
        }
        // ⚠️ THE BOUND MUST FALL PAST THE i1 DELTA AND NOT PAST THE CHAIN'S END,
        // which is refinement (a)'s "passes on the tail" trap. Below the delta
        // the walk reads it and the replay is answered from the delta rather
        // than the seed; beyond the chain's end the barrier guard rejects the
        // checkpoint outright and the walk is never bounded at all. Either way
        // it is ONE delta whatever the slot names, and the case constrains
        // nothing. ⚠️ IT IS AN INTERVAL, NOT THE LITERAL 2: review swept 0-4 and
        // measured 0,1 -> 1/1, 2 -> 1/2, 3 -> 1/2, 4 -> 1/1. An earlier comment
        // here said "sequence 2 is load-bearing", which names a point where the
        // code has a window -- and an author who moves the chain end while
        // keeping the literal lands back on the tail.
        io.github.huyz0.os.biningester.format.Checkpoint cp = new io.github.huyz0.os.biningester.format.Checkpoint(2L,
                java.util.Map.of(),
                java.util.Map.of("podb",
                        new io.github.huyz0.os.biningester.format.Checkpoint.PodState(inc, 0, 1, 1)));
        byte[] bytes = cp.encode();
        store.put(new LogKeys(PREFIX, 1L).latestCheckpointKey(),
                new io.github.huyz0.os.biningester.binstore.Body(bytes.length,
                        () -> new java.io.ByteArrayInputStream(bytes)));

        LocalSequencer successor = takeOver(store, "podc");
        try {
            successor.commitAll(List.of(early));
        } finally {
            successor.close();
        }
        return deltasCarrying(store, "seg/0");
    }

    @Test
    void aSUPERSEDEDIncarnationsReplayIsAppliedTWICE_AndTheSAMEFixtureAnswersITSOwn()
            throws Exception {
        // ⚠️ THE SPEC's INCARNATION LIMIT, WHOSE CONSEQUENCE NOTHING ASSERTED IN
        // EITHER DIRECTION until M5.52b. ADR-0036 states the price in as many
        // words: an incarnation id is UNORDERED, so nothing tells a NEWER
        // incarnation from an OLDER one, and the checkpoint holds ONE slot per
        // pod -- "X commits (Ix, 5), Y commits (Iy, 3) and takes the slot, and
        // X's retry of 5 is then tested against a foreign watermark".
        //
        // ⚠️ THE CONTRAST IS THE PROPERTY, AND ONE HALF ALONE WOULD NOT BE. The
        // two runs differ in ONE character -- which incarnation the slot names --
        // so the duplication cannot be blamed on the checkpoint bounding the
        // walk, which is `aBAREV0CheckpointSlotLeavesItsPodUNPROTECTED`'s
        // subject and would look identical from the duplicated half alone.
        // MEASURED both ways before this was written.
        //
        // ⚠️ WHY: `ChainReplay` seeds `slot(pod, state.incarnationId())`, so a
        // slot naming i2 seeds `podb\0i2` and i1's replay matches nothing. It is
        // not detected and not refused -- it is treated as FRESH. That is the
        // direction I2 forbids, pinned rather than fixed, because closing it is
        // a DECISION (order incarnations, or give the window a second tier)
        // rather than an oversight.
        //
        // ⚠️ TWO THINGS THIS FIXTURE DOES NOT PIN, so nothing credits it with
        // them. The duplicate lands at the SAME offsets here, not at two
        // different ones: the forged checkpoint carries no `streams`, so the
        // successor rewinds and both deltas sit at 0..2 -- with realistic
        // stream offsets the second would land at 3..5. And `PodState(inc, 0,
        // 1, 1)` gives epoch and sequence the same value, so transposing them
        // in `IdempotencyWindow` survives this case; three neighbours in this
        // file kill it.
        assertThat(deltasAfterReplayBehindSlotNaming("i1"))
                .as("the slot names the replay's OWN incarnation, so it is answered from "
                        + "the seed and applied once")
                .isEqualTo(1);
        assertThat(deltasAfterReplayBehindSlotNaming("i2"))
                .as("a LATER incarnation took the one slot podb owns, so the replay "
                        + "MATCHES NO KEY AT ALL and lands a SECOND time. Note what does "
                        + "NOT happen: no watermark is compared -- `replayOf` keys on "
                        + "(podId, incarnationId) and returns empty before any flushSeq "
                        + "test. The stated price of an unordered incarnation id")
                .isEqualTo(2);
    }

    @Test
    void aMIDDLELeadersCheckpointDoesNotDROPThePodsItNeverSaw() throws Exception {
        // ⚠️ RECORDS DUPLICATED ACROSS TWO TAKEOVERS AND NOTHING KNEW (M5.55).
        // `LocalSequencer.start` seeds the in-memory WINDOW from
        // `log.recoveredPods()` and then builds the `CheckpointWriter` with
        // NOTHING, so its pods map starts EMPTY. A middle leader therefore
        // writes a checkpoint carrying cumulative OFFSETS but a TRUNCATED pods
        // map, `ChainReplay` stops the next successor's walk AT that
        // checkpoint, and a pod that committed in the epoch before it is
        // unprotected -- its retry is applied a second time, which is I2.
        //
        // ⚠️ THE INTERMEDIATE CHECKPOINT IS THE MECHANISM, NOT THE INTERMEDIATE
        // COMMITS, which is why the middle leader runs at K=1: with K high
        // enough that no checkpoint is written, the same two commits leave the
        // retry answered. ⚠️ AND THE MIDDLE LEADER MUST BE A DIFFERENT POD:
        // were the commits podb's own, podb would be IN that checkpoint at a
        // higher watermark and the retry refused by the pointer branch -- one
        // delta, opposite outcome, different mechanism.
        //
        // ⚠️ IT NEEDS NO RESTART, NO v0 OBJECT AND NO LEGACY BUILD: two
        // takeovers, i.e. any rolling deploy.
        MemoryBinStore store = new MemoryBinStore();
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        // ⚠️ THE THREE COMPONENTS MUST DIFFER, and an earlier fixture left them
        // equal. With `podb` committing a single flushSeq 0 as the chain's only
        // delta, its inherited state was `PodState[i1, 0, epoch=1, sequence=1]`
        // -- so review MEASURED two mutations of `inherit` surviving the whole
        // module: TRANSPOSING `epoch` and `sequence` (both 1, a literal no-op)
        // and replacing the watermark with the constant `0L` (the true value).
        // ⚠️ THE SECOND IS THE DANGEROUS ONE: in production a constant watermark
        // silently re-admits every retry of flushSeq 1..N for any pod that ever
        // committed more than once. ⚠️ AND THE FIRST IS A TRAP THIS FILE ALREADY
        // RECORDS, at the sibling case that notes `PodState(inc, 0, 1, 1)` gives
        // epoch and sequence one value. A priming flush separates all three.
        CommitRequest early = new CommitRequest("podb", "i1", 2, "seg/0", counts(3));

        LocalSequencer first = takeOver(store, "poda");
        try {
            // ⚠️ TWO PRIMING FLUSHES, NOT ONE, SO ALL THREE COMPONENTS DIFFER.
            // One left `PodState[i1, 1, epoch=1, sequence=2]` -- watermark and
            // epoch BOTH 1 -- and review MEASURED that exchanging those two
            // arguments still survived the module. Separating `sequence` fixed
            // one instance of the trap and left another beside it. With two,
            // podb inherits `[i1, 2, epoch=1, sequence=3]`.
            first.commitAll(List.of(
                    new CommitRequest("podb", "i1", 0, "seg/pre", counts(1))));
            first.commitAll(List.of(
                    new CommitRequest("podb", "i1", 1, "seg/pre2", counts(1))));
            first.commitAll(List.of(early));
        } finally {
            first.close();
        }

        // The middle leader: a DIFFERENT pod, K=1 so it checkpoints.
        LocalSequencer middle = LocalSequencer.start(store, PREFIX, manager(store, "pode"), 8,
                LocalSequencer.sleepFor(java.time.Duration.ofSeconds(3)), 1, frozen)
                .orElseThrow();
        try {
            middle.commitAll(List.of(new CommitRequest("pode", "i9", 0, "seg/m0", counts(2))));
            middle.commitAll(List.of(new CommitRequest("pode", "i9", 1, "seg/m1", counts(2))));
        } finally {
            middle.close();
        }

        LocalSequencer third = takeOver(store, "podc");
        try {
            third.commitAll(List.of(early));
        } finally {
            third.close();
        }

        assertThat(deltasCarrying(store, "seg/0"))
                .as("podb committed under the FIRST leader and its retry reaches the THIRD; "
                        + "the middle leader's checkpoint must carry podb forward rather than "
                        + "dropping it, or the same records land at two offsets")
                .isEqualTo(1);
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
