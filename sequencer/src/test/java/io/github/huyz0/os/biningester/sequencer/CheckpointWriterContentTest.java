// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * WHAT a checkpoint carries (M4.8b2), decoded from the bytes actually PUT.
 *
 * <p>⚠️ NOTHING HERE ASSERTS THAT AN OBJECT APPEARED. A constant or stale body,
 * and a constant {@code seq} overwriting one key, pass every count, every
 * boundary, the idle case and a grammar test aimed at the formatter — because
 * {@code CountingBinStore} counts PUTs and not distinct keys. So the bytes are
 * decoded and their values asserted against a workload where the streams and the
 * pods DIFFER from each other.
 *
 * <p>⚠️ {@code seq} IS THE CHAIN'S {@code nextSequence} — the next slot NOT
 * included — and this file says so because the row required the choice to be
 * made and stated. The alternatives it rules out: a per-checkpoint counter,
 * which leaves M4.9 unable to bound replay from the key at all; and an
 * off-by-one against the last INCLUDED sequence, which silently drops one
 * delta's offsets when M4.9 replays from {@code seq}.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CheckpointWriterContentTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
    private static final RunKey RA = new RunKey(A, 0);
    private static final RunKey RB = new RunKey(B, 1);
    private static final Duration T = Duration.ofSeconds(30);

    private static CheckpointWriter.Ticker frozen() {
        return () -> new CountDownLatch(1).await();
    }

    private static Map<RunKey, Integer> counts(Object... pairs) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((RunKey) pairs[i], (Integer) pairs[i + 1]);
        }
        return m;
    }

    private static void commit(CommitLog log, CheckpointWriter writer, String pod, long flushSeq,
            Map<RunKey, Integer> what) throws IOException {
        commit(log, writer, pod, "i1", flushSeq, what);
    }

    /** ⚠️ The incarnation is a PARAMETER: every fixture passing one constant is
     * what let a merge that maxes ACROSS incarnations survive. */
    private static long commit(CommitLog log, CheckpointWriter writer, String pod,
            String incarnation, long flushSeq, Map<RunKey, Integer> what) throws IOException {
        List<CommitRequest> requests = List.of(new CommitRequest(pod, incarnation, flushSeq,
                "seg/" + pod + "/" + incarnation + "/" + flushSeq, what));
        long applied = log.commitAll(requests).sequence();
        writer.observe(requests, applied);
        return applied;
    }

    /**
     * ⚠️ ASSERTING `hasPointer()` IS NOT ASSERTING A POINTER. The writer read
     * `log.nextSequence()` inside `observe`, which runs AFTER `commitAll` has
     * advanced it, so every slot named the delta ONE PAST the one it described —
     * measured, a delta at sequence 0 stored 1/1. `pointerEpoch = 0;
     * pointerSequence = 0;` passed the entire suite before this test existed.
     */
    @Test
    void theSlotPointsAtTheDeltaThatAppliedNotTheOneAfterIt() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        // ⚠️ EPOCH 4 ON PURPOSE. Every other fixture in this file is epoch 1, so
        // `pointerEpoch = 1L` was indistinguishable from `log.epoch()`.
        CommitLog log = new CommitLog(store, "bins", 4);
        log.open(0, 0);
        // ⚠️ the 1 is everyDeltas, NOT the epoch -- checkpoint on every commit.
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            long applied = commit(log, writer, "poda", "i1", 3, counts(RA, 2));

            Checkpoint.PodState slot = lastWritten(store).pods().get("poda");
            assertThat(slot.sequence())
                    .as("the pointer names the delta that applied, not nextSequence")
                    .isEqualTo(applied);
            assertThat(slot.epoch()).as("and its epoch").isEqualTo(log.epoch());
            assertThat(slot.epoch())
                    .as("EPOCH 4, not the 1 every other fixture uses -- `pointerEpoch = 1L` "
                            + "survived the whole module before this literal existed")
                    .isEqualTo(4L);
        }
    }

    /**
     * ⚠️ DEPTH TWO, because depth one passes the pre-ADR merge. `Math::max`
     * carried ACROSS incarnations keeps the dead incarnation's higher watermark
     * for ever, so a restarted pod's SECOND commit is refused — the suppression
     * ADR-0036 exists to prevent. Every other fixture in this file passes one
     * constant incarnation and cannot see it.
     */
    @Test
    void aRestartedPodsSecondCommitIsNotRefusedByTheDeadIncarnationsWatermark()
            throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            commit(log, writer, "poda", "i1", 9, counts(RA, 1));
            commit(log, writer, "poda", "i2", 0, counts(RA, 1));
            long applied = commit(log, writer, "poda", "i2", 1, counts(RA, 1));

            Checkpoint.PodState slot = lastWritten(store).pods().get("poda");
            assertThat(slot.incarnationId())
                    .as("the LIVE incarnation owns the slot, not the dead one")
                    .isEqualTo("i2");
            assertThat(slot.lastAppliedFlushSeq())
                    .as("and its own watermark, not 9 carried across the restart")
                    .isEqualTo(1);
            assertThat(slot.sequence()).isEqualTo(applied);
        }
    }

    private static Checkpoint lastWritten(RecordingBinStore store) throws IOException {
        return Checkpoint.decode(store.lastCheckpointBody()
                .orElseThrow(() -> new AssertionError("no checkpoint was ever written")));
    }

    @Test
    void theCheckpointCarriesEachStreamsOwnNextOffsetAndEachPodsOwnFlushSeq() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 3, T, frozen())) {
            // ⚠️ THE STREAMS AND THE PODS MUST DIFFER FROM EACH OTHER, or a
            // writer that swapped or shared them round-trips green.
            commit(log, writer, "poda", 5, counts(RA, 3));
            commit(log, writer, "podb", 11, counts(RB, 7));
            commit(log, writer, "poda", 6, counts(RA, 2));

            Checkpoint ckpt = lastWritten(store);
            assertThat(ckpt.streams().get(RA).nextOffset())
                    .as("stream A committed 3 then 2 records")
                    .isEqualTo(5);
            assertThat(ckpt.streams().get(RB).nextOffset())
                    .as("stream B committed 7, and is not stream A")
                    .isEqualTo(7);
            assertThat(ckpt.pods())
                    .as("each pod's own latest flushSeq, not each other's")
                    .hasSize(2).hasEntrySatisfying("poda", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(6L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    }).hasEntrySatisfying("podb", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(11L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    });
        }
    }

    @Test
    void everyStreamsOldestRetainedOffsetIsZeroUntilRetentionExists() throws Exception {
        // ⚠️ 0 IS THE TRUTHFUL VALUE for the writer until M7, so this is exact
        // rather than a placeholder. It kills `StreamOffsets(next, next)` and
        // `StreamOffsets(next, next - 1)` alike, and unlike "the two fields
        // differ" it cannot misfire for a stream sitting at offset 0.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            commit(log, writer, "poda", 0, counts(RA, 4, RB, 9));

            Checkpoint ckpt = lastWritten(store);
            assertThat(ckpt.streams()).hasSize(2);
            assertThat(ckpt.streams().values())
                    .allSatisfy(s -> assertThat(s.oldestRetainedOffset()).isZero());
            assertThat(ckpt.streams().get(RA).nextOffset())
                    .as("and the OTHER field is not zero, so this is not vacuous")
                    .isEqualTo(4);
        }
    }

    @Test
    void aPodsFlushSeqNeverGoesBACKWARDSWhenAnOlderFlushArrivesLate() throws Exception {
        // ⚠️ `put` IN PLACE OF A `Math::max` FOLD SURVIVES ANY WORKLOAD WHOSE
        // FLUSH SEQS ONLY RISE, so this one does not: pod A commits 9, then 4.
        // A late or retried flush is exactly how that happens in production.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 2, T, frozen())) {
            commit(log, writer, "poda", 9, counts(RA, 1));
            commit(log, writer, "poda", 4, counts(RA, 1));

            assertThat(lastWritten(store).pods())
                    .as("the highest flushSeq seen, not the last one seen")
                    .hasEntrySatisfying("poda", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(9L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    });
        }
    }

    @Test
    void aPodWithTWOIncarnationsInheritsTheOneWithTheHIGHERPointer() throws Exception {
        // ⚠️ THE COLLISION PATH IN `inherit`, WHICH NOTHING ELSE REACHES.
        // `recoveredPods` is keyed `podId\0incarnationId`, so a pod that
        // restarted mid-term supplies TWO slots; `inherit` collapses them onto
        // one `podId` key and something must choose. Review MEASURED that every
        // remapper survived the whole module before this case existed --
        // `(prior, now) -> prior`, `-> now`, and ordering on the watermark --
        // because no fixture produced a collision. All three die on the pairs
        // below.
        //
        // ⚠️ `>` WEAKENED TO `>=` IS DELIBERATELY NOT IN THAT LIST, and round 6
        // MEASURED it green rather than repeating the round-4 claim that the
        // TIE case kills it. It is an equivalent mutant twice over under the
        // shipped shape: the pointer comparison's `>` sits inside
        // `if (byPointer != 0)`, so `>= 0` and `> 0` decide identically there,
        // and the id tie-break's `>` is reached only from `inherit`'s merge,
        // whose two slots differ in the incarnation id by construction of the
        // `podId\0incarnationId` key -- `compareTo` is never 0. No fixture can
        // kill either, which is why neither is answered with another pair.
        //
        // ⚠️ THREE PAIRS, EACH KILLING MUTATIONS THE OTHERS LEAVE ALIVE, and
        // the general rule so it is not rediscovered a fourth time: killing
        // BOTH directions of a field takes two pairs whose correlation with the
        // pointer is OPPOSITE -- one pair kills the three DESCENDING rules and
        // necessarily leaves the three ASCENDING ones alive -- and reaching the
        // SEQUENCE half of the comparison at all takes a pair whose EPOCHS ARE
        // EQUAL. Two rounds of review each measured mutations surviving a
        // fixture that had only the first of the three.
        //
        // ⚠️ THE FIELDS ARE NAMED FOR THE POINTER, NOT FOR LIVENESS. Only the
        // first pair carries the dead-restarted-live story; the other two exist
        // to constrain the comparison, and calling their slots live or dead
        // would assert something the fixture does not establish.
        record Collision(String podId, Checkpoint.PodState lowerPointer,
                Checkpoint.PodState higherPointer) {}
        List<Collision> pairs = List.of(
                // EPOCHS DIFFER (1 against 2), so the epoch half decides, and
                // this is the dead-incarnation shape: every other field runs
                // OPPOSITE to the pointer, so watermark-descending (9 > 3),
                // id-descending ("i9" > "i2") and sequence-only (9 > 5) each
                // pick the losing slot and die here. Until this pair was
                // anti-correlated, review measured the ENTIRE pointer
                // comparison deletable with the whole module green.
                new Collision("podb", new Checkpoint.PodState("i9", 9, 1, 9),
                        new Checkpoint.PodState("i2", 3, 2, 5)),
                // EPOCHS EQUAL (4), so the SEQUENCE half decides (7 > 3), and
                // the watermark and the id CORRELATE with it -- which is the
                // half the pair above cannot cover: the ASCENDING rules,
                // lowest-watermark-wins (0 < 9) and lowest-id-wins
                // ("i2" < "i9"), pick the losing slot and die here.
                new Collision("podc", new Checkpoint.PodState("i2", 0, 4, 3),
                        new Checkpoint.PodState("i9", 9, 4, 7)),
                // EPOCHS EQUAL (6) with the id ANTI-correlated, which is what
                // constrains the sequence line itself: DELETE it and the
                // epochs tie, so the id tie-break picks "i9" -- the losing
                // slot; INVERT it and it picks the lower sequence (8 < 9) --
                // the losing slot again. Both of those were green before this
                // pair existed.
                new Collision("podd", new Checkpoint.PodState("i9", 5, 6, 8),
                        new Checkpoint.PodState("i2", 1, 6, 9)));
        // ⚠️ AND NO SLOT REPEATS A VALUE ACROSS ITS OWN FIELDS. An earlier
        // draft's winner was `PodState("i2", 0, 2, 2)`, so transposing the last
        // two arguments of `inherit`'s `rememberPod` call, or replacing the
        // watermark argument with the constant 0, was invisible to the
        // assertion below and survived on this file alone.
        //
        // ⚠️ AND BOTH ORDERS ARE ASSERTED, which is what makes this
        // DETERMINISTIC rather than a 50% flake: the map `inherit` iterates is
        // a `Map.copyOf` whose order the JDK salts per JVM, so a single
        // ordering would pass or fail by coin toss. An earlier draft of this
        // pin went through a real takeover and did exactly that -- review
        // measured the mutation showing on 4 of 8 fresh JVMs.
        for (boolean lowerFirst : new boolean[] {true, false}) {
            RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
            CommitLog log = new CommitLog(store, "bins", 1);
            log.open(0, 0);
            try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T,
                    frozen())) {
                Map<String, Checkpoint.PodState> fromChain = new LinkedHashMap<>();
                for (Collision pair : pairs) {
                    String lower = pair.podId() + "\0" + pair.lowerPointer().incarnationId();
                    String higher = pair.podId() + "\0" + pair.higherPointer().incarnationId();
                    if (lowerFirst) {
                        fromChain.put(lower, pair.lowerPointer());
                        fromChain.put(higher, pair.higherPointer());
                    } else {
                        fromChain.put(higher, pair.higherPointer());
                        fromChain.put(lower, pair.lowerPointer());
                    }
                }
                writer.inherit(fromChain);
                commit(log, writer, "poda", "i1", 0, counts(RA, 1));

                Checkpoint written = lastWritten(store);
                for (Collision pair : pairs) {
                    assertThat(written.pods().get(pair.podId()))
                            .as("%s must carry the slot with the HIGHER pointer, whichever "
                                    + "order the inherited map happens to iterate in "
                                    + "(lowerFirst=%s)", pair.podId(), lowerFirst)
                            .isEqualTo(pair.higherPointer());
                }
            }
        }
    }

    @Test
    void twoSlotsSHARINGAPointerCollapseTheSAMEWayInEitherOrder() throws Exception {
        // ⚠️ THE TIE, WHICH IS REACHABLE BY ONE `commitAll` AND WHICH AN EARLIER
        // DRAFT EXCUSED AS AN EQUIVALENT MUTANT. Both reviewers measured the
        // path: `podx/i1` submits a flush, the pod dies and restarts as `i2`,
        // and `i2` submits before the batcher drains. Nothing partitions a batch
        // by pod and only SEGMENT keys must be distinct, so both land in ONE
        // delta and `applyOffsets` stamps both attributions with the SAME
        // pointer. Review then measured the consequence over fresh JVMs: the
        // checkpoint carried the dead incarnation on 5 of 8 starts and the live
        // one on 3 -- an I2 duplication decided by `Map.copyOf`'s salt.
        //
        // ⚠️ THIS PINS DETERMINISM, NOT CORRECTNESS, and the distinction is the
        // point. The pointer is equal by construction and ADR-0036 makes the id
        // unordered, so the tie-break is stable rather than right. ⚠️ THE
        // WATERMARK IS NOT A RULE HERE EITHER, and the absolute form of that --
        // "nothing in the data identifies the live incarnation" -- was wrong in
        // one direction and this file's fixture is wrong in the other: the
        // values below give the DEAD slot watermark 5 against the restart's 0,
        // a window in which `<`-on-watermark would pick the live one, while in
        // the MINIMAL tie the dead incarnation's in-flight flush is its own
        // first and both watermarks are 0. M5.72 records it as an option NOT
        // TAKEN, with the window it holds in. What must not happen is the
        // answer CHANGING between starts, so the fixture asserts only that both
        // iteration orders agree.
        // ⚠️ IT DELIBERATELY DOES NOT ASSERT WHICH WINS -- that would pin an
        // arbitrary choice as though it were a rule. M5.72 owns the rest.
        Checkpoint.PodState first = null;
        for (boolean deadFirst : new boolean[] {true, false}) {
            RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
            CommitLog log = new CommitLog(store, "bins", 1);
            log.open(0, 0);
            try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T,
                    frozen())) {
                Map<String, Checkpoint.PodState> fromChain = new LinkedHashMap<>();
                Checkpoint.PodState dead = new Checkpoint.PodState("i1", 5, 1, 1);
                Checkpoint.PodState live = new Checkpoint.PodState("i2", 0, 1, 1);
                if (deadFirst) {
                    fromChain.put("podx\0i1", dead);
                    fromChain.put("podx\0i2", live);
                } else {
                    fromChain.put("podx\0i2", live);
                    fromChain.put("podx\0i1", dead);
                }
                writer.inherit(fromChain);
                commit(log, writer, "poda", "i1", 0, counts(RA, 1));

                Checkpoint.PodState carried = lastWritten(store).pods().get("podx");
                // ⚠️ ASSERTED BEFORE THE LATCH, because the latch alone admits
                // the vacuous case: with `inherit` emptied, `carried` is null on
                // BOTH passes, `first` never becomes non-null, and the case
                // passes having executed no assertion at all. Review measured
                // exactly that.
                assertThat(carried)
                        .as("podx must be carried into the checkpoint at all (deadFirst=%s)",
                                deadFirst)
                        .isNotNull();
                if (first == null) {
                    first = carried;
                } else {
                    assertThat(carried)
                            .as("two slots sharing a pointer must collapse the same way "
                                    + "whichever order they are visited in -- the choice is "
                                    + "arbitrary, but it must not flip between JVM starts")
                            .isEqualTo(first);
                }
            }
        }
    }

    @Test
    @DisplayName("a freshly constructed writer carries no pods until inherit is called")
    void theFirstCheckpointAfterATakeoverCarriesNoPodsAtAll() throws Exception {
        // ⚠️ THE REASON CHANGED IN M5.55; THE ASSERTION DID NOT. The METHOD name
        // still reads as a claim about takeovers, which stopped being true of
        // the SYSTEM when `LocalSequencer.start` began calling `inherit`.
        // ⚠️ A `@DisplayName` CARRIES THE CORRECTION INSTEAD OF A RENAME,
        // because `check-tdd` keys on `Class#method`: renaming makes it demand a
        // red record for a test that is not new. The display name is what a test
        // report shows, so the correction reaches a reader without inventing a
        // red. ⚠️ A RENAME IS STILL AVAILABLE with `SKIP=check-tdd` and a stated
        // reason -- the route M1.18 records -- if a future change makes the
        // method name actively misleading rather than merely narrow.
        // This case used to say "the first checkpoint AFTER A TAKEOVER carries
        // no pods at all", on the ground that "NOTHING IN src/main READS IT
        // BACK -- so a successor still starts with an empty map". That is FALSE
        // as of `LocalSequencer.start`, which now calls `CheckpointWriter
        // .inherit` beside the window seed; leaving the old name would have
        // left a green test asserting a system property this commit removed.
        //
        // ⚠️ WHAT IT PINS NOW IS THE CONSTRUCTOR, and that is worth pinning
        // precisely BECAUSE `inherit` exists: the writer starts empty, so a
        // successor that is never told what the chain gave it checkpoints a
        // truncated pods map -- which is the duplication M5.55 fixed. This
        // fixture builds the writer directly and never goes through `start`,
        // so it sees the empty start rather than the inherited one.
        //
        // ⚠️ ITS OWN STANDING INSTRUCTION SAID THIS MUST BE CHANGED RATHER THAN
        // DELETED when the inversion came, and M5.55 is that moment.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog first = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, first, "bins", 1, T, frozen())) {
            commit(first, writer, "poda", 42, counts(RA, 3));
            assertThat(lastWritten(store).pods()).hasEntrySatisfying("poda", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(42L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    });
        }

        CommitLog successor = new CommitLog(store, "bins", 2);
        successor.recover();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, successor, "bins", 1, T, frozen())) {
            commit(successor, writer, "podb", 0, counts(RB, 1));

            assertThat(lastWritten(store).pods())
                    .as("the WRITER starts empty: poda's flushSeq is on the chain, and "
                            + "`LocalSequencer.start` is what reads it back by calling "
                            + "`inherit` -- this fixture builds the writer directly, so it "
                            + "sees the un-inherited start that makes that call necessary")
                    .doesNotContainKey("poda");
        }
    }

    @Test
    void theBodysSequenceIsTheKeysSequenceAndBothAdvanceWithDeltas() throws Exception {
        // ⚠️ A CONSTANT `Checkpoint.sequence` WITH CORRECTLY INCREMENTING KEYS
        // survives every other assertion in this file, so the body is asserted
        // AGAINST the key rather than on its own.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            commit(log, writer, "poda", 0, counts(RA, 1));
            String firstKey = store.checkpointKeys().get(0);
            long firstSeq = lastWritten(store).sequence();

            commit(log, writer, "poda", 1, counts(RA, 1));
            List<String> keys = store.checkpointKeys();
            long secondSeq = lastWritten(store).sequence();

            assertThat(secondSeq)
                    .as("strictly increasing across two triggers WITH deltas between")
                    .isGreaterThan(firstSeq);
            // ⚠️ AND ABSOLUTE, because the relative form admits `nextSequence()
            // - 1`: the key and body shift together and every other assertion
            // here still holds. That is one of the three derivations the task
            // names as wrong, and it drops one delta's offsets in M4.9.
            assertThat(secondSeq)
                    .as("seq is EXCLUSIVE -- the next slot NOT covered")
                    .isEqualTo(log.nextSequence());
            assertThat(keys.get(1))
                    .as("and the key names the same sequence the body carries")
                    .isEqualTo(new LogKeys("bins", 1).checkpointKeyFor(secondSeq));
            assertThat(firstKey)
                    .isEqualTo(new LogKeys("bins", 1).checkpointKeyFor(firstSeq));
        }
    }

    @Test
    void eachCheckpointCarriesTheStateAtITSOwnMomentNotTheFirstOnes() throws Exception {
        // ⚠️ EVERY OTHER TEST HERE OBSERVES AT MOST ONE CHECKPOINT, so a writer
        // that computes the body once and reuses it forever -- while `sequence`
        // keeps advancing correctly -- passes all of them. M4.9 would then
        // replay from N with the offsets from before N, which is I2.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            commit(log, writer, "poda", 0, counts(RA, 3));
            commit(log, writer, "podb", 5, counts(RB, 7));

            List<byte[]> bodies = store.checkpointBodies();
            assertThat(bodies).hasSize(2);
            Checkpoint first = Checkpoint.decode(bodies.get(0));
            Checkpoint second = Checkpoint.decode(bodies.get(1));

            assertThat(first.streams()).containsOnlyKeys(RA);
            assertThat(first.pods()).containsOnlyKeys("poda");
            assertThat(second.streams())
                    .as("the second checkpoint knows about the stream the second commit added")
                    .containsOnlyKeys(RA, RB);
            assertThat(second.pods())
                    .as("and about the pod that committed it")
                    .hasSize(2).hasEntrySatisfying("poda", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(0L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    }).hasEntrySatisfying("podb", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(5L);
                        assertThat(s.incarnationId()).isNotNull();
                        assertThat(s.hasPointer()).isTrue();
                    });
        }
    }

    @Test
    void aCheckpointIsWrittenUnderITSOwnEpochsPrefix() throws Exception {
        // ⚠️ `new LogKeys(prefix, 1)` -- a HARDCODED epoch -- survives every
        // other test in this commit, because they all run at epoch 1. If the
        // writer used a constant, then after any failover checkpoints would land
        // under the SEALED ancestor's prefix, M4.9 would find none, and
        // M4.8b1's key filter would make that silent rather than loud.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog successor = new CommitLog(store, "bins", 2);
        successor.recover();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, successor, "bins", 1, T, frozen())) {
            commit(successor, writer, "podb", 0, counts(RB, 1));

            assertThat(store.checkpointKeys())
                    .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                    .as("under epoch 2's prefix, not epoch 1's")
                    .startsWith(new LogKeys("bins", 2).logPrefix());
        }
    }

    @Test
    void theCheckpointKeyIsZeroPaddedSoLexicographicOrderIsNumericOrder() {
        // ⚠️ COUNT CANNOT SEE THIS: `Long.toHexString(seq)` leaves every count
        // identical while `10.ckpt` sorts before `9.ckpt`. Asserted as an
        // ORDERED PAIR, with a seq pair whose padded and unpadded orders DIFFER.
        // ⚠️ M4.9 DOES NOT TAKE THE LAST KEY UNDER THE PREFIX, and an earlier
        // version of this comment said it did -- that is the mechanism ADR-0034
        // rejects; it reads `latestCheckpointKey()`. The padding still matters,
        // because these are the ordered history M7 prunes. THIRD SITE of one
        // claim, corrected twice before this one was found by grep rather than
        // by fixing the instance a reviewer named.
        LogKeys keys = new LogKeys("bins", 1);
        String nine = keys.checkpointKeyFor(9);
        String sixteen = keys.checkpointKeyFor(16);

        assertThat(nine).isEqualTo("bins/ctl/log/0/0000000000000001/ckpt/0000000000000009.ckpt");
        assertThat(sixteen).isEqualTo("bins/ctl/log/0/0000000000000001/ckpt/0000000000000010.ckpt");
        assertThat(nine)
                .as("9 sorts BEFORE 16; unpadded it would sort after")
                .isLessThan(sixteen);

        // ⚠️ AND THE EPOCH IS PADDED TOO, on the same argument one level up.
        assertThat(new LogKeys("bins", 9).checkpointKeyFor(0))
                .isLessThan(new LogKeys("bins", 16).checkpointKeyFor(0));
    }
}
