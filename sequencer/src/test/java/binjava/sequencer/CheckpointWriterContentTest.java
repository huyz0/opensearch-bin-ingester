// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
    void theFirstCheckpointAfterATakeoverCarriesNoPodsAtAll() throws Exception {
        // ⚠️ THE STATE IS PROCESS-LOCAL AND THIS TEST EXISTS TO SAY SO, not to
        // bless it. ⚠️ THE REASON CHANGED IN THIS COMMIT and the assertion did
        // not: ADR-0036 put `(podId, incarnationId, flushSeq)` ON the chain, so
        // it is no longer true that the fact is unrecoverable. What is still
        // true is that NOTHING IN src/main READS IT BACK -- `observe` learns
        // only from live commits -- so a successor still starts with an empty
        // map, emptying the idempotency window exactly when duplicates are
        // likeliest. ⚠️ M4.10d IS WHERE THIS INVERTS: folding the recovered
        // tail into the window makes `doesNotContainKey` the wrong assertion,
        // and this comment is the notice that it must be changed, not deleted.
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
                    .as("poda's flushSeq is on the chain but nothing reads it back yet")
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
