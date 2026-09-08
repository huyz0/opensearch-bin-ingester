// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Many flushes commit as ONE chain entry (M4.7, ADR-0032).
 *
 * <p>⚠️ THE NUMBER THAT MUST NOT REGRESS IS COMMITS PER SECOND, NOT COMMITS PER
 * STREAM. The M4 SPEC prices the commit chain at one writer times
 * {@code 1 / commitBatchInterval} — $52/month at 250 ms — and says plainly that
 * the POD dimension is the one to assert, "because the stream dimension already
 * holds in today's code and a test asserting only that would pass without
 * constraining the change". So the assertions here vary pods and count PUTs.
 */
class CommitLogBatchTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static Map<RunKey, Integer> counts(Object... pairs) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((RunKey) pairs[i], (Integer) pairs[i + 1]);
        }
        return m;
    }

    private static CommitRequest from(String pod, long flushSeq, Map<RunKey, Integer> counts) {
        return new CommitRequest(pod, "i1", flushSeq, "bins/" + pod + "/" + flushSeq + ".bseg", counts);
    }

    @Test
    void sixPodsFlushingIntoOneWindowCostTheSAMEONEPutAsOne() throws Exception {
        // ⚠️ ACCEPTANCE CRITERION 9's POD DIMENSION, and the reason the format
        // had to change first: with one segment per entry, six flushes were six
        // PUTs and the $52/month became $52 x pods, which non-negotiable 6
        // forbids by name.
        CountingBinStore one = new CountingBinStore(new MemoryBinStore());
        CommitLog logOne = new CommitLog(one, "p", 1);
        logOne.commitAll(List.of(from("pod0", 0, counts(new RunKey(A, 0), 3))));

        CountingBinStore six = new CountingBinStore(new MemoryBinStore());
        CommitLog logSix = new CommitLog(six, "p", 1);
        var batch = new java.util.ArrayList<CommitRequest>();
        for (int pod = 0; pod < 6; pod++) {
            batch.add(from("pod" + pod, 0, counts(new RunKey(A, 0), 3)));
        }
        CommitDelta delta = logSix.commitAll(batch);

        assertThat(six.counts().puts())
                .as("six pods in one window must cost what one pod costs")
                .isEqualTo(one.counts().puts())
                .isEqualTo(1L);
        // ⚠️ AND WHAT WAS WRITTEN, because a cheap commit that DROPS work is
        // cheaper still: truncating the batch to its first two submissions left
        // this assertion more satisfied, not less, until it also checked the
        // contents. Four pods' records would simply have vanished, acked.
        assertThat(delta.segments()).as("every pod's flush is in the entry").hasSize(6);
        assertThat(delta.segments().stream()
                .map(s -> s.runs().get(0).firstOffset()))
                .as("and each starts where the previous stopped")
                .containsExactly(0L, 3L, 6L, 9L, 12L, 15L);
    }

    // ⚠️ NO TEST HERE FOR THE STREAM DIMENSION, deliberately. Criterion 9 names
    // both, but the SPEC also says the stream half "already holds in today's
    // code and a test asserting only that would pass without constraining the
    // change" — and a thousand streams have always been one submission and so
    // one PUT, before this change and after. Such a test could not fail under
    // any mutation of `commitAll`, which testing.md calls coverage theatre.
    // `CommitLogTest.committingCostsExactlyOneRequestWhenUncontended` is where
    // that dimension is pinned, and it predates this task.

    @Test
    void offsetsADVANCEAcrossSegmentsWhenTwoPodsShareAStreamInOneWindow() throws Exception {
        // ⚠️ THE CORRECTNESS BUG A BATCHER INTRODUCES IF WRITTEN THE OBVIOUS
        // WAY. Computing each segment's offsets from the log's COMMITTED state
        // hands both contributors the same base, so two segments claim one
        // range — I2 — and no conditional write catches it, because the entry
        // is written once and is internally wrong.
        // ⚠️ ONTO A NON-EMPTY LOG, deliberately. On a fresh one the first
        // contributor's base is 0, so "starts where the log had reached" cannot
        // be told from a hardcoded zero — measured: seeding the cursor with
        // `k -> 0L` for batches only left the whole suite green while handing
        // out a range the log had already assigned, which is I2.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        log.commit("bins/earlier.bseg", counts(new RunKey(A, 0), 10));

        CommitDelta delta = log.commitAll(List.of(
                from("pod0", 0, counts(new RunKey(A, 0), 3)),
                from("pod1", 0, counts(new RunKey(A, 0), 5))));

        assertThat(delta.segments()).hasSize(2);
        assertThat(delta.segments().get(0).runs().get(0).firstOffset())
                .as("the first contributor starts where the log had reached, not at zero")
                .isEqualTo(10);
        assertThat(delta.segments().get(1).runs().get(0).firstOffset())
                .as("and the second starts where the FIRST stopped, not where the log had")
                .isEqualTo(13);
        assertThat(log.nextOffset(new RunKey(A, 0)))
                .as("the log then reflects both contributions").isEqualTo(18);
    }

    @Test
    void aBatchThatLOSESARaceRecomputesEverySegmentsBase() throws Exception {
        // ⚠️ THE COMMENT IN CAPITALS SAYS THE CURSOR "cannot be hoisted out" of
        // the retry loop, and nothing in the BATCH dimension held it: hoisting
        // it for batches only left the suite green, killed solely by a
        // pre-existing SINGLE-submission test. A lost race folds the winner's
        // offsets in, so every base must move — a stale cursor re-assigns a
        // range the winner already took, which is I2 and which the conditional
        // write cannot catch, since the retry wins its own slot honestly.
        MemoryBinStore shared = new MemoryBinStore();
        CommitLog rival = new CommitLog(shared, "p", 1);
        CommitLog mine = new CommitLog(shared, "p", 1);
        rival.commit("bins/rival.bseg", counts(new RunKey(A, 0), 4));

        CommitDelta delta = mine.commitAll(List.of(
                from("pod0", 0, counts(new RunKey(A, 0), 3)),
                from("pod1", 0, counts(new RunKey(A, 0), 2))));

        assertThat(delta.sequence()).as("the batch took the next slot").isEqualTo(1);
        assertThat(delta.segments().stream().map(s -> s.runs().get(0).firstOffset()))
                .as("both bases recomputed after folding the winner's four records in")
                .containsExactly(4L, 7L);
    }

    @Test
    void aStreamTOUCHEDByOnlyOneSegmentIsUnaffectedByTheOther() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);

        CommitDelta delta = log.commitAll(List.of(
                from("pod0", 0, counts(new RunKey(A, 0), 3)),
                from("pod1", 0, counts(new RunKey(B, 0), 5))));

        assertThat(delta.segments().get(1).runs().get(0).firstOffset())
                .as("a different stream starts at its own zero, not after the first segment's")
                .isZero();
    }

    @Test
    void aCallerFindsITSOffsetsByTheSegmentKeyItSubmitted() throws Exception {
        // ⚠️ THIS IS HOW M4.7 SETTLES THE QUESTION the Sequencer contract left
        // open: "a returned delta may carry runs this caller did not submit ...
        // Selecting by RunKey is NOT sufficient either". Selecting by SEGMENT
        // KEY is, because each request brings its own segment and a batch
        // refuses duplicates.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        CommitRequest mine = from("pod1", 7, counts(new RunKey(A, 0), 5));

        CommitDelta delta = log.commitAll(List.of(
                from("pod0", 0, counts(new RunKey(A, 0), 3)), mine));

        var found = delta.segments().stream()
                .filter(s -> s.segmentKey().equals(mine.segmentKey()))
                .findFirst().orElseThrow();
        assertThat(found.runs().get(0).firstOffset()).isEqualTo(3);
    }

    @Test
    void twoSubmissionsNamingTheSameSegmentAreRefused() throws Exception {
        // ⚠️ Attribution rests on the key being unique in a batch, so the
        // ambiguity is refused rather than resolved: one caller would be
        // unfindable and the other would answer for both.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        CommitRequest first = from("pod0", 0, counts(new RunKey(A, 0), 1));
        CommitRequest sameKey = new CommitRequest("pod1", "i1", 0, first.segmentKey(),
                counts(new RunKey(B, 0), 1));

        assertThatThrownBy(() -> log.commitAll(List.of(first, sameKey)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same segment");
    }

    @Test
    void aDeltaNamingTheSameSegmentTwiceIsRefusedByTheRECORDItself() {
        // ⚠️ THE SAME INVARIANT ONE RUNG LOWER. `commitAll` refusing a colliding
        // batch protects the WRITE path only; a delta with duplicate keys
        // arriving from a torn object or a foreign writer would decode without
        // complaint, and a caller applying the attribution rule
        // (`filter(s -> s.segmentKey().equals(mine)).findFirst()`) would
        // silently take the wrong segment's offsets. Enforcing it in the record
        // makes the state unrepresentable for every producer and every decoder.
        var twice = List.of(
                new binjava.format.SegmentCommit("bins/same.bseg",
                        List.of(new binjava.format.RunCommit(new RunKey(A, 0), 1, 0))),
                new binjava.format.SegmentCommit("bins/same.bseg",
                        List.of(new binjava.format.RunCommit(new RunKey(B, 0), 1, 0))));

        assertThatThrownBy(() -> new CommitDelta(1, twice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same segment twice");
    }

    @Test
    void anEmptyBatchIsRefused() {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);

        // ⚠️ THE MESSAGE NAMES THE LAYER, and it has to: `CommitDelta` refuses
        // an empty segment list too, so asserting only the type and the phrase
        // "commits nothing" cannot tell this guard from its absence — the
        // request would simply fail one layer down with a message about a
        // DELTA, which is not what an empty BATCH is. Measured: the guard was
        // an equivalent mutant until this assertion named it.
        assertThatThrownBy(() -> log.commitAll(List.<CommitRequest>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commit with no segments");
    }

    @Test
    void aBatchedCommitSurvivesARecoveryWithEveryOffsetIntact() throws Exception {
        // ⚠️ The entry has to be READ back as well as written: a batched delta
        // that only the writer understands is worse than no batching.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 1);
        log.commitAll(List.of(
                from("pod0", 0, counts(new RunKey(A, 0), 3)),
                from("pod1", 0, counts(new RunKey(A, 0), 5, new RunKey(B, 2), 7))));

        CommitLog restarted = new CommitLog(store, "p", 1);
        restarted.recover();

        assertThat(restarted.nextOffset(new RunKey(A, 0))).isEqualTo(8);
        assertThat(restarted.nextOffset(new RunKey(B, 2))).isEqualTo(7);
        assertThat(restarted.nextSequence()).isEqualTo(1);
    }
}
