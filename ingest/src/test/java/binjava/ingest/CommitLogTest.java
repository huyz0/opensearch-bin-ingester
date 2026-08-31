// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

/** ⚠️ The store is the only coordinator (ADR-0002); offsets are assigned here (ADR-0001). */
class CommitLogTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static Map<RunKey, Integer> counts(Object... pairs) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((RunKey) pairs[i], (Integer) pairs[i + 1]);
        }
        return m;
    }

    @Test
    void offsetsAreMonotonicPerStreamAcrossFlushes() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p");
        CommitDelta first = log.commit("seg-1", counts(new RunKey(A, 0), 3));
        CommitDelta second = log.commit("seg-2", counts(new RunKey(A, 0), 2));

        assertThat(first.runs().get(0).firstOffset()).isZero();
        assertThat(first.runs().get(0).lastOffset()).isEqualTo(2);
        // ⚠️ The counter must NOT reset on flush. Resetting it makes every
        // segment start at zero, so a consumer replays from the beginning
        // forever and dedup by _offset is meaningless (NFR-11).
        assertThat(second.runs().get(0).firstOffset()).isEqualTo(3);
        assertThat(second.runs().get(0).lastOffset()).isEqualTo(4);
    }

    @Test
    void offsetsAreIndependentPerStream() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p");
        log.commit("seg-1", counts(new RunKey(A, 0), 5, new RunKey(B, 0), 2));
        CommitDelta second = log.commit("seg-2", counts(new RunKey(A, 0), 1, new RunKey(B, 0), 1));

        // ⚠️ One counter PER STREAM. A single global counter would leave gaps in
        // every partition, and a gap stalls the consumer that meets it.
        assertThat(second.runs().stream()
                .filter(r -> r.key().equals(new RunKey(A, 0))).findFirst().orElseThrow()
                .firstOffset()).isEqualTo(5);
        assertThat(second.runs().stream()
                .filter(r -> r.key().equals(new RunKey(B, 0))).findFirst().orElseThrow()
                .firstOffset()).isEqualTo(2);
    }

    @Test
    void aLostRaceRetriesAtTheNextSlotRatherThanFailing() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        CommitLog mine = new CommitLog(shared, "p");
        CommitLog theirs = new CommitLog(shared, "p");

        theirs.commit("their-seg", counts(new RunKey(A, 0), 4));
        // ⚠️ `mine` still believes slot 0 is free. Losing it is NORMAL -- the
        // commit log is a chain of exactly these races -- so it must re-read,
        // fold in the winner's offsets and take slot 1.
        CommitDelta mineDelta = mine.commit("my-seg", counts(new RunKey(A, 0), 3));

        assertThat(mineDelta.sequence()).isEqualTo(1);
        assertThat(mineDelta.runs().get(0).firstOffset())
                .as("offsets continue after the winner's, never overlap them").isEqualTo(4);
    }

    @Test
    void concurrentWritersProduceOneUnbrokenOffsetSequence() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        int writers = 8;
        var pool = Executors.newFixedThreadPool(writers);
        try {
            CyclicBarrier start = new CyclicBarrier(writers);
            var tasks = new java.util.ArrayList<Callable<CommitDelta>>();
            for (int i = 0; i < writers; i++) {
                final int id = i;
                tasks.add(() -> {
                    CommitLog log = new CommitLog(shared, "p");
                    start.await();
                    return log.commit("seg-" + id, counts(new RunKey(A, 0), 1));
                });
            }
            var seen = new java.util.TreeSet<Long>();
            var sequences = new java.util.TreeSet<Long>();
            for (var f : pool.invokeAll(tasks)) {
                CommitDelta d = f.get();
                seen.add(d.runs().get(0).firstOffset());
                sequences.add(d.sequence());
            }
            // ⚠️ Invariant I1: every writer got a DISTINCT slot and a DISTINCT
            // offset, with no gaps -- guaranteed by putIfAbsent alone, with no
            // lock and no lease.
            assertThat(seen).hasSize(writers);
            assertThat(seen.first()).isZero();
            assertThat(seen.last()).isEqualTo(writers - 1L);
            assertThat(sequences).hasSize(writers);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void recoveryRebuildsOffsetsFromTheLogAlone() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        CommitLog before = new CommitLog(shared, "p");
        before.commit("seg-1", counts(new RunKey(A, 0), 3, new RunKey(B, 7), 5));
        before.commit("seg-2", counts(new RunKey(A, 0), 2));

        // a fresh process, nothing in memory
        CommitLog after = new CommitLog(shared, "p");
        assertThat(after.nextOffset(new RunKey(A, 0))).as("before recovery it knows nothing")
                .isZero();
        after.recover();

        // ⚠️ The log is the SOURCE OF TRUTH. If recovery cannot rebuild this,
        // a restart re-issues offsets that were already handed out and every
        // consumer sees duplicates it cannot detect.
        assertThat(after.nextOffset(new RunKey(A, 0))).isEqualTo(5);
        assertThat(after.nextOffset(new RunKey(B, 7))).isEqualTo(5);
        assertThat(after.nextSequence()).isEqualTo(2);

        CommitDelta next = after.commit("seg-3", counts(new RunKey(A, 0), 1));
        assertThat(next.sequence()).isEqualTo(2);
        assertThat(next.runs().get(0).firstOffset()).isEqualTo(5);
    }

    @Test
    void aDeltaRoundTripsThroughItsEncoding() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p");
        CommitDelta committed =
                log.commit("some/segment/key.bseg", counts(new RunKey(A, 0), 3, new RunKey(B, 9), 1));
        byte[] encoded = committed.encode();
        CommitDelta decoded = CommitDelta.decode(encoded);
        assertThat(decoded).isEqualTo(committed);
        assertThat(decoded.segmentKey()).isEqualTo("some/segment/key.bseg");
    }

    @Test
    void aCorruptOrTruncatedDeltaIsRefused() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p");
        byte[] good = log.commit("k", counts(new RunKey(A, 0), 1)).encode();
        assertThatThrownBy(() -> CommitDelta.decode(java.util.Arrays.copyOf(good, good.length - 2)))
                .isInstanceOf(java.io.IOException.class);
        byte[] badMagic = good.clone();
        badMagic[0] ^= (byte) 0xFF;
        assertThatThrownBy(() -> CommitDelta.decode(badMagic))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("magic");
        assertThatThrownBy(() -> CommitDelta.decode(new byte[4]))
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void anEmptyCommitIsRefused() {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p");
        // ⚠️ It would consume a sequence number and commit nothing, so a replay
        // would see a gap it cannot explain.
        // ⚠️ The MESSAGE, so it is CommitLog's guard being tested and not
        // CommitDelta's. Removing this one lets the empty map reach the delta,
        // which throws its own IllegalArgumentException -- so asserting the type
        // alone left the guard deletable while the test stayed green.
        assertThatThrownBy(() -> log.commit("k", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a commit with no runs");
    }

    @Test
    void committingCostsExactlyOneRequestWhenUncontended() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p");
        log.commit("seg-1", counts(new RunKey(A, 0), 3, new RunKey(B, 0), 2));
        // ⚠️ ONE putIfAbsent for the whole flush, whatever it carries -- so a
        // flush costs one PUT for the segment and one for its delta, and neither
        // scales with streams.
        assertThat(store.counts().puts()).isEqualTo(1);
        assertThat(store.counts().total()).isEqualTo(1);
    }
}
