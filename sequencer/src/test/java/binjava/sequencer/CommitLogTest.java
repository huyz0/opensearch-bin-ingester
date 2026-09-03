// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

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

    // ---- M4.4: the epoch in the object path ----

    @Test
    void theKeyCarriesTheEpochZeroPaddedSoLexicographicOrderIsNumericOrder() {
        // ⚠️ The epoch is IN THE PATH (ADR-0002). That is what makes fencing
        // independent of catching a write in time: a fenced leader's in-flight
        // PUT lands under its OWN epoch, where readers of the new epoch never
        // look. Nothing has to arrive before anything else.
        // ⚠️ Zero-padded to 16 hex digits, like the sequence beside it, so
        // lexicographic order IS numeric order -- the property `list` depends
        // on, since ADR-0022 removed `lastModifiedMillis` and left key order as
        // the only ordering available.
        assertThat(new CommitLog(new MemoryBinStore(), "p", 0).logPrefix())
                .isEqualTo("p/ctl/log/0/0000000000000000/");
        assertThat(new CommitLog(new MemoryBinStore(), "p", 1).logPrefix())
                .isEqualTo("p/ctl/log/0/0000000000000001/");
        assertThat(new CommitLog(new MemoryBinStore(), "p", 255).logPrefix())
                .isEqualTo("p/ctl/log/0/00000000000000ff/");
        // ⚠️ The FULL key, not only the prefix. Round-1 test review measured
        // that `keyFor` was pinned by nothing: dropping the `.delta` suffix, or
        // rendering the SEQUENCE unpadded, each left all 490 tests green -- and
        // this test's own name reads as covering the key grammar when it did
        // not. The sequence carries the same padding for the same reason the
        // epoch does.
        assertThat(new CommitLog(new MemoryBinStore(), "p", 255).keyFor(4095))
                .isEqualTo("p/ctl/log/0/00000000000000ff/0000000000000fff.delta");
    }

    @Test
    void epochPrefixesSortInNumericOrderAcrossTheNineToSixteenBoundary() {
        // ⚠️ The case an UNPADDED epoch gets wrong: "10" sorts before "9", and
        // hex makes it worse -- 16 renders "10" and would sort before "9" too.
        String nine = new CommitLog(new MemoryBinStore(), "p", 9).logPrefix();
        String ten = new CommitLog(new MemoryBinStore(), "p", 10).logPrefix();
        String sixteen = new CommitLog(new MemoryBinStore(), "p", 16).logPrefix();
        assertThat(nine).isLessThan(ten);
        assertThat(ten).isLessThan(sixteen);
    }

    @Test
    void theTwoArgConstructorIsEpochZeroSoEveryExistingCallSiteKeepsItsMeaning() {
        // ⚠️ M1 wrote slot 0 / epoch 0 and said so: "their SLOTS are in the
        // grammar from the first object so that M4's leases and epochs are not
        // a key-grammar change". This is that promise being kept -- the
        // dimension was always there, and M4 only fills it in.
        assertThat(new CommitLog(new MemoryBinStore(), "p").logPrefix())
                .isEqualTo(new CommitLog(new MemoryBinStore(), "p", 0).logPrefix());
    }

    @Test
    void theChainReportsWhichTermItBelongsTo() {
        // ⚠️ `epoch()` is how a caller (M4.5's CONTINUE header, M4.6's seal)
        // learns which term a chain is; untested, `return 0` is unconstrained.
        assertThat(new CommitLog(new MemoryBinStore(), "p", 7).epoch()).isEqualTo(7);
        assertThat(new CommitLog(new MemoryBinStore(), "p").epoch())
                .as("the no-lease default is epoch 0, reserved by M4.4b so no leased chain collides")
                .isZero();
    }

    @Test
    void aNegativeEpochIsRefused() {
        assertThatThrownBy(() -> new CommitLog(new MemoryBinStore(), "p", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void twoEpochsWriteToDisjointPrefixesAndNeitherSeesTheOthersDeltas() throws Exception {
        // ⚠️ THE FENCING PROPERTY ITSELF (I3): a reader applies only deltas in
        // a sealed prefix or a LATER epoch's chain, never in a discarded
        // suffix. Here the new epoch simply cannot see the old one's writes,
        // because they are not under its prefix at all.
        MemoryBinStore shared = new MemoryBinStore();
        CommitLog old = new CommitLog(shared, "p", 0);
        old.commit("seg-old", counts(new RunKey(A, 0), 4));

        CommitLog fresh = new CommitLog(shared, "p", 1);
        fresh.commit("seg-new", counts(new RunKey(B, 0), 7));
        CommitLog reader = new CommitLog(shared, "p", 1);
        reader.recover();
        // ⚠️ A POSITIVE CONTROL first: round-1 test review measured that this
        // test survived `recover()` with an EMPTY BODY, because every other
        // assertion here is that something is NOT seen. Asserting the epoch's
        // own commit IS seen is what stops "sees nothing at all" passing for
        // "sees nothing of the other epoch".
        assertThat(reader.nextOffset(new RunKey(B, 0)))
                .as("the epoch DOES see its own chain").isEqualTo(7);

        CommitLog fresh2 = new CommitLog(shared, "p", 2);
        fresh2.recover();
        assertThat(fresh2.nextSequence()).as("a new epoch starts its own chain at 0").isZero();
        assertThat(fresh2.nextOffset(new RunKey(A, 0)))
                .as("and sees none of the old epoch's offsets from its own prefix")
                .isZero();
    }

    @Test
    void aFencedWritersLaterPutLandsWhereTheNewEpochsReaderNeverLooks() throws Exception {
        // ⚠️ The whole point of putting the epoch in the PATH rather than
        // checking it on write: the fenced leader does not have to be stopped
        // in time. It keeps writing, and its writes are simply invisible.
        MemoryBinStore shared = new MemoryBinStore();
        CommitLog fenced = new CommitLog(shared, "p", 0);
        CommitLog current = new CommitLog(shared, "p", 1);
        current.commit("seg-new", counts(new RunKey(A, 0), 2));

        // the fenced leader, unaware, commits again
        fenced.commit("seg-zombie", counts(new RunKey(A, 0), 99));

        CommitLog reader = new CommitLog(shared, "p", 1);
        reader.recover();
        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("the zombie's 99 records are not in this epoch's chain")
                .isEqualTo(2);
    }
}
