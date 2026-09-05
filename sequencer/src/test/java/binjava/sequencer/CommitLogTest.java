// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.binstore.Body;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.Seal;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** ⚠️ The store is the only coordinator (ADR-0002); offsets are assigned here (ADR-0001). */
// ⚠️ A CLASS-LEVEL TIMEOUT because `commit`'s retry loop is unbounded by
// design: a mutation that stops `nextSequence` advancing past an inert entry
// makes a writer re-race the same slot forever, so the test HANGS instead of
// failing and Gradle writes no XML. That is the JVM-crash blind spot AGENTS.md
// records, wearing a different hat -- and a hung task cannot be red-recorded.
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
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
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 0);
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
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 0);
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
        CommitLog mine = new CommitLog(shared, "p", 0);
        CommitLog theirs = new CommitLog(shared, "p", 0);

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
                    CommitLog log = new CommitLog(shared, "p", 0);
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
        CommitLog before = new CommitLog(shared, "p", 0);
        before.commit("seg-1", counts(new RunKey(A, 0), 3, new RunKey(B, 7), 5));
        before.commit("seg-2", counts(new RunKey(A, 0), 2));

        // a fresh process, nothing in memory
        CommitLog after = new CommitLog(shared, "p", 0);
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
    void recoveryFoldsOffsetsFromEVERYSegmentOfABatchedDelta() throws Exception {
        // ⚠️ THE CALLER `allRuns()` WAS ADDED FOR, and it had no test: reverting
        // `ChainReplay.fold` to `delta.runs()` — undoing the production change
        // outright — left the whole suite green, because nothing replayed a
        // chain that CONTAINED a batched delta. Once M4.7 batches, that revert
        // either throws out of `fold` or folds one pod's runs and silently
        // reassigns offsets another pod already acked, which is I2.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 0);
        byte[] batched = new CommitDelta(0, List.of(
                new binjava.format.SegmentCommit("seg-pod-a",
                        List.of(new RunCommit(new RunKey(A, 0), 3, 0))),
                new binjava.format.SegmentCommit("seg-pod-b",
                        List.of(new RunCommit(new RunKey(B, 7), 5, 0))))).encode();
        store.putIfAbsent(log.keyFor(0),
                new Body(batched.length, () -> new java.io.ByteArrayInputStream(batched)));

        CommitLog after = new CommitLog(store, "p", 0);
        after.recover();

        assertThat(after.nextOffset(new RunKey(A, 0)))
                .as("the first segment's stream advanced").isEqualTo(3);
        assertThat(after.nextOffset(new RunKey(B, 7)))
                .as("and so did the SECOND segment's -- a stream folded from only the "
                        + "first would be handed offsets another pod already used")
                .isEqualTo(5);
    }

    @Test
    void aDeltaRoundTripsThroughItsEncoding() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 0);
        CommitDelta committed =
                log.commit("some/segment/key.bseg", counts(new RunKey(A, 0), 3, new RunKey(B, 9), 1));
        byte[] encoded = committed.encode();
        CommitDelta decoded = CommitDelta.decode(encoded);
        assertThat(decoded).isEqualTo(committed);
        assertThat(decoded.segmentKey()).isEqualTo("some/segment/key.bseg");
    }

    @Test
    void aCorruptOrTruncatedDeltaIsRefused() throws Exception {
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 0);
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
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 0);
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
        CommitLog log = new CommitLog(store, "p", 0);
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
    void theChainReportsWhichTermItBelongsTo() {
        // ⚠️ `epoch()` is how a caller (M4.5's CONTINUE header, M4.6's seal)
        // learns which term a chain is; untested, `return 0` is unconstrained.
        assertThat(new CommitLog(new MemoryBinStore(), "p", 7).epoch()).isEqualTo(7);
        // ⚠️ EPOCH 0 IS STILL A LEGAL ARGUMENT, and this is the assertion that
        // keeps it so: M4.6f removed the two-arg constructor that DEFAULTED to
        // it, not the value. `CONTINUE` uses `prevEpoch = 0` for "no
        // predecessor", and a test must still be able to build the historical
        // unleased chain deliberately -- what is gone is reaching it by
        // omission.
        assertThat(new CommitLog(new MemoryBinStore(), "p", 0).epoch()).isZero();
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

    @Test
    void recoveryAdvancesPastAContinueThatOpensItsChain() throws Exception {
        // ⚠️ EVERY READER UPDATED IN THE SAME COMMIT (M4.5). A CONTINUE carries
        // no runs, so it moves no offsets -- but it DOES consume a sequence
        // number on purpose, and a reader that folds only deltas leaves
        // `nextSequence` pointing at a slot that is already taken. The next
        // commit then races for it, loses, re-reads, and races for the same
        // slot again: a spin, not an error, which is the failure mode that does
        // not announce itself.
        // ⚠️ A REPRESENTABLE CHAIN: epoch 2's own chain, opened by a CONTINUE
        // at seq 0 linking back to epoch 1. Written directly, because WRITING
        // these is M4.6's job and this commit only teaches readers.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog epoch2 = new CommitLog(store, "bins", 2);
        byte[] cont = new Continue(0, 1, 5).encode();
        store.putIfAbsent(epoch2.keyFor(0), new Body(cont.length,
                () -> new java.io.ByteArrayInputStream(cont)));

        CommitLog reader = new CommitLog(store, "bins", 2);
        reader.recover();
        // ⚠️ The chain is the CONTINUE and NOTHING ELSE -- a leader that opened
        // its chain and has not committed yet. With a delta after it the test
        // would not discriminate: the delta's own sequence carries
        // `nextSequence` to the same place, so the mutation would survive.
        // ⚠️ NO commit afterwards, deliberately. Under that mutation a commit
        // would race for a taken slot, lose, re-read, and race again: the test
        // would HANG rather than fail, which is the JVM-crash blind spot in
        // `check-tdd` wearing a different hat.
        assertThat(reader.nextSequence())
                .as("the CONTINUE consumed slot 0, so the next free slot is 1")
                .isEqualTo(1);
        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("and it moved no offsets, because it commits no runs")
                .isZero();
    }

    @Test
    void recoveryAdvancesPastASealWithoutApplyingIt() throws Exception {
        // ⚠️ Deliberately asserts NOTHING about committing afterwards, and
        // that restraint is why it did not have to be inverted. When this was
        // written a writer whose recovered state sat beyond a SEAL won its slot
        // outright and never reached `commit`'s fenced check -- I5 by a second
        // route. Asserting a commit SUCCEEDS here would have specified the
        // unsafe path. M4.6a closed it: `recover` now stops at the seal and
        // `commit` refuses on it, and the assertions below still hold unchanged
        // because the seal's slot is still counted. `CommitLogSealTest` owns
        // the refusal.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        byte[] seal = new Seal(1, 2).encode();
        store.putIfAbsent(log.keyFor(1), new Body(seal.length,
                () -> new java.io.ByteArrayInputStream(seal)));

        CommitLog reader = new CommitLog(store, "bins", 1);
        reader.recover();
        assertThat(reader.nextSequence())
                .as("the seal consumed a sequence number, so recovery counts it")
                .isEqualTo(2);
        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("and applied none of its own, because it commits no runs")
                .isEqualTo(3);
    }

    @Test
    void recoveryStopsOnAnEntryItCannotUnderstandRatherThanSkippingIt() throws Exception {
        // ⚠️ THE RULE ADR-0028 LEADS WITH, tested where it can actually be
        // broken. That an unknown kind makes `decode` THROW is proved in the
        // codec's own tests -- but `decode` returns a ChainEntry, so skipping
        // is structurally impossible there. The only place a chain can lose a
        // SEAL is this loop, and a `catch (IOException) { continue; }` here
        // would leave a reader applying a discarded suffix, which is I3.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        byte[] alien = new Seal(1, 2).encode();
        alien[8] = (byte) 0x63;
        store.putIfAbsent(log.keyFor(1), new Body(alien.length,
                () -> new java.io.ByteArrayInputStream(alien)));

        assertThatThrownBy(() -> new CommitLog(store, "bins", 1).recover())
                .as("an entry this build cannot read stops recovery; it is never skipped")
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("kind");
    }

    @Test
    void losingASlotToASealStopsRatherThanWritingPastTheBarrier() throws Exception {
        // ⚠️ INVARIANT I5, which the corpus singles out as "the one a plausible
        // implementation violates by accident" -- no acknowledged commit exists
        // beyond a SEAL in its own chain.
        // ⚠️ This is a regression M4.5 CREATED and had to close in the same
        // commit. Before it, a seal was an unreadable version and the writer
        // failed closed by accident; teaching the reader to understand one made
        // `apply` fold it in and retry at the next slot -- writing past the
        // barrier and ACKING it, with recovery then applying a discarded
        // suffix (I3). A read side that ships first must be no less safe than
        // the one it replaces.
        // ⚠️ Epoch 1, not 0: the unleased chain has no leader to fence, so it
        // is never sealed. The behaviour here is epoch-independent, but a
        // fixture that cannot arise makes a reader doubt the rest of it.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog fenced = new CommitLog(store, "bins", 1);
        fenced.commit("seg/0", counts(new RunKey(A, 0), 3));

        // A new leader at epoch 2 seals the old chain at the slot this writer
        // is about to want.
        byte[] seal = new Seal(1, 2).encode();
        store.putIfAbsent(fenced.keyFor(1), new Body(seal.length,
                () -> new java.io.ByteArrayInputStream(seal)));

        assertThatThrownBy(() -> fenced.commit("seg/1", counts(new RunKey(A, 0), 2)))
                .as("losing to a seal is proof of being fenced, not ordinary contention")
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("sealed");
        assertThat(store.stat(fenced.keyFor(2)))
                .as("and nothing was written past the barrier")
                .isEmpty();
    }

    @Test
    void losingASlotToAContinueIsOrdinaryContentionAndRetries() throws Exception {
        // ⚠️ THE BARRIER IS THE SEAL, AND ONLY THE SEAL. Without this, widening
        // the fenced check to `instanceof Seal || instanceof Continue` passes
        // every other test -- and an over-broad fail-closed is a liveness bug
        // in the same family as the one it is guarding against: a writer would
        // stop dead on an entry that means "carry on here".
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 2);
        byte[] cont = new Continue(0, 1, 5).encode();
        store.putIfAbsent(log.keyFor(0), new Body(cont.length,
                () -> new java.io.ByteArrayInputStream(cont)));

        assertThat(log.commit("seg/0", counts(new RunKey(A, 0), 3)).sequence())
                .as("it lost slot 0 to a CONTINUE, folded it in, and took slot 1")
                .isEqualTo(1);
    }
}
