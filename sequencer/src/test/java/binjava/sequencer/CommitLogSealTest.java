// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A SEAL is a BARRIER during recovery, not an entry to count and move past.
 *
 * <p>⚠️ M4.5 shipped the read side and closed this in {@code commit} only: a
 * writer that LOSES a slot to a seal fails closed. It cannot close it for a
 * writer that never loses one. Recovery advanced {@code nextSequence} past the
 * seal, so the next commit picked a FREE slot, won it outright, and never
 * reached that check -- I5 by a second route, with no lost race anywhere in it.
 */
// ⚠️ CLASS-LEVEL TIMEOUT, for the reason CommitLogTest carries one: `commit`'s
// retry loop is unbounded by design, so a mutation that stops `nextSequence`
// advancing makes a writer re-race one slot forever. The test HANGS rather than
// failing, Gradle writes no `<failure>`, and AGENTS.md records that a hang
// cannot be red-recorded.
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitLogSealTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static void put(MemoryBinStore store, String key, byte[] bytes) throws IOException {
        store.putIfAbsent(key, new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
    }

    /** Puts a SEAL at {@code seq}, as a fencing leader would. */
    private static void seal(MemoryBinStore store, CommitLog log, long seq, long continuedAt)
            throws IOException {
        put(store, log.keyFor(seq), new Seal(seq, continuedAt).encode());
    }

    @Test
    void aWriterThatRecoveredPastASealRefusesToCommitRatherThanWritingBeyondIt()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        seal(store, first, 1, 2);

        // A FRESH reader, which is the whole scenario: it has no memory of
        // having lost anything, so `commit`'s lost-race branch is unreachable
        // for it. Slot 2 is empty and its `putIfAbsent` WOULD win.
        CommitLog fenced = new CommitLog(store, "bins", 1);
        fenced.recover();
        // ⚠️ PINS THE NAME. Without this the test passes for the wrong reason:
        // a mutation that stops at the seal WITHOUT remembering it leaves
        // `nextSequence` at 1, so `commit` races slot 1, LOSES to the seal, and
        // throws from the old lost-race branch with a byte-identical message.
        // Measured -- the test passed while never recovering past a seal.
        assertThat(fenced.nextSequence())
                .as("recovery counted the seal's slot, so this writer WOULD have "
                        + "won an empty slot 2 rather than losing a race")
                .isEqualTo(2);

        assertThatThrownBy(() -> fenced.commit("seg/1", counts(new RunKey(A, 0), 5)))
                .as("a chain that has been sealed accepts no further commits, "
                        + "whether or not this writer lost a race to learn it")
                .isInstanceOf(IOException.class)
                // ⚠️ The FIELDS, not just the word. Swapping `sequence()` and
                // `continuedAt()` inside the shared helper left everything
                // green when only "sealed" was matched.
                .hasMessageContaining("sealed at seq 1")
                .hasMessageContaining("epoch 2");
        // ⚠️ AND NOTHING LANDED. Moving the guard to AFTER the PUT succeeds
        // leaves every other assertion here true while the fenced writer has
        // already written a delta into the sealed chain -- which is I5, the
        // thing being refused, happening anyway.
        assertThat(store.stat(fenced.keyFor(2)))
                .as("no delta was written past the barrier")
                .isEmpty();
    }

    @Test
    void aSealAtSequenceZeroFencesTheChainExactlyAsOneAtAnyOtherSlot() throws Exception {
        // ⚠️ EVERY OTHER SEAL IN THIS REPO SITS AT SEQUENCE 1, so `if
        // (seal.sequence() > 0)` guarding the barrier survived the whole suite.
        // Measured on this chain: the commit SUCCEEDED at seq 1, an
        // acknowledged commit beyond a seal in its own chain -- I5.
        // ⚠️ Representable, and M4.6c writes it -- but NOT for the reason a
        // first draft of this comment gave. "A leader that opened its chain and
        // was fenced before committing" seals at ONE, because its CONTINUE
        // occupies slot 0. A seal at 0 needs the stronger case: a lease
        // acquired and the holder dead before writing even the CONTINUE, so its
        // chain is empty and the successor seals the first slot there is.
        // M4.5's `prevSeq=0` is the successor truthfully recording that.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        seal(store, log, 0, 2);

        CommitLog fenced = new CommitLog(store, "bins", 1);
        fenced.recover();
        // ⚠️ Same trap test 1 fell into, mirrored here. Without this a mutation
        // that stops at the seal WITHOUT remembering it leaves `nextSequence`
        // at 0, so `commit` races slot 0, LOSES to the seal, and throws the
        // identical message from the old lost-race branch -- passing while
        // never having recovered past a seal.
        assertThat(fenced.nextSequence())
                .as("recovery counted the seal's slot, so the refusal below is "
                        + "the recovered barrier and not an ordinary lost race")
                .isEqualTo(1);

        assertThatThrownBy(() -> fenced.commit("seg/0", counts(new RunKey(A, 0), 4)))
                .as("sequence 0 is a slot like any other; a seal there fences the chain")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sealed at seq 0");
        assertThat(store.stat(fenced.keyFor(1)))
                .as("and nothing was written after it").isEmpty();
    }

    @Test
    void recoveryDoesNotApplyTheDiscardedSuffixAfterASeal() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        seal(store, first, 1, 2);
        // A delta PAST the seal. Today only a fenced leader mid-flight writes
        // one; once it exists it is a discarded suffix, and applying it is I3.
        put(store, first.keyFor(2), new CommitDelta(2, "seg/late",
                List.of(new RunCommit(new RunKey(A, 0), 6, 3))).encode());

        CommitLog reader = new CommitLog(store, "bins", 1);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("offsets stop at the seal; the suffix beyond it was discarded")
                .isEqualTo(3);
        assertThat(reader.nextSequence())
                .as("and the chain ends at the seal, which consumed slot 1")
                .isEqualTo(2);
    }

    @Test
    void recoveryStopsTheListAtTheSealRatherThanPagingOnPastIt() throws Exception {
        // ⚠️ `return` vs `break` -- INDISTINGUISHABLE on every other fixture in
        // this repo, because each holds fewer objects than the 1000 `recover`
        // asks for, so `nextStartAfter` is always empty and the outer loop
        // never runs twice. Measured: the mutation survived the whole sequencer
        // suite, and
        // on a chain long enough to page it applied the discarded suffix.
        // `SmallPageStore` buys that discrimination with 3 objects instead of
        // 1100.
        MemoryBinStore backing = new MemoryBinStore();
        CommitLog first = new CommitLog(backing, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        seal(backing, first, 1, 2);
        put(backing, first.keyFor(2), new CommitDelta(2, "seg/late",
                List.of(new RunCommit(new RunKey(A, 0), 6, 3))).encode());

        // Page size 2, so the seal ends page ONE and the discarded suffix sits
        // on page two -- reachable only if recovery keeps paging.
        CountingBinStore counting =
                new CountingBinStore(new SmallPageStore(backing, 2));
        CommitLog reader = new CommitLog(counting, "bins", 1);
        reader.recover();

        assertThat(counting.counts().lists())
                .as("recovery stopped at the seal instead of fetching page two")
                .isEqualTo(1);
        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("so the suffix on page two was never applied")
                .isEqualTo(3);
    }

    @Test
    void theSamePaginatedChainWithoutASealTakesBothPages() throws Exception {
        // ⚠️ THE CONTROL FOR THE TEST ABOVE, varying exactly one conjunct: same
        // three objects, same page size, seal replaced by an ordinary delta.
        // Without it `lists() == 1` is satisfied by two different worlds -- the
        // seal stopped recovery, OR there was never a second page at all.
        // Measured: deleting `SmallPageStore`'s `Math.min` clamp leaves the
        // whole suite green AND re-arms the `break` mutation the test above
        // exists to kill, because a decorator that no longer paginates makes
        // both look identical. This test fails the moment the clamp stops
        // clamping, which is what gives `lists() == 1` its meaning.
        MemoryBinStore backing = new MemoryBinStore();
        CommitLog first = new CommitLog(backing, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        put(backing, first.keyFor(1), new CommitDelta(1, "seg/mid",
                List.of(new RunCommit(new RunKey(A, 0), 2, 3))).encode());
        put(backing, first.keyFor(2), new CommitDelta(2, "seg/late",
                List.of(new RunCommit(new RunKey(A, 0), 6, 5))).encode());

        CountingBinStore counting =
                new CountingBinStore(new SmallPageStore(backing, 2));
        CommitLog reader = new CommitLog(counting, "bins", 1);
        reader.recover();

        assertThat(counting.counts().lists())
                .as("three objects at page size two IS two pages -- so the page "
                        + "size is real, and the seal above is what stopped the second fetch")
                .isEqualTo(2);
        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("and with no seal every entry applies")
                .isEqualTo(11);
    }

    @Test
    void recoveryDoesNotStopAtAContinueBecauseTheBarrierIsTheSealAlone() throws Exception {
        // ⚠️ THE NEGATIVE CONTROL. M4.5 shipped one for the sibling check in
        // `commit` and said why: widening the barrier to `Seal || Continue`
        // passes every test that only ever builds sealed chains. This commit
        // put the same barrier in `recover` and shipped no matching control,
        // so `if (!(entry instanceof CommitDelta)) return;` survived.
        // ⚠️ The harm is not theoretical once M4.6c writes the CONTINUE that
        // OPENS a chain: recovery would return at seq 0 having applied
        // nothing, and the leader would re-issue offsets the chain already
        // assigned.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 2);
        put(store, log.keyFor(0), new Continue(0, 1, 5).encode());
        put(store, log.keyFor(1), new CommitDelta(1, "seg/first",
                List.of(new RunCommit(new RunKey(A, 0), 5, 0))).encode());

        CommitLog reader = new CommitLog(store, "bins", 2);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("a CONTINUE is ordinary chain content; recovery reads straight through it")
                .isEqualTo(5);
        assertThat(reader.nextSequence())
                .as("and both entries consumed a slot")
                .isEqualTo(2);
    }
}
