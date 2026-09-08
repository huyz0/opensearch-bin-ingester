// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
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
 * WRITING the seal — the branch M4.5 and M4.6a could only read.
 *
 * <p>⚠️ THE SEAL HAS TWO LOSING BRANCHES AND THEY ARE OPPOSITE. A fenced old
 * leader that loses {@code N+1} treats the loss as proof it is fenced and stops
 * ({@code CommitLogSealTest}). A NEW leader holding a valid lease must REDRIVE
 * to {@code N+2} instead: it is racing the old leader's in-flight commits, and
 * stopping would surrender sequencing cluster-wide while holding the lease — a
 * self-inflicted outage, which is the thing this branch exists to prevent.
 *
 * <p>⚠️ CONSECUTIVENESS IS WHAT MAKES THIS WORK. A fenced leader must claim
 * {@code N+1} before {@code N+2}, so it necessarily collides with the seal. The
 * protocol never asks whether a slot is empty — it CLAIMS it. A timestamp-keyed
 * or randomly-keyed log would have no such property.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitLogSealWriteTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static void put(MemoryBinStore store, String key, byte[] bytes) throws IOException {
        store.putIfAbsent(key, new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
    }

    /** A commit the OLD leader lands while the new one is trying to seal. */
    private static void oldLeaderCommits(MemoryBinStore store, CommitLog log, long seq,
            int records, long firstOffset) throws IOException {
        put(store, log.keyFor(seq), new CommitDelta(seq, "seg/zombie-" + seq,
                List.of(new RunCommit(new RunKey(A, 0), records, firstOffset))).encode());
    }

    @Test
    void sealingAnUncontendedChainClosesItAtTheNextFreeSlot() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));

        // ⚠️ 9, NOT 2. Every other fixture seals an epoch-1 chain continuing at
        // 2, and `LocalSequencer` always seals E-1 with E -- so `continuedAt`
        // coincided with `epoch + 1` everywhere and ignoring the parameter
        // entirely survived. Same shape as the `prevEpoch = epoch - 1` finding,
        // one parameter over.
        Seal seal = log.seal(9, 5);

        assertThat(seal.sequence()).as("the seal claims the next free slot").isEqualTo(1);
        assertThat(seal.continuedAt())
                .as("and names the epoch it was TOLD, not one derived from its own")
                .isEqualTo(9);
        assertThat(store.stat(log.keyFor(1))).as("and it is durable").isPresent();
    }

    @Test
    void sealingRedrivesPastADeltaTheOldLeaderWonRatherThanSurrenderingTheCluster()
            throws Exception {
        // ⚠️ THE WHOLE TASK. The new leader proposes a seal at slot 1 and LOSES
        // -- the fenced old leader, which does not yet know it is fenced, landed
        // a commit there first. Stopping here would leave the cluster with no
        // sequencer while this node holds a valid lease.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        oldLeaderCommits(store, log, 1, 4, 3);

        Seal seal = log.seal(2, 5);

        assertThat(seal.sequence())
                .as("the seal redrove to the next slot instead of giving up")
                .isEqualTo(2);
        assertThat(log.nextOffset(new RunKey(A, 0)))
                .as("and the zombie's commit was READ and applied on the way past, "
                        + "because a redrive re-reads rather than blind-retrying")
                .isEqualTo(7);
    }

    @Test
    void theBoundIsTheBudgetGivenRatherThanTheNumberOfWritesAlreadyInFlight()
            throws Exception {
        // ⚠️ THE NEGATIVE CONTROL FOR THE BOUND, and the spec calls this out by
        // name: capping the redrive at the old leader's IN-FLIGHT DEPTH gives up
        // early, because a fenced leader keeps originating commits for up to a
        // full renew interval after being fenced. The budget must be the
        // caller's, derived from that interval -- so the same four contenders
        // that defeat a budget of 2 must be cleared by a budget of 4.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        for (long seq = 1; seq <= 4; seq++) {
            oldLeaderCommits(store, log, seq, 1, 2 + seq);
        }

        Seal seal = log.seal(2, 4);

        assertThat(seal.sequence())
                .as("a budget large enough clears every contender and seals beyond them")
                .isEqualTo(5);
    }

    @Test
    void sealingAChainAnotherLeaderAlreadySealedAdoptsThatSealRatherThanFailing()
            throws Exception {
        // ⚠️ Losing to a SEAL is not the same as losing to a delta. Nothing is
        // wrong: the chain is closed, which is the outcome being asked for. A
        // leader that failed here would abandon a takeover that had SUCCEEDED.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        byte[] theirs = new Seal(1, 9).encode();
        put(store, log.keyFor(1), theirs);

        Seal seal = log.seal(2, 5);

        assertThat(seal.sequence()).as("the existing seal is adopted").isEqualTo(1);
        assertThat(seal.continuedAt())
                .as("including ITS continuation epoch, not the one we proposed")
                .isEqualTo(9);
    }

    @Test
    void openingAChainThatThisNodeAlreadyOpenedIsIdempotent() throws Exception {
        // ⚠️ The javadoc INVITES a caller to re-run this after a partial start,
        // and nothing tested that it may. `open()` had NO direct test at all --
        // it was reached only through `LocalSequencer.start`, always on a fresh
        // empty epoch, so `putIfAbsent` always won and every line below that
        // win was unreachable from the suite.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 2);

        Continue first = log.open(1, 5);
        // ⚠️ ASSERTED BETWEEN THE TWO CALLS. Checking only after both left the
        // two `apply(opening)` sites pinned as a DISJUNCTION -- either alone
        // satisfied it. The one with teeth is the win branch: without it
        // `nextSequence` stays 0 after `LocalSequencer.start`, so the leader's
        // first commit races slot 0 against its own CONTINUE, loses, and spends
        // a GET reading it -- a wasted PUT and GET on every failover.
        assertThat(log.nextSequence())
                .as("the winning open consumed slot 0 straight away").isEqualTo(1);
        Continue again = log.open(1, 5);

        assertThat(again).as("re-opening returns the same CONTINUE").isEqualTo(first);
        assertThat(log.nextSequence())
                .as("and slot 0 is consumed exactly once, not twice")
                .isEqualTo(1);
    }

    @Test
    void openingAChainSomebodyElseOpenedDIFFERENTLYIsRefusedRatherThanAbsorbed()
            throws Exception {
        // ⚠️ THE FORK, ARRIVING SILENTLY. Relaxing the equality check to accept
        // whatever is at slot 0 left the whole sequencer suite green -- and under it two pods
        // handed the same epoch by a lease ambiguity would BOTH report having
        // opened the chain, both commit, and both assign offsets from 0. This
        // module has four test classes about lease ambiguity precisely because
        // that hand is dealable.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 2);
        put(store, log.keyFor(0), new Continue(0, 1, 99).encode());

        assertThatThrownBy(() -> log.open(1, 5))
                .as("a chain opened by someone else is reported, never absorbed")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("opened by something else");
    }

    @Test
    void afterAdoptingAnotherLeadersSealThisWriterIsBoundByItToo() throws Exception {
        // ⚠️ FOUND BY MUTATION, not by review: deleting `sealedAt = existing`
        // in the adopt branch killed NOTHING. The test below covers the seal
        // this writer WON; nothing covered the seal it ADOPTED. Under that
        // mutation a leader whose takeover lost the seal race learns the chain
        // is closed, says so to its caller, and is then still free to commit to
        // it -- I5, held by a writer that has already read the barrier.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        put(store, log.keyFor(1), new Seal(1, 9).encode());

        log.seal(2, 5);

        assertThatThrownBy(() -> log.commit("seg/after", counts(new RunKey(A, 0), 2)))
                .as("a seal you adopted binds you exactly as hard as one you wrote")
                .isInstanceOf(IOException.class)
                // ⚠️ THEIR seal, not the one we proposed. The fixture is built so
                // the two differ (adopted continues at 9, ours proposed 2) and
                // matching only "sealed" threw that discrimination away: binding
                // to the WRONG seal left the writer refused, but pointing a
                // cross-boundary reader at a chain that does not exist.
                .hasMessageContaining("continued at epoch 9");
    }

    @Test
    void theBudgetBoundaryIsExactRatherThanOffByOne() throws Exception {
        // ⚠️ THIS IS ALSO THE ONLY BOUND TEST. A sibling asserting budget 2
        // against the same four contenders was deleted: for any implementation
        // monotone in `maxRedrives`, "fails at 2" is implied by "fails at 3",
        // so its kill set was a strict SUBSET of this one's and it was never
        // the sole killer of anything. Keeping it would read as coverage of the
        // bound while this test does the work -- the same trade M4.6a recorded.
        // ⚠️ ALSO FOUND BY MUTATION: `redrives >= maxRedrives` -> `>` killed
        // nothing, because the existing budget tests sit either side of the
        // boundary rather than ON it. Four contenders need exactly four
        // redrives, so a budget of THREE must fail -- under the off-by-one it
        // succeeds. An operator sizing this budget from the renew interval is
        // entitled to have the number mean what it says.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        for (long seq = 1; seq <= 4; seq++) {
            oldLeaderCommits(store, log, seq, 1, 2 + seq);
        }

        assertThatThrownBy(() -> log.seal(2, 3))
                .as("four contenders need four redrives; three is one short")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not converge");
    }

    @Test
    void sealingAChainThisWriterALREADYKNOWSIsSealedWritesNoSecondSeal()
            throws Exception {
        // ⚠️ THE M4.6a HOLE, ONE METHOD OVER, and review found it: `seal` had no
        // `sealedAt` guard where `commit` has one. A fresh log that recovers an
        // already-sealed chain proposed at sealSeq+1, WON uncontended, and wrote
        // a SECOND seal past the barrier -- measured, "first seal seq=1, second
        // seal seq=2".
        // ⚠️ Downstream that is worse than untidy: the CONTINUE would name a slot
        // past the real boundary, which is precisely what M4.6e's crossing
        // reader follows.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        first.seal(2, 5);

        CommitLog second = new CommitLog(store, "bins", 1);
        second.recover();
        Seal adopted = second.seal(2, 5);

        assertThat(adopted.sequence())
                .as("it adopts the seal already there rather than writing past it")
                .isEqualTo(1);
        assertThat(store.stat(second.keyFor(2)))
                .as("and slot 2 stays empty -- no second seal beyond the barrier")
                .isEmpty();
    }

    @Test
    void recoverChainEndPAGESToTheRealEndRatherThanStoppingAtTheFirstPage()
            throws Exception {
        // ⚠️ M4.6a'S MAJOR, VERBATIM, IN A NEW METHOD. Collapsing the paging loop
        // to a single `list()` survived the whole suite, because every fixture
        // holds fewer objects than the 1000 asked for. On a chain past one page
        // a takeover would compute its end from the FIRST page and then either
        // blow its redrive budget -- failing over while HOLDING the lease -- or
        // grind a PUT and a GET per entry to catch up.
        // ⚠️ `SmallPageStore` already existed for exactly this, one method over,
        // and nothing pointed it at this one.
        MemoryBinStore backing = new MemoryBinStore();
        CommitLog seed = new CommitLog(backing, "bins", 1);
        seed.commit("seg/0", counts(new RunKey(A, 0), 1));
        seed.commit("seg/1", counts(new RunKey(A, 0), 1));
        seed.commit("seg/2", counts(new RunKey(A, 0), 1));

        CommitLog probe = new CommitLog(new SmallPageStore(backing, 2), "bins", 1);
        probe.recoverChainEnd();

        assertThat(probe.nextSequence())
                .as("three entries at page size two is TWO pages; the end is 3, not 2")
                .isEqualTo(3);
    }

    @Test
    void recoverChainEndREMEMBERSASealSoTheGuardCanFire() throws Exception {
        // ⚠️ WITHOUT THIS THE ROUND-2 GUARD IS DEAD CODE. `seal()` refuses when
        // `sealedAt` is set -- but if this method never sets it, the guard cannot
        // fire, the writer proposes at sealSeq+1, WINS UNCONTENDED, and puts a
        // second seal past the barrier. That is I5, and the CONTINUE would then
        // name a slot past the real boundary, which is exactly what M4.6e
        // follows across.
        // ⚠️ The mirror of `sealingAChainThisWriterALREADYKNOWSIsSealed...`,
        // which pins the same property on the `recover()` path.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, "bins", 1);
        first.commit("seg/0", counts(new RunKey(A, 0), 3));
        first.seal(9, 5);

        CommitLog probe = new CommitLog(store, "bins", 1);
        probe.recoverChainEnd();
        Seal adopted = probe.seal(9, 5);

        assertThat(adopted.sequence())
                .as("the boundary probe saw the seal, so sealing again adopts it")
                .isEqualTo(1);
        assertThat(store.stat(probe.keyFor(2)))
                .as("and nothing landed past the barrier").isEmpty();
    }

    @Test
    void afterSealingAChainThisWriterRefusesToCommitToIt() throws Exception {
        // ⚠️ Sealing a chain is the strongest possible statement that you will
        // not write to it again. M4.6a made `recover` remember a seal it READ;
        // a seal this instance WROTE must bind it just as hard, or the leader
        // that closed the chain is the one node still able to violate I5 on it.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(new RunKey(A, 0), 3));
        log.seal(2, 5);

        assertThatThrownBy(() -> log.commit("seg/after", counts(new RunKey(A, 0), 2)))
                .as("the writer that sealed the chain is bound by its own seal")
                .isInstanceOf(IOException.class)
                // ⚠️ WHICH seal, mirroring the adopt test one method down. Binding
                // to a seal with the wrong `continuedAt` leaves the writer
                // correctly refused while pointing M4.6e's crossing reader at a
                // chain nobody opened -- and matching only "sealed" could not
                // tell the two apart.
                .hasMessageContaining("continued at epoch 2");
    }
}
