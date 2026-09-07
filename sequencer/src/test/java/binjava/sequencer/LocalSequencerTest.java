// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import binjava.format.SegmentCommit;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Becoming the leader is one act, not three.
 *
 * <p>⚠️ A node that acquires the lease and starts committing WITHOUT sealing its
 * predecessor has forked the log: two chains are live, both acking, and neither
 * reader can see the other. So the seal, the CONTINUE and the first commit are
 * one commit's worth of behaviour — shipping the CONTINUE without the seal would
 * leave exactly the fork this task exists to close.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    private static final String PREFIX = "bins/cluster-a";

    private static final class TestClock extends Clock {
        private long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId z) {
            return this;
        }
    }

    private static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW),
                new TestClock());
    }

    private static LocalSequencer start(BinStore store, String pod) throws IOException {
        return LocalSequencer.start(store, PREFIX, manager(store, pod), 8).orElseThrow();
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(A, 0), n);
        return m;
    }

    private static CommitRequest request(String pod, long flushSeq, String seg, int n) {
        return new CommitRequest(pod, "i1", flushSeq, seg, counts(n));
    }

    private static ChainEntry entryAt(BinStore store, long epoch, long seq) throws IOException {
        CommitLog addressing = new CommitLog(store, PREFIX, epoch);
        try (InputStream in = store.get(addressing.keyFor(seq))) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }

    @Test
    void theFirstLeaderOpensItsChainWithAContinueNamingNoPredecessor() throws Exception {
        // ⚠️ Epoch 0 is RESERVED for "no lease" (M4.4b), so the first leader's
        // predecessor is not a chain at all. `prevEpoch = 0, prevSeq = 0` is not
        // a placeholder -- it TRUTHFULLY says the previous chain was empty,
        // which M4.5 chose over an explicit absent-marker.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer seq =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();

        assertThat(seq.epoch()).as("the first term is epoch 1, not 0").isEqualTo(1);
        assertThat(entryAt(store, 1, 0))
                .as("the chain is OPENED by a CONTINUE at slot 0")
                .isEqualTo(new Continue(0, 0, 0));
    }

    @Test
    void takingOverSealsThePredecessorsChainBeforeOpeningItsOwn() throws Exception {
        // ⚠️ THE FORK THIS TASK CLOSES. Without the seal the old leader's chain
        // stays open: it keeps acking commits no reader at the new epoch will
        // ever list, and it re-issues offsets the new chain also assigns.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();

        LocalSequencer second =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 8).orElseThrow();

        assertThat(second.epoch()).as("the successor's term is one higher").isEqualTo(2);
        assertThat(entryAt(store, 1, 2))
                .as("the predecessor's chain is SEALED, and it names where to continue")
                .isEqualTo(new Seal(2, 2));
    }

    @Test
    void theContinueNamesTheSealSoAReaderCanCrossTheBoundary() throws Exception {
        // ⚠️ The two halves are a PAIR: the seal says "continued at epoch 2" and
        // the CONTINUE says "I follow epoch 1 at seq 2". A reader that has one
        // and not the other cannot cross, so both are written by this one act.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();

        LocalSequencer second =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 8).orElseThrow();

        assertThat(entryAt(store, second.epoch(), 0))
                .as("the CONTINUE points back at the predecessor's SEALED slot")
                .isEqualTo(new Continue(0, 1, 2));
    }

    @Test
    void theFirstLeaderDoesNotTouchTheReservedUnleasedChain() throws Exception {
        // ⚠️ FOUND BY MUTATION: `prevEpoch >= 1` -> `>= 0` killed nothing, and
        // it is not cosmetic. Epoch 0 is RESERVED for the unleased chain, which
        // has no leader to fence -- so a seal there fences nobody, while a live
        // unleased writer is still appending to it. The first leader would
        // silently stop that writer and consume a slot in a chain it does not
        // own. M4.6d removes the unleased path entirely; until it does, this is
        // the guard that keeps the two apart.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog unleased = new CommitLog(store, PREFIX, 0);
        unleased.commit("seg/unleased", counts(3));
        long before = store.list(unleased.logPrefix(), null, 100).objects().size();

        LocalSequencer leader =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();

        assertThat(store.list(unleased.logPrefix(), null, 100).objects().size())
                .as("the reserved chain gains nothing -- no seal, no slot consumed")
                .isEqualTo(before);
        // ⚠️ THE READ SIDE, and it was unpinned. There are TWO epoch-0 guards --
        // one on the recover path, one in `open`'s inheritance -- and only the
        // first was constrained. Deleting the `open` one passed everything while
        // a first leader INHERITED the reserved unleased chain's offsets: the
        // M4.6d fork, arriving through the other door. The fixture already wrote
        // those 3 records; nothing asked what the leader made of them.
        assertThat(leader.commit(request("pod1", 2, "seg/1", 1))
                        .runs().getFirst().firstOffset())
                .as("a first leader starts its stream at 0, not after epoch 0's records")
                .isZero();
    }

    @Test
    void aNodeThatCannotAcquireTheLeaseOpensNoChainAtAll() throws Exception {
        // ⚠️ The negative control for every assertion above. A loser that still
        // opened a chain would be the fork, arriving by the other door.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();

        Optional<LocalSequencer> loser =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 8);

        assertThat(loser).as("only the leaseholder sequences").isEmpty();
        assertThat(store.stat(new CommitLog(store, PREFIX, 2).keyFor(0)))
                .as("and the loser opened no chain of its own").isEmpty();
    }

    @Test
    void commitsLandInThisLeadersOwnChainAfterTheContinueThatOpenedIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer seq =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();

        var delta = seq.commit(request("pod1", 1, "seg/0", 5));
        var second = seq.commit(request("pod1", 2, "seg/1", 4));

        assertThat(delta.sequence())
                .as("slot 0 belongs to the CONTINUE, so the first commit is slot 1")
                .isEqualTo(1);
        assertThat(delta.runs().getFirst().firstOffset())
                .as("and it is the first record of the stream").isZero();
        // ⚠️ THE SEAM MUST FORWARD WHAT IT WAS GIVEN. Returning a constant
        // segment key, or flattening the record counts, survived every
        // assertion above -- both are true of ANY first commit at slot 1 on a
        // fresh chain, whatever it carries. The segment key is the only pointer
        // to the durable bytes and the counts are what assign offsets.
        assertThat(delta.segmentKey())
                .as("the request's segment key reaches the delta").isEqualTo("seg/0");
        assertThat(second.runs().getFirst().firstOffset())
                .as("and the FIRST commit's count is what the second starts after")
                .isEqualTo(5);
    }

    @Test
    void aBATCHEDCommitCarriesEVERYRequestNotJustTheFirst() throws Exception {
        // ⚠️ THE REAL SEQUENCER'S BATCHED PATH HAD NO TEST AT ALL. `commitAll`
        // is the interface PRIMITIVE and this is its only production
        // implementation, but every test here drove `commit` with one request,
        // and `BatchingSequencer` is only ever composed with test-local
        // delegates -- so `log.commitAll(requests)` could have been
        // `log.commitAll(List.of(requests.getFirst()))` with the whole suite
        // green. That mutation silently drops every pod but one from a batch,
        // which is precisely what M4.7 exists to make safe: the batch is ONE
        // conditional PUT, so the dropped flushes are acked and never durable.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer seq =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();

        // ⚠️ TWO DIFFERENT PODS, because the per-POD dimension is the one M4.7's
        // row says has to be built and asserted, and DISTINCT counts, so an
        // implementation that keeps the right number of runs but pairs them
        // wrongly cannot pass.
        var delta = seq.commitAll(List.of(
                request("pod1", 1, "seg/a", 5),
                request("pod2", 7, "seg/b", 3)));

        assertThat(delta.segments())
                .as("both submissions are in the one entry")
                .hasSize(2);
        assertThat(delta.segments().stream().map(SegmentCommit::segmentKey).toList())
                .as("and each keeps its own segment key")
                .containsExactly("seg/a", "seg/b");
        // ⚠️ THE PAIRING, NOT JUST THE ARITY. Asserting only `hasSize(2)` is
        // satisfied by an implementation that returns both keys with each
        // other's offsets; the counts are 5 and 3 precisely so the second
        // segment's first offset can only be 5 if the FIRST one's count was
        // what advanced the stream.
        assertThat(delta.segments().getFirst().runs().getFirst().firstOffset())
                .as("the first segment opens the stream").isZero();
        assertThat(delta.segments().get(1).runs().getFirst().firstOffset())
                .as("and the second starts after the first one's records")
                .isEqualTo(5);
    }

    @Test
    void aTakeoverRedrivesPastTheFencedLeadersInFlightCommitRatherThanGivingUp()
            throws Exception {
        // ⚠️ THE SCENARIO BOTH M4.6b AND M4.6c EXIST FOR, and neither tested it:
        // every other takeover here follows a voluntary close() against a frozen
        // clock, so the seal always wins its first attempt. Measured -- deleting
        // seal's ENTIRE redrive loop left every test in this file green.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(backing, PREFIX, manager(backing, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();
        // The fenced leader's in-flight delta lands in the slot the successor's
        // seal is about to claim.
        CommitLog chain1 = new CommitLog(backing, PREFIX, 1);
        byte[] inFlight = new CommitDelta(2, "seg/in-flight",
                List.of(new RunCommit(new RunKey(A, 0), 2, 3))).encode();
        BinStore contended = new StealFirstPutStore(backing, chain1.keyFor(2), inFlight);

        LocalSequencer second = LocalSequencer
                .start(contended, PREFIX, manager(contended, "pod2"), 8).orElseThrow();

        assertThat(entryAt(backing, 1, 3))
                .as("the seal redrove past the in-flight commit to the next slot")
                .isEqualTo(new Seal(3, 2));
        assertThat(entryAt(backing, 2, 0))
                .as("and the CONTINUE names the slot the seal actually reached")
                .isEqualTo(new Continue(0, 1, 3));
        assertThat(second.epoch()).isEqualTo(2);
    }

    @Test
    void aTakeoverGivenNoRedriveBudgetSurrendersRatherThanSealing() throws Exception {
        // ⚠️ The other half, and the reason the budget is a PARAMETER: passing 0
        // -- or any number below what the contention needs -- inverts M4.6b's
        // whole thesis and leaves the cluster with no sequencer while this node
        // holds the lease. Both `seal(epoch, 0)` and `seal(epoch, MAX_VALUE)`
        // survived every test before this one existed.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(backing, PREFIX, manager(backing, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();
        CommitLog chain1 = new CommitLog(backing, PREFIX, 1);
        byte[] inFlight = new CommitDelta(2, "seg/in-flight",
                List.of(new RunCommit(new RunKey(A, 0), 2, 3))).encode();
        BinStore contended = new StealFirstPutStore(backing, chain1.keyFor(2), inFlight);

        assertThatThrownBy(() ->
                LocalSequencer.start(contended, PREFIX, manager(contended, "pod2"), 0))
                .as("a budget of zero cannot absorb even one lost slot")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not converge");
    }

    @Test
    void aThirdTermSealsItsImmediatePredecessorRatherThanTheFirstChain()
            throws Exception {
        // ⚠️ `prevEpoch = epoch - 1` was pinned only where it COINCIDES with the
        // constant 1: no test reached a third term, so `epoch - 1` and `1` were
        // indistinguishable. Under that mutant a third leader seals epoch 1
        // (already sealed, so it silently adopts the stale seal) and leaves
        // epoch 2 LIVE AND UNFENCED, while a reader crossing out of epoch 1
        // lands on epoch 2 rather than 3.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();
        LocalSequencer second =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 8).orElseThrow();
        second.commit(request("pod2", 1, "seg/1", 2));
        second.close();

        LocalSequencer third =
                LocalSequencer.start(store, PREFIX, manager(store, "pod3"), 8).orElseThrow();

        assertThat(third.epoch()).isEqualTo(3);
        assertThat(entryAt(store, 2, 2))
                .as("the SECOND chain is the one sealed, and it continues at 3")
                .isEqualTo(new Seal(2, 3));
        assertThat(entryAt(store, 3, 0))
                .as("and the CONTINUE follows epoch 2, not the first chain")
                .isEqualTo(new Continue(0, 2, 2));
    }

    @Test
    void takingOverAnEmptyChainSealsItAtSlotZero() throws Exception {
        // ⚠️ THE SHAPE M4.6a PREDICTED THIS TASK WOULD PRODUCE. Its row says a
        // seal at sequence 0 is "representable and imminent: a leader dead
        // before writing even the CONTINUE", and pinned it with a hand-built
        // fixture. This is the code path that actually GENERATES one: a pod that
        // wins the lease and dies before opening its chain.
        MemoryBinStore store = new MemoryBinStore();
        LeaseManager dying = manager(store, "pod1");
        dying.tryAcquire().orElseThrow();
        dying.release();

        LocalSequencer second =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 8).orElseThrow();

        assertThat(second.epoch()).isEqualTo(2);
        assertThat(entryAt(store, 1, 0))
                .as("an empty predecessor chain is sealed at the first slot there is")
                .isEqualTo(new Seal(0, 2));
        assertThat(entryAt(store, 2, 0))
                .as("and the CONTINUE names that slot")
                .isEqualTo(new Continue(0, 1, 0));
    }

    @Test
    void thePredecessorIsSealedBEFORETheSuccessorsChainIsOpened() throws Exception {
        // ⚠️ "BEFORE" IS NOT TERMINAL STATE. Reordering these two survived every
        // test, because each asserted only which objects exist when `start`
        // returns and both orders produce the same objects. Ordering needs the
        // sequence of writes, so the store records it.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(backing, PREFIX, manager(backing, "pod1"), 8).orElseThrow();
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();
        PutOrderStore ordered = new PutOrderStore(backing);

        LocalSequencer.start(ordered, PREFIX, manager(ordered, "pod2"), 8).orElseThrow();

        String sealKey = new CommitLog(backing, PREFIX, 1).keyFor(2);
        String openKey = new CommitLog(backing, PREFIX, 2).keyFor(0);
        assertThat(ordered.keys()).as("both writes happened").contains(sealKey, openKey);
        assertThat(ordered.keys().indexOf(sealKey))
                .as("the predecessor is FENCED before a chain readers can cross into exists")
                .isLessThan(ordered.keys().indexOf(openKey));
    }

    @Test
    void thePredecessorIsRECOVEREDSoTheSealCostsOneSlotRatherThanOnePerEntry()
            throws Exception {
        // ⚠️ Named for `recover()`, which production no longer calls -- it now
        // uses `recoverChainEnd()`, a boundary probe rather than a recovery. The
        // property is unchanged, so the name is drift rather than a wrong claim.
        // ⚠️ AN EARLIER DRAFT OF THIS COMMENT justified the test as "the sole
        // killer of a two-slots-short end". That was true one hash ago and THIS
        // COMMIT invalidated it: tightening the sibling's GET assertion to an
        // exact 2 made it catch any extra redrive, so three tests now kill that
        // mutant. The real justification is narrower and checkable -- this is
        // the only test asserting the SEAL ENTRY, slot and continuation, at the
        // end of a multi-entry chain through `LocalSequencer`.
        // ⚠️ The boundary walk was deletable with everything green,
        // because seal's redrive loop silently substitutes for it: starting at
        // slot 0 it loses to every existing entry in turn and eventually wins.
        // The fixtures hid it -- 2 entries against a budget of 8.
        // ⚠️ In production that is a failover that FAILS once the predecessor
        // chain is longer than the budget, and costs a wasted PUT+GET per entry
        // instead of one LIST. Here: five entries against a budget of ONE, which
        // recovery clears in a single uncontended write and the redrive cannot.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();
        for (int i = 0; i < 4; i++) {
            first.commit(request("pod1", i, "seg/" + i, 1));
        }
        first.close();

        LocalSequencer second =
                LocalSequencer.start(store, PREFIX, manager(store, "pod2"), 1).orElseThrow();

        assertThat(entryAt(store, 1, 5))
                .as("recovery found the chain's end, so ONE write sealed it")
                .isEqualTo(new Seal(5, 2));
        assertThat(second.epoch()).isEqualTo(2);
    }

    @Test
    void committingAfterCloseFailsRatherThanSilentlySucceeding() throws Exception {
        // ⚠️ The Sequencer contract says so in as many words. A commit that
        // succeeds after the lease was released is a node writing to a chain
        // another leader may already have sealed.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer seq =
                LocalSequencer.start(store, PREFIX, manager(store, "pod1"), 8).orElseThrow();
        seq.close();

        assertThatThrownBy(() -> seq.commit(request("pod1", 2, "seg/1", 1)))
                .as("a released lease is not a licence to keep sequencing")
                .isInstanceOf(IOException.class)
                // ⚠️ WHICH refusal. Every path in this stack throws IOException
                // -- the closed check, the sealed check, and the store itself --
                // and "refused because closed" and "refused because someone
                // sealed the chain underneath me" need different operator
                // responses.
                .hasMessageContaining("released its lease at epoch 1");
    }
}
