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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ADR-0029: a successor seals the ancestor it inherits offsets from, not
 * blindly {@code epoch - 1}.
 *
 * <p>Reproduces the shape the commit-protocol simulation (M4.20) found and
 * ADR-0029 diagnosed: epoch 1 genuinely opened and committing, one or more
 * epochs burned right after it (minted but never opened -- a failed {@code
 * start} on the takeover path), and a successor several epochs later. Before
 * this fix {@code LocalSequencer.start} sealed only {@code epoch - 1} -- one
 * of the BURNED epochs, which has nothing to seal -- so epoch 1's leader,
 * unaware it had been superseded, kept committing into offsets the successor
 * was about to reassign. I2.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerAncestorSealTest {

    private static final UUID STREAM = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    private static final String PREFIX = "bins/cluster-a";

    private static final class FixedClock extends Clock {
        @Override public long millis() {
            return 1_000_000L;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId z) {
            return this;
        }
    }

    static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW),
                new FixedClock());
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(STREAM, 0), n);
        return m;
    }

    /**
     * Burns {@code count} lease epochs without ever opening a chain for them --
     * the shape a failed {@code start} leaves: the epoch is minted (the lease's
     * CAS write lands) but nothing is ever written under its commit-log prefix,
     * because the failure is before {@code CommitLog.open} runs. Mirrors
     * {@code tryAcquire} minting the epoch before {@code start} does its work,
     * which ADR-0029's Context section names as the reason burns happen in
     * runs.
     */
    static void burnEpochs(BinStore store, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            // ⚠️ ONE instance for both calls. `release()` is a documented no-op
            // when its own `belief` is null (never having itself acquired), so
            // two separate `manager(...)` instances -- one to acquire, another
            // to release -- would silently fail to free the lease at all.
            LeaseManager churner = manager(store, "churner" + i);
            churner.tryAcquire().orElseThrow();
            // ⚠️ NOT released via LocalSequencer.close(): a real burn is a start
            // that failed AFTER acquiring, which releases the LEASE but writes
            // NOTHING to the commit log -- exactly what leaves the prefix empty.
            churner.release();
        }
    }

    static ChainEntry slotZero(BinStore store, long epoch) throws IOException {
        String key = new CommitLog(store, PREFIX, epoch).keyFor(0);
        try (InputStream in = store.get(key)) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }

    @Test
    void aTakeoverSealsTheAncestorItINHERITSFromNotBlindlyEpochMinusOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();

        // Epoch 1: genuinely opened, commits 0..2 for STREAM, never sealed --
        // the leader whose lease is about to lapse without it knowing.
        CommitLog zombie = new CommitLog(store, PREFIX, 1);
        zombie.open(0, 0);
        zombie.commit("seg/a", counts(3));

        // Epochs 2 and 3: minted and abandoned, nothing ever written under
        // them -- two failed takeovers in a row. ⚠️ THREE burns, not two:
        // epoch 1's lease itself was never acquired above (the zombie's chain
        // was written directly through `CommitLog`, bypassing `LeaseManager`
        // entirely, which is fine -- `CommitLog`'s own fencing is independent
        // of lease state), so the lease counter starts fresh at 0 and the
        // first burn would otherwise consume epoch 1 rather than 2.
        burnEpochs(store, 3);
        // Epoch 3 -- the successor's IMMEDIATE predecessor -- gets a `Seal` at
        // slot 0 rather than staying wholly untouched: `ChainReplay.neverOpened`
        // javadoc's own two legitimate burned-chain shapes are "a Seal at slot
        // 0 (a successor got as far as fencing it)" or "nothing at all (that
        // successor died before even that)". ⚠️ ONLY THE FIRST SHAPE IS THIS
        // ROW'S TO TEST: a wholly-empty chain AT THE ORIGIN hits `neverOpened`'s
        // own disclosed gap (M4.23, a separate row) where empty-at-origin is
        // currently (mis)treated as "normal" rather than walked past -- fixing
        // that here would be scope this task does not own. Epoch 2, NOT the
        // origin, stays wholly empty on purpose: `neverOpened` already walks a
        // non-origin empty chain correctly, and that is real coverage the Seal
        // shape alone would not exercise.
        new CommitLog(store, PREFIX, 3).seal(4, 1);

        // The successor wins epoch 4.
        LocalSequencer successor = LocalSequencer.start(store, PREFIX,
                manager(store, "podB"), 8).orElseThrow();
        assertThat(successor.epoch()).isEqualTo(4);

        // ⚠️ THE ASSERTION THAT MATTERS: epoch 1's zombie learns it is fenced.
        // Before this fix nothing ever sealed epoch 1 -- this commit would have
        // WON, reassigning offset 3 a second time. That is the I2 violation
        // ADR-0029 exists to close.
        assertThatThrownBy(() -> zombie.commit("seg/zombie", counts(1)))
                .as("epoch 1 was sealed as part of the takeover, so the leader "
                        + "that never learned it was superseded loses its next "
                        + "write instead of reassigning an offset already handed "
                        + "out by the successor")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("fenced");

        // ⚠️ THE BURNED EPOCHS ARE NOW SEALED TOO, and this assertion is the
        // REVERSE of what it said under ADR-0029. That record left them
        // untouched as wasted work on the path NFR-9 bounds; ADR-0037 amends it,
        // because a burned epoch is empty only because its leader holds the
        // lease and has not written YET, so leaving it unsealed lets it open and
        // write beside this chain -- the same I2 by another route. The cost that
        // reverses the judgement is measured: the crossed RUN is mean 1.56 and
        // max 7, not the ~70 per takeover ADR-0029 costed.
        assertThat(slotZero(store, 2))
                .as("epoch 2 was crossed by the walk, so its leader is fenced")
                .isEqualTo(new Seal(0, 4));
        assertThat(slotZero(store, 3))
                .as("epoch 3 already carried its own Seal from the fixture setup, and "
                        + "sealing an already-sealed chain is a no-op rather than a "
                        + "second write")
                .isEqualTo(new Seal(0, 4));

        // The new chain's CONTINUE names the real ancestor directly, not the
        // burned epoch immediately below it -- ADR-0029's Consequences section.
        ChainEntry opening = slotZero(store, 4);
        assertThat(opening).isInstanceOf(Continue.class);
        assertThat(((Continue) opening).prevEpoch())
                .as("the forward link points at epoch 1, where the offsets "
                        + "actually are, skipping the two burned epochs")
                .isEqualTo(1);

        // And offsets carried across correctly: the successor resumes the
        // stream at 3, it does not restart it and does not reassign it.
        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(2)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("resumes where epoch 1 left off (0..2 assigned), not at 0")
                .isEqualTo(3);
    }

    @Test
    void aRedrivenSealsExtraDeltaReachesTheSuccessorProvingOrderIsLoadBearing()
            throws Exception {
        // ⚠️ WHY THIS TEST EXISTS SEPARATELY FROM THE ONE ABOVE. That test's
        // fixture is single-threaded and fully written before `start` ever
        // runs, so seal-then-read and read-then-seal produce the SAME observed
        // result: nothing changes between the two steps, whichever runs first.
        // ADR-0029's property 3 -- "order is load-bearing" -- is a claim about
        // a WRITE landing IN BETWEEN those two steps, which needs the ancestor's
        // seal to be CONTESTED, not merely present. `StealFirstPutStore`
        // (already used by `LocalSequencerTest` for the plain `epoch - 1` case)
        // makes exactly that happen: it injects a zombie's in-flight delta into
        // the seal's first slot, forcing a redrive.
        //
        // ⚠️ THE MUTATION THIS CATCHES that the other test cannot: swap
        // `LocalSequencer.start`'s seal-then-`log.open` into `log.open`-then-seal.
        // Under the swap, `open`'s crossing runs BEFORE the redrive has happened,
        // so it reads the ancestor only up to the PRE-redrive boundary and never
        // sees the delta the redrive folded in -- exactly the reopened defect
        // ADR-0029's Decision section names.
        MemoryBinStore backing = new MemoryBinStore();

        // Epoch 1: opened, commits 0..2 for STREAM. Its `nextSequence` is 2 --
        // slot 2 is the first one its OWN next commit, or a successor's seal,
        // will try to claim.
        CommitLog chain1 = new CommitLog(backing, PREFIX, 1);
        chain1.open(0, 0);
        chain1.commit("seg/a", counts(3));

        // The fenced leader's in-flight delta -- offsets 3..4, continuing the
        // stream where its last SEEN commit left off -- lands in the slot the
        // successor's seal is about to claim, exactly as
        // `aTakeoverRedrivesPastTheFencedLeadersInFlightCommitRatherThanGivingUp`
        // does for the plain `epoch - 1` case in `LocalSequencerTest`.
        byte[] inFlight = new CommitDelta(2, "seg/in-flight",
                List.of(new RunCommit(new RunKey(STREAM, 0), 2, 3))).encode();
        BinStore contended = new StealFirstPutStore(backing, chain1.keyFor(2), inFlight);

        // ⚠️ ONE BURN, matching `aTakeoverSealsTheAncestorItINHERITSFromNot...`'s
        // own note: epoch 1's lease was never itself acquired above (`chain1`
        // was written directly, bypassing `LeaseManager`), so the lease counter
        // starts fresh at 0 and the successor would otherwise win epoch 1 too.
        burnEpochs(backing, 1);

        // The successor wins epoch 2 -- the IMMEDIATE predecessor is epoch 1,
        // so `firstInheritableAncestor` finds it on its first check and no
        // ancestor-walk is exercised here; this test isolates the ORDERING
        // claim from the WALK-BACK claim the test above covers.
        LocalSequencer successor = LocalSequencer.start(contended, PREFIX,
                manager(contended, "podB"), 8).orElseThrow();
        assertThat(successor.epoch()).isEqualTo(2);

        assertThat(entryAt(backing, 1, 3))
                .as("the seal redrove past the in-flight delta to the next free slot")
                .isEqualTo(new Seal(3, 2));

        // ⚠️ THE ASSERTION THAT MATTERS. `firstOffset` here is 5 only if the
        // CROSSING that populates the successor's offsets ran AFTER the redrive
        // -- i.e. only if seal genuinely happened before read. Read-before-seal
        // would cross using the PRE-redrive boundary (up to slot 1, offsets
        // 0..2 only) and this would read 3 instead, COLLIDING with the in-flight
        // delta's own 3..4 -- the exact I2 violation this ADR closes, reopened
        // by one swapped pair of statements.
        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(1)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("resumes AFTER the redriven delta's 3..4, not before it")
                .isEqualTo(5);
    }

    private static ChainEntry entryAt(BinStore store, long epoch, long sequence)
            throws IOException {
        String key = new CommitLog(store, PREFIX, epoch).keyFor(sequence);
        try (InputStream in = store.get(key)) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }

    @Test
    void aWalkWithNoGenuineAncestorAnywhereStopsAtZeroRatherThanSealingAnUnopenedEpoch()
            throws Exception {
        // ⚠️ THE `chainEpoch >= 1` BOUNDARY, isolated. Every fixture above
        // happens to land the real ancestor at epoch 1 -- reachable whether the
        // walk's guard is `>= 1` or the OFF-BY-ONE `> 1`, since both admit
        // epoch 1 as a candidate to CHECK. Only a walk with NO genuine ancestor
        // anywhere back to (and including) epoch 1 distinguishes them: `> 1`
        // would stop one epoch early, at chainEpoch == 1, without ever
        // inspecting it, and wrongly report epoch 1 -- itself never opened --
        // as inheritable.
        MemoryBinStore store = new MemoryBinStore();
        // Epochs 1-4: minted and abandoned, nothing ever written under any of
        // them.
        burnEpochs(store, 4);
        // Epoch 4 -- the successor's immediate predecessor -- gets the Seal
        // shape rather than staying wholly empty, for the same atOrigin reason
        // as the fixture above: an empty chain AT THE ORIGIN is `neverOpened`'s
        // own disclosed (M4.23) gap, not this walk's to close.
        new CommitLog(store, PREFIX, 4).seal(5, 1);

        LocalSequencer successor = LocalSequencer.start(store, PREFIX,
                manager(store, "podB"), 8).orElseThrow();
        assertThat(successor.epoch()).isEqualTo(5);

        ChainEntry opening = slotZero(store, 5);
        assertThat(opening).isInstanceOf(Continue.class);
        assertThat(((Continue) opening).prevEpoch())
                .as("no epoch back to 1 was ever genuinely opened, so the walk "
                        + "must stop at 0 -- epoch 0 is the RESERVED unleased "
                        + "chain (M4.4b), which truthfully says 'no predecessor' "
                        + "rather than naming epoch 1, which never had a leader "
                        + "to fence either")
                .isZero();

        // ⚠️ THE WALK NOW SEALS WHAT IT CROSSES, including epoch 1 (ADR-0037).
        // Under ADR-0029 this asserted the opposite -- that nothing was sealed
        // -- on the reasoning that an unopened epoch has "no leader to fence
        // either". That reasoning is what M4.47 falsified: an unopened epoch's
        // leader holds the lease and may write at any moment, and with no
        // genuine ancestor anywhere the successor inherits 0, so that write
        // lands on offsets this chain is about to reassign.
        // ⚠️ THIS IS THE UNBOUNDED CASE, and it is bounded only by cluster
        // birth: the run is every epoch minted before the first successful open.
        for (long e = 1; e <= 4; e++) {
            assertThat(slotZero(store, e))
                    .as("epoch " + e + " was crossed by the walk, so its leader is fenced "
                            + "-- it held the lease and had merely not written yet")
                    .isEqualTo(new Seal(0, 5));
        }
    }

    /**
     * ADR-0037: the same takeover when the IMMEDIATE PREDECESSOR IS WHOLLY
     * EMPTY, which is the case the test above deliberately does not build.
     *
     * <p>⚠️ THAT EXCLUSION IS WHY THIS EXISTS. The sibling gives epoch 3 a
     * {@code Seal} at slot 0 and says why in as many words -- a wholly-empty
     * chain at the origin hits {@code ChainReplay.neverOpened}'s disclosed gap,
     * "scope this task does not own". M4.13's sweep then measured that gap
     * producing a real I2 in production: {@code firstInheritableAncestor} stops
     * at the burned epoch, seals THAT, and leaves the genuine ancestor unsealed
     * and still growing, so a late in-flight write extends what successors
     * inherit AFTER they have committed at lower offsets.
     *
     * <p>⚠️ THE FIXTURE DIFFERS FROM THE SIBLING BY ONE LINE -- epoch 3 keeps
     * nothing at slot 0 -- and that is the whole point: the defect is invisible
     * to every shape where the walk's first probe finds bytes.
     */
    @Test
    void aTakeoverWhoseIMMEDIATEPredecessorIsWHOLLYEMPTYStillSealsTheRealAncestor()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();

        CommitLog zombie = new CommitLog(store, PREFIX, 1);
        zombie.open(0, 0);
        zombie.commit("seg/a", counts(3));

        // Epochs 2 AND 3 burned and wholly empty -- epoch 3 is the successor's
        // immediate predecessor and the walk's origin.
        burnEpochs(store, 3);

        LocalSequencer successor = LocalSequencer.start(store, PREFIX,
                manager(store, "podB"), 8).orElseThrow();
        assertThat(successor.epoch()).isEqualTo(4);

        assertThatThrownBy(() -> zombie.commit("seg/zombie", counts(1)))
                .as("epoch 1 is the genuine ancestor and must be sealed even though "
                        + "the walk began at an EMPTY chain")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("fenced");

        // ⚠️ THE BURNED RUN IS SEALED, which the sibling asserts the opposite of
        // for its own shape. A burned epoch is empty because its leader holds
        // the lease and has not written YET, not because it is dead -- stepping
        // over it without sealing leaves it free to open and write beside the
        // successor, which is ADR-0037's rejected alternative (c).
        assertThat(slotZero(store, 3))
                .as("the walk crossed epoch 3, so it must fence epoch 3's own leader")
                .isEqualTo(new Seal(0, 4));
        assertThat(slotZero(store, 2))
                .as("and epoch 2, likewise crossed")
                .isEqualTo(new Seal(0, 4));

        ChainEntry opening = slotZero(store, 4);
        assertThat(opening).isInstanceOf(Continue.class);
        assertThat(((Continue) opening).prevEpoch())
                .as("the forward link names epoch 1, where the offsets are")
                .isEqualTo(1);

        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(2)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("resumes at 3, it does not restart the stream at 0")
                .isEqualTo(3);
    }
}
