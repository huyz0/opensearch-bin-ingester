// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Offsets must survive a failover, and until now they did not.
 *
 * <p>⚠️ {@link Sequencer#commit} promises offsets "will never be reassigned (I2,
 * and NFR-11 across a failover)" and the M4 SPEC makes it acceptance criterion
 * 4. A new leader's {@code recover()} read only its OWN epoch's prefix, so a
 * successor began the stream again at 0 — measured on M4.6c: leader 1 acks 100
 * records at 0..99 and leader 2's first commit returns {@code firstOffset = 0}.
 *
 * <p>⚠️ M4.6a made {@code recover} STOP at a seal but stay permissive, saying at
 * the time that a legitimate cross-boundary reader must be able to read the
 * sealed prefix. This is that reader. The bytes it needs have existed since
 * M4.6c wrote the SEAL and the CONTINUE that pair with it; nothing read them.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitLogCrossEpochTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static void put(MemoryBinStore store, CommitLog log, long seq, byte[] bytes)
            throws IOException {
        store.putIfAbsent(log.keyFor(seq),
                new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
    }

    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    /** A delta touching stream A and, uniquely to epoch 1, stream B. */
    private static byte[] twoStreams(long seq, int records, long firstOffset) {
        return new CommitDelta(seq, "seg/" + seq, List.of(
                new RunCommit(new RunKey(A, 0), records, firstOffset),
                new RunCommit(new RunKey(B, 0), records, 0))).encode();
    }

    private static byte[] delta(long seq, int records, long firstOffset) {
        return new CommitDelta(seq, "seg/" + seq,
                List.of(new RunCommit(new RunKey(A, 0), records, firstOffset))).encode();
    }

    /** epoch 1: CONTINUE, one delta of {@code records}, SEAL continuing at 2. */
    private static void sealedPredecessor(MemoryBinStore store, int records) throws IOException {
        CommitLog one = new CommitLog(store, "bins", 1);
        put(store, one, 0, new Continue(0, 0, 0).encode());
        put(store, one, 1, delta(1, records, 0));
        put(store, one, 2, new Seal(2, 2).encode());
    }

    @Test
    void aSuccESSORResumesTheStreamWhereItsPredecessorStopped() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        sealedPredecessor(store, 6);
        CommitLog two = new CommitLog(store, "bins", 2);
        put(store, two, 0, new Continue(0, 1, 2).encode());

        CommitLog reader = new CommitLog(store, "bins", 2);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("the CONTINUE names epoch 1 at slot 2, so the six records it "
                        + "assigned are part of this stream's history")
                .isEqualTo(6);
        // ⚠️ OFFSETS CROSS; SLOT NUMBERS DO NOT. A first draft folded the
        // predecessor in with `apply`, which advances `nextSequence` -- so this
        // reader's next commit would have gone to slot 3 of its OWN chain,
        // leaving holes at 1 and 2 and destroying the consecutiveness the seal
        // protocol depends on to collide with a fenced leader.
        assertThat(reader.nextSequence())
                .as("this chain holds only its CONTINUE, so its next slot is 1")
                .isEqualTo(1);
    }

    @Test
    void theCrossingStopsAtTheSlotTheCONTINUENamesRatherThanReadingTheWholePredecessor()
            throws Exception {
        // ⚠️ THE PREDECESSOR IS DELIBERATELY UNSEALED. An earlier version of this
        // test put the zombie delta PAST a seal, so `applyChain` returned at the
        // seal before ever reaching it and the assertion was satisfied by
        // M4.6a's barrier -- already pinned in `CommitLogSealTest`. The `upTo`
        // bound itself was unconstrained: deleting it, or making it `>=`, or
        // crossing at `prevSeq + 1`, all passed. A test named for a property it
        // does not check READS as coverage, which is worse than no test.
        // ⚠️ Without a seal, only `upTo` can stop the walk -- so the 99 records
        // at slot 2 are reachable and the assertion discriminates.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 0, 0).encode());
        put(store, chain1, 1, delta(1, 6, 0));
        put(store, chain1, 2, delta(2, 99, 6));

        CommitLog chain2 = new CommitLog(store, "bins", 2);
        put(store, chain2, 0, new Continue(0, 1, 1).encode());
        CommitLog reader = new CommitLog(store, "bins", 2);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("the CONTINUE named slot 1, so slot 2's 99 records are NOT history")
                .isEqualTo(6L);
    }

    @Test
    void aChainSEALEDBeforeItWasEverOpenedDoesNotLoseItsAncestorsHistory()
            throws Exception {
        // ⚠️ REACHABLE THROUGH THIS COMMIT'S OWN ERROR PATH, which is what makes
        // it worth a test rather than a row. A leader that acquires an epoch and
        // fails before writing its CONTINUE -- `start` then releases the lease,
        // by design -- leaves a chain holding a SEAL at slot 0 and no link at
        // all. Measured before the fix: the crossing reached that chain, found
        // no CONTINUE, and silently discarded every offset ever assigned. A
        // successor then reassigned offset 0, which is I2 and acceptance
        // criterion 4 -- the precise defect this whole task exists to close.
        MemoryBinStore store = new MemoryBinStore();
        sealedPredecessor(store, 7);

        // Epoch 2: acquired, sealed by its own successor, never opened.
        CommitLog chain2 = new CommitLog(store, "bins", 2);
        put(store, chain2, 0, new Seal(0, 3).encode());

        CommitLog chain3 = new CommitLog(store, "bins", 3);
        put(store, chain3, 0, new Continue(0, 2, 0).encode());
        CommitLog reader = new CommitLog(store, "bins", 3);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("epoch 2 never opened, so its predecessor's 7 records are still "
                        + "the history -- the link is rebuilt by epoch arithmetic "
                        + "because the CONTINUE that would carry it was never written")
                .isEqualTo(7L);
    }

    @Test
    void anEMPTYChainMidAncestryDoesNotLoseTheHistoryBeyondIt() throws Exception {
        // ⚠️ THE SECOND SHAPE OF A NEVER-OPENED CHAIN, and the first fix handled
        // only the first. `[SEAL@0]` happens when a successor got as far as
        // fencing the dead leader; NOTHING AT ALL happens when the successor
        // died before that too.
        // ⚠️ Reachable, and by the most correlated failure this path meets: two
        // consecutive takeovers each failing at or before their first PUT -- a
        // store outage spanning a failover, which is exactly when several pods
        // try in quick succession. Measured before the fix, the walk broke at
        // the empty chain and offset 0 was reassigned, which is I2.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 0, 0).encode());
        put(store, chain1, 1, delta(1, 100, 0));

        // epoch 2: acquired, then nothing -- not even a seal.
        CommitLog chain3 = new CommitLog(store, "bins", 3);
        put(store, chain3, 0, new Seal(0, 4).encode());
        CommitLog chain4 = new CommitLog(store, "bins", 4);
        put(store, chain4, 0, new Continue(0, 3, 0).encode());

        CommitLog reader = new CommitLog(store, "bins", 4);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("the walk steps over BOTH abandoned chains -- the sealed-at-0 "
                        + "one and the entirely empty one -- to the history beyond")
                .isEqualTo(100L);
    }

    @Test
    void aCONTINUENamingAnEMPTYChainStillReachesTheHistoryBehindIt() throws Exception {
        // ⚠️ ONE OF TWO `atOrigin` TRANSITIONS, isolated. The combined fixture
        // walks CONTINUE -> SEAL@0 -> empty, so it passes through BOTH branches
        // before reaching the empty chain and either assignment alone satisfies
        // it. Deleting the one in the CONTINUE branch then discards everything
        // beyond the empty chain and reassigns offset 0, silently -- I2.
        // ⚠️ M0.67's shape for the fourth time in this milestone: a fixture that
        // cannot express which of two mechanisms did the work.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 0, 0).encode());
        put(store, chain1, 1, delta(1, 100, 0));
        // epoch 2 abandoned entirely -- not even a seal.
        CommitLog chain3 = new CommitLog(store, "bins", 3);
        put(store, chain3, 0, new Continue(0, 2, 0).encode());

        CommitLog reader = new CommitLog(store, "bins", 3);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("a CONTINUE naming an empty chain steps over it to the history")
                .isEqualTo(100L);
    }

    @Test
    void aChainWhoseOWNSlotZeroIsASealStillReachesTheHistoryBehindIt()
            throws Exception {
        // ⚠️ THE OTHER `atOrigin` TRANSITION. Here the ORIGIN is itself a chain
        // sealed before it was opened, so the never-opened branch is what must
        // clear `atOrigin` before the empty chain below it is recognised.
        // Deleting that assignment loses the history the same way.
        // ⚠️ Reachable for any reader recovering a historical epoch, which is
        // what M4.6e exists to make possible.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 0, 0).encode());
        put(store, chain1, 1, delta(1, 100, 0));
        // epoch 2 abandoned entirely; epoch 3 sealed before it was ever opened.
        CommitLog chain3 = new CommitLog(store, "bins", 3);
        put(store, chain3, 0, new Seal(0, 4).encode());

        CommitLog reader = new CommitLog(store, "bins", 3);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("an origin sealed at slot 0 steps over the empty chain below it")
                .isEqualTo(100L);
    }

    @Test
    void aCONTINUENamingAnEpochThatIsNotLOWERIsRefusedRatherThanFollowed()
            throws Exception {
        // ⚠️ THE ONLY TERMINATION ARGUMENT THE CROSSING HAS. Nothing validated
        // that a CONTINUE names a lower epoch, so a two-object CYCLE recursed
        // until the stack went -- a `StackOverflowError` unwinding through a
        // public API on the startup path, which is an Error rather than a
        // failure a caller can act on.
        // ⚠️ The old defence was a depth cap of 1000 justified as "deeper than
        // this is corrupt". That was false -- depth is bounded by the EPOCH
        // NUMBER and every restart burns one, so at 1002 legitimate terms the
        // cluster could never elect a sequencer again. The check belongs on the
        // epoch ORDER, where the claim is actually true.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 2, 9).encode());
        CommitLog chain2 = new CommitLog(store, "bins", 2);
        put(store, chain2, 0, new Continue(0, 1, 9).encode());

        CommitLog reader = new CommitLog(store, "bins", 2);
        assertThatThrownBy(reader::recover)
                .as("a cycle is corrupt bytes and is named as such, not a stack overflow")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("is not LOWER");

        // ⚠️ AND THE EQUALITY HALF, which the 1<->2 cycle above cannot reach.
        // `>= chainEpoch` -> `> chainEpoch` survived everything, and its mutant
        // is WORSE than the one the guard replaced: a chain naming its OWN epoch
        // grows the hop list without bound, so `StackOverflowError` becomes
        // `OutOfMemoryError`. This is M0.67's shape arriving the same day it was
        // written down -- the fixture could not express the equality case.
        MemoryBinStore selfRef = new MemoryBinStore();
        CommitLog itself = new CommitLog(selfRef, "bins", 2);
        put(selfRef, itself, 0, new Continue(0, 2, 9).encode());

        CommitLog reader2 = new CommitLog(selfRef, "bins", 2);
        assertThatThrownBy(reader2::recover)
                .as("a chain continuing from ITSELF is refused, not followed forever")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("is not LOWER");
    }

    @Test
    void aLeaderRESTARTINGOnItsOwnAlreadyOpenedChainDoesNotRewindItsOffsets()
            throws Exception {
        // ⚠️ ONE FIXTURE, TWO UNPINNED MECHANISMS, both found by review and both
        // capable of a false green on their own.
        // ⚠️ FIRST: `open`'s inherited merge used `Math::max`, and replacing it
        // with `put` passed the whole suite -- yet it REWINDS. A log that has
        // already recovered its own chain knows offset 10; the inherited value
        // is 6; an unconditional put drops it back to 6 and the next commit
        // reassigns 6..9. That is I2, and `open`'s own javadoc calls re-running
        // after a partial start legitimate, so it is contract-reachable.
        // ⚠️ SECOND: `applyChain` binds a seal only when `hop.epoch() == ownEpoch`.
        // Dropping that guard also passed everything -- and then a PREDECESSOR's
        // seal fences a healthy new leader permanently, which is the opposite of
        // what the seal means one chain down.
        MemoryBinStore store = new MemoryBinStore();
        sealedPredecessor(store, 6);
        CommitLog chain2 = new CommitLog(store, "bins", 2);
        put(store, chain2, 0, new Continue(0, 1, 2).encode());
        put(store, chain2, 1, delta(1, 4, 6));

        CommitLog restarted = new CommitLog(store, "bins", 2);
        restarted.recover();
        assertThat(restarted.nextOffset(new RunKey(A, 0)))
                .as("recovery already crossed and read its own delta").isEqualTo(10L);

        restarted.open(1, 2);

        assertThat(restarted.nextOffset(new RunKey(A, 0)))
                .as("re-opening merges the INHERITED offsets, which are older -- it "
                        + "must not overwrite what this chain already knows")
                .isEqualTo(10L);
        assertThatCode(() -> restarted.commit("seg/next", java.util.Map.of(new RunKey(A, 0), 1)))
                .as("and the PREDECESSOR's seal does not fence this healthy leader")
                .doesNotThrowAnyException();
    }

    @Test
    void theCrossingIsTRANSITIVEAcrossSeveralTerms() throws Exception {
        // ⚠️ THE SECOND STREAM IS THE WHOLE TEST. An earlier version asserted a
        // stream that EVERY term wrote to -- and because a delta encodes its own
        // firstOffset, epoch 2's bytes already carried epoch 1's contribution, so
        // "follow one CONTINUE" and "follow every CONTINUE" were
        // indistinguishable. The test named for transitivity did not test it.
        // ⚠️ `B` is written ONLY by epoch 1. Reaching it requires actually
        // walking past epoch 2, so a one-hop ancestry fails here and nowhere
        // else. Same fixture defect as the `upTo` test: a value derivable from
        // context, so ignoring the mechanism survived.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog chain1 = new CommitLog(store, "bins", 1);
        put(store, chain1, 0, new Continue(0, 0, 0).encode());
        put(store, chain1, 1, delta(1, 6, 0));
        put(store, chain1, 2, twoStreams(2, 4, 6));
        put(store, chain1, 3, new Seal(3, 2).encode());

        CommitLog chain2 = new CommitLog(store, "bins", 2);
        put(store, chain2, 0, new Continue(0, 1, 3).encode());
        put(store, chain2, 1, delta(1, 4, 10));
        put(store, chain2, 2, new Seal(2, 3).encode());

        CommitLog chain3 = new CommitLog(store, "bins", 3);
        put(store, chain3, 0, new Continue(0, 2, 2).encode());
        CommitLog reader = new CommitLog(store, "bins", 3);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("6 from epoch 1 plus 4 from each of epochs 1 and 2, all reached "
                        + "through CONTINUEs")
                .isEqualTo(14L);
        assertThat(reader.nextOffset(new RunKey(B, 0)))
                .as("stream B exists ONLY in epoch 1 -- reaching it proves the walk "
                        + "went past the immediate predecessor")
                .isEqualTo(4L);
    }

    @Test
    void aFirstChainNamingNoPredecessorCrossesNothing() throws Exception {
        // ⚠️ THE NEGATIVE CONTROL, and the termination argument. `prevEpoch = 0`
        // is the RESERVED unleased chain, which a first leader names truthfully
        // to say there was no predecessor. A crossing that treated 0 as an epoch
        // to visit would read the unleased chain into a leased one's history --
        // the fork M4.6d exists to remove -- or recurse without a base case.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog one = new CommitLog(store, "bins", 1);
        put(store, one, 0, new Continue(0, 0, 0).encode());
        put(store, one, 1, delta(1, 3, 0));
        CommitLog zero = new CommitLog(store, "bins", 0);
        put(store, zero, 0, delta(0, 50, 0));      // unleased chain, must be invisible

        CommitLog reader = new CommitLog(store, "bins", 1);
        reader.recover();

        assertThat(reader.nextOffset(new RunKey(A, 0)))
                .as("epoch 0 is reserved, not a predecessor to follow")
                .isEqualTo(3);
    }
}
