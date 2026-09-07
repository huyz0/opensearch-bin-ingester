// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import static binjava.sequencer.InvariantFixtures.delta;
import static binjava.sequencer.InvariantFixtures.put;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import binjava.sequencer.Invariants.Violation;
import binjava.sequencer.ReaderInvariants.ReaderView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * I3 and I4, the two invariants {@link Invariants}' own list records as NOT-RUN
 * because neither is a property of the stored bytes (M4.13a).
 *
 * <p><b>I3</b> — a reader applies only deltas in a sealed prefix or a later
 * epoch's chain, never in a discarded suffix. M4's SPEC states it as criterion
 * 3(a): "asserted on the reader, not merely that its call failed".
 * {@code checkChain} already reports an entry PAST a seal, which is the
 * WRITER's side of the same fact — that such bytes exist. Whether a reader
 * APPLIED them is a different question and needs the reader's answer.
 *
 * <p><b>I4</b> — committed records are never dropped or reordered. ⚠️ ONLY THE
 * DROP CLAUSE IS ASSERTED HERE, and this file says so rather than letting a
 * reader conclude M4's completion condition is met: a reader's next-offset map
 * is a high-water mark, so runs folded in the wrong ORDER end on the same
 * number. The reorder clause was settled by M4.13f: it has no arm of its own, because at the granularity the chain carries it follows from I2 plus append-onlyness, and a permutation within one delta is the clause's permitted half.
 *
 * <p>⚠️ I3 TAKES THE READER'S VIEW AS AN INPUT, and that is the whole design.
 * A checker that recomputed the view itself could not DISAGREE with production
 * — the defect M4.19's round-1 review found and the reason
 * `crossEpochOffsetsChecked()` was deleted. Here the EXPECTED side is derived
 * independently from the bytes (fold everything before the first seal) and the
 * ACTUAL side is whatever the reader says it got, so the two can differ and the
 * test can supply a broken reader.
 */
class ReaderInvariantsTest {

    private static final RunKey RA = new RunKey(InvariantFixtures.A, 0);
    private static final String PREFIX = InvariantFixtures.PREFIX;

    /**
     * A chain of CONTINUE, one delta, a SEAL, and a delta BEYOND the seal.
     *
     * <p>⚠️ BUILT BY RAW PUT, not by driving a second {@code CommitLog}, and
     * the difference was measured rather than assumed. A second log that
     * recovers and commits produces NO entry past the seal -- the write is
     * refused -- so a fixture built that way leaves the seal as the last entry,
     * and every case below still reads plausibly while asserting nothing about
     * a discarded suffix. `InvariantsTest#anEntryBEYONDASealIsReportedAsI5`
     * builds the same shape the same way.
     */
    private static MemoryBinStore chainWithZombieBeyondTheSeal(int applied, int zombie)
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, applied, 0));
        put(store, log, 2, new Seal(2, 2).encode());
        put(store, log, 3, delta(3, zombie, applied));

        // ⚠️ THE FIXTURE MUST ACTUALLY CONTAIN THE ZOMBIE. Without this the
        // suite passes against a chain whose last entry is the seal, and
        // `theREALReaderAppliesNothingBeyondTheSeal` asserts that a reader stops
        // at a barrier nothing lies past. Measured: the first version of this
        // helper, which drove a second CommitLog, did exactly that.
        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("bytes must lie BEYOND the seal for I3 to be about anything")
                .contains("I5");
        return store;
    }

    /**
     * ⚠️ THE BASELINE CASE, and it is the one every other case here is measured
     * against: a reader that applied exactly the sealed prefix violates nothing.
     * It briefly also asserted a {@code judged()} flag, which M4.13i removed --
     * once every chain is judged, that flag could not go false, and this file's
     * neighbours record what an always-true claim asserted in a test of its own
     * costs.
     */
    @Test
    void aReaderThatSTOPSAtTheSealViolatesNothing() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);

        // The honest reader: it applied the pre-seal delta only.
        List<Violation> verdict = ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 3L)));

        assertThat(verdict).isEmpty();
    }

    /**
     * ⚠️ THE CASE I3 EXISTS FOR, and it cannot be produced by driving
     * production — `ChainReplay` stops at the seal, correctly. So the BROKEN
     * reader is supplied here, which is what makes this an assertion about
     * readers rather than a restatement of what production happens to do.
     */
    @Test
    void aReaderThatAPPLIEDTheDiscardedSuffixVIOLATESI3() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);

        // A reader that folded the zombie delta too: 3 + 5.
        List<Violation> found =
                ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 8L)));

        assertThat(found).as("applying a discarded suffix is I3").hasSize(1);
        assertThat(found.get(0).invariant()).isEqualTo("I3");
        assertThat(found.get(0).detail())
                .as("and it must name the stream and both offsets")
                .contains("3").contains("8");
    }

    /**
     * ⚠️ A READER THAT APPLIED **LESS** THAN THE SEALED PREFIX IS I4, NOT I3.
     * Dropping a committed record is the other invariant, and conflating them
     * would report the wrong one to whoever pins the seed.
     */
    @Test
    void aReaderThatDROPPEDACommittedRecordVIOLATESI4() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);

        List<Violation> found =
                ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 1L)));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).invariant()).as("dropped, not over-applied").isEqualTo("I4");
    }

    /**
     * ⚠️ A STREAM THE READER NEVER MENTIONS IS A DROP, not an absence. A reader
     * returning an empty view for a chain that committed records has dropped
     * every one of them, and reporting nothing would make the emptiest possible
     * failure the quietest.
     */
    @Test
    void aReaderThatSAWNoStreamsAtAllVIOLATESI4() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);

        List<Violation> found = ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of()));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).invariant()).isEqualTo("I4");
    }

    /**
     * ⚠️ AND THE PRODUCTION READER MUST PASS ITS OWN CHECK. The three cases
     * above supply hand-made views; this one drives the real recovery path, so
     * a reader that started applying discarded suffixes would fail here without
     * anyone writing a new fixture.
     */
    @Test
    void theREALReaderAppliesNothingBeyondTheSeal() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);

        CommitLog reader = new CommitLog(store, PREFIX, 1);
        reader.recover();

        List<Violation> verdict = ReaderInvariants.checkReader(store, PREFIX, ReaderView.of(reader));
        assertThat(verdict)
                .as("production's own reader, judged by the same predicate")
                .isEmpty();
    }

    /**
     * ⚠️ A STREAM THE CHAIN NEVER COMMITTED IS I3, and the arm that catches it
     * cannot be reached by the cases above: they iterate the streams the chain
     * DID commit, so a reader inventing one is invisible to them. Measured --
     * deleting that arm left the other five green.
     */
    @Test
    void aReaderReportingAStreamTheChainNEVERCommittedVIOLATESI3() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);
        RunKey invented = new RunKey(InvariantFixtures.A, 7);

        List<Violation> found = ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 3L, invented, 4L)));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).invariant()).isEqualTo("I3");
        assertThat(found.get(0).detail()).contains("never committed");
    }

    /**
     * ⚠️ THE EXPECTED FOLD STARTS FROM WHAT THE CHAIN INHERITED, not from zero,
     * and no single-chain fixture can tell the difference. Deltas carry
     * ABSOLUTE first offsets, so a chain that re-commits every stream folds to
     * the same numbers with or without its inherited base. The difference only
     * shows for a stream the predecessor committed and this chain did NOT touch
     * -- here {@code RA}, committed in epoch 1 and absent from epoch 2. A
     * reader legitimately still carrying it would be reported as having
     * invented a stream. Measured: replacing the inherited base with an empty
     * map left the other five green.
     */
    @Test
    void aReaderCARRYINGAnINHERITEDStreamItsOwnChainNeverTouchedIsCLEAN() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        RunKey other = new RunKey(InvariantFixtures.A, 1);
        CommitLog second = new CommitLog(store, PREFIX, 2);
        put(store, second, 0, new Continue(0, 1, 2).encode());
        put(store, second, 1, new CommitDelta(1, "seg/other",
                List.of(new RunCommit(other, 2, 0))).encode());

        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(2, Map.of(RA, 3L, other, 2L))))
                .as("RA is inherited from the sealed epoch 1, not invented by the reader")
                .isEmpty();
    }

    /**
     * ⚠️ THE FOLD KEEPS THE HIGHER END, and no fixture above can tell: each
     * commits a given stream exactly ONCE, so a fold that kept the LOWER of two
     * ends is indistinguishable from one that kept the higher. Measured --
     * turning the fold's {@code Math::max} into {@code Math::min} left the
     * other seven green, and it is not a cosmetic mutation: the sealed prefix
     * would then end at the FIRST delta, and a reader correctly carrying both
     * would be reported as having applied a discarded suffix.
     */
    @Test
    void aChainCommittingOneStreamTWICEFoldsToTheHIGHEREnd() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        put(store, log, 2, delta(2, 2, 3));
        put(store, log, 3, new Seal(3, 2).encode());

        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 5L))))
                .as("3 records then 2 more is a sealed prefix ending at 5, not at 3")
                .isEmpty();
    }

    /**
     * ⚠️ A NEXT-OFFSET OF ZERO IS "APPLIED NOTHING", NOT "INVENTED A STREAM".
     * A reader may carry an initialised, empty entry for a stream, and
     * reporting I3 there would be a false positive on the commonest possible
     * reader state. Measured -- relaxing the arm's {@code > 0} guard to
     * {@code >= 0} left the other seven green.
     */
    @Test
    void aReaderReportingAStreamAtOffsetZEROHasAppliedNothingAndIsCLEAN() throws Exception {
        MemoryBinStore store = chainWithZombieBeyondTheSeal(3, 5);
        RunKey untouched = new RunKey(InvariantFixtures.A, 7);

        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 3L, untouched, 0L))))
                .as("an empty entry is not an applied record")
                .isEmpty();
    }

    /**
     * ⚠️ TWO COMMITTED STREAMS, ONE BROKEN EACH WAY, and the reason it exists
     * is that every other case here commits exactly ONE stream. Measured: with
     * only those, the expected-side loop can be cut to its FIRST iteration and
     * all nine remain green -- an invariant checker returning "no violations"
     * for a reader that over-applied the discarded suffix of the second stream,
     * which is worse than having no checker.
     *
     * <p>⚠️ ORDER-INDEPENDENT BY CONSTRUCTION. {@code expected} is a HashMap, so
     * which stream a first-iteration-only loop would reach is not defined; both
     * streams are broken, so the count discriminates whichever it is.
     *
     * <p>⚠️ AND IT ASSERTS THE MESSAGES, not just the labels. Both offsets
     * appear in an I3 detail, so a bare {@code contains("3")} and
     * {@code contains("8")} holds just as well for a message that transposes
     * them -- and a pinned seed reported with the reader's offset and the
     * sealed-prefix end the wrong way round sends whoever debugs it backwards.
     */
    @Test
    void TWOStreamsBrokenInOPPOSITEDirectionsAreBOTHReported() throws Exception {
        RunKey other = new RunKey(InvariantFixtures.A, 3);
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, new CommitDelta(1, "seg/both",
                List.of(new RunCommit(RA, 3, 0), new RunCommit(other, 4, 0))).encode());
        put(store, log, 2, new Seal(2, 2).encode());
        put(store, log, 3, delta(3, 5, 3));

        // RA over-applied to 8, `other` dropped back to 1.
        List<Violation> found =
                ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 8L, other, 1L)));

        assertThat(found).hasSize(2);
        assertThat(found).anySatisfy(v -> {
            assertThat(v.invariant()).isEqualTo("I3");
            assertThat(v.detail()).contains(RA.toString())
                    .contains("up to 8").contains("ends at 3");
        });
        assertThat(found).anySatisfy(v -> {
            assertThat(v.invariant()).isEqualTo("I4");
            assertThat(v.detail()).contains(other.toString())
                    .contains("up to 1").contains("committed up to 4");
        });
    }

    /**
     * ⚠️ A SUCCESSOR'S READER CARRIES ITS ANCESTORS' RECORDS, and judging it at
     * ITS OWN epoch must say so. This is the "or a later epoch's chain" half of
     * I3, and the fixture is deliberately CLEAN -- the risk here is a false
     * positive, not a missed violation. MEASURED before this test existed: a
     * probe over a clean four-epoch history with production's own reader
     * produced spurious I3s on three of the four epochs.
     */
    @Test
    void aSUCCESSORSReaderCarryingItsANCESTORSRecordsIsCLEAN() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog second = new CommitLog(store, PREFIX, 2);
        put(store, second, 0, new Continue(0, 1, 2).encode());
        put(store, second, 1, delta(1, 4, 3));

        CommitLog reader = new CommitLog(store, PREFIX, 2);
        reader.recover();

        assertThat(reader.offsets()).as("the reader carries all 7 records").containsEntry(RA, 7L);
        assertThat(ReaderInvariants.checkReader(store, PREFIX, ReaderView.of(reader)))
                .as("3 inherited plus 4 of its own is the whole history, not a discarded suffix")
                .isEmpty();
    }

    /**
     * ⚠️ THE FOLD KEEPS THE HIGHER END, NOT THE LAST ONE WRITTEN, and a
     * monotonic fixture cannot tell those apart. Measured: with only the 3-then-5
     * chain above, replacing the fold's {@code merge(..., Math::max)} with a
     * plain {@code put} left every test green. A third delta ending BELOW the
     * mark separates them -- and the bytes are exactly what {@code checkChain}
     * reports as I2, so a fold that took the last write would stack a spurious
     * I3 on top of a real I2 and the pinned seed would name the wrong invariant.
     */
    @Test
    void aDeltaEndingBELOWTheMarkDoesNotLowerTheFold() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        put(store, log, 2, delta(2, 2, 3));
        put(store, log, 3, delta(3, 1, 0));
        put(store, log, 4, new Seal(4, 2).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("those bytes ARE an I2 -- checkReader must not add a second, wrong name")
                .extracting(Violation::invariant).contains("I2");
        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 5L)))
                )
                .as("the sealed prefix still ends at 5")
                .isEmpty();
    }

    /**
     * ⚠️ `checkReader` RETURNS I3 AND I4 AND NOTHING ELSE. It folds through
     * {@code inheritedOffsets}, which raises violations of its own, and those
     * are DISCARDED because `checkChain` raises every one of them over the same
     * chain -- a caller running both would otherwise see each twice, and a
     * report of two violations would be ambiguous between two defects and one
     * counted double. Measured: letting them through left all twelve other
     * tests green.
     */
    @Test
    void aBrokenPREDECESSORLinkIsCheckChainsToReportNotThisOnes() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        // ⚠️ NO SEAL: epoch 2 continues from a chain that was never sealed for it.
        CommitLog second = new CommitLog(store, PREFIX, 2);
        put(store, second, 0, new Continue(0, 1, 2).encode());
        put(store, second, 1, delta(1, 4, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .as("the broken link is checkChain's finding")
                .extracting(Violation::invariant).contains("link");
        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(2, Map.of(RA, 7L)))
                )
                .as("and checkReader must not report it a second time")
                .isEmpty();
    }

    /**
     * ⚠️ A VIEW TAKEN BEFORE A COMMIT THAT HAS SINCE LANDED IS REPORTED AS I4,
     * and pinning it is the point: this is a real limit of the contract, not a
     * defect to be papered over, and the only defence is the caller taking the
     * view after the workload quiesces. Nothing was dropped in this fixture --
     * the chain is clean and the reader was correct when it was asked. Left
     * undocumented and unpinned, whoever wires M4.13's sweep hands it the live
     * writer's log and every seed with a commit in flight fails as I4.
     */
    @Test
    void aSTALEViewIsReportedAsI4AndTheCONTRACTSaysSo() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));

        CommitLog reader = new CommitLog(store, PREFIX, 1);
        reader.recover();
        ReaderView takenEarly = ReaderView.of(reader);

        put(store, log, 2, delta(2, 4, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("the chain is clean -- the staleness is entirely the caller's")
                .isEmpty();
        assertThat(ReaderInvariants.checkReader(store, PREFIX, takenEarly))
                .extracting(Violation::invariant).containsExactly("I4");
    }
}
