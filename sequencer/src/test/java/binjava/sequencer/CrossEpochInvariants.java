// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import binjava.sequencer.Invariants.Violation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What holds ACROSS the join between two chains: the link a CONTINUE claims,
 * and the offsets a successor inherits through it.
 *
 * <p>⚠️ A SEPARATE FILE ALONG A SEAM THE TESTS ALREADY USE. M4.12 split
 * {@code InvariantsAcrossChainsTest} from {@code InvariantsTest} on exactly this
 * line -- one asks what holds WITHIN a chain, the other what holds across the
 * join -- and {@link Invariants} had grown past code-structure.md's 500-line cap
 * with both halves in it. The within-chain walk stays there; everything that
 * reads a PREDECESSOR lives here.
 *
 * <p>⚠️ {@code readChain} AND {@code neverOpened} STAY IN {@link Invariants},
 * because {@code checkChain} needs them too. Two copies of "what is a chain
 * entry" or "what is a burned epoch" is how a checker stops agreeing with its
 * sibling, which this file's own history records twice.
 */
final class CrossEpochInvariants {

    private CrossEpochInvariants() {
    }

    /**
     * The offsets this chain INHERITS, VERIFIED against the predecessor's own
     * SEAL rather than taken on the successor's word.
     *
     * <p>⚠️ THE INPUT IS WHERE THE DEFECT LIVES, and the first version of this
     * method got that exactly wrong. It read {@code prevEpoch} and
     * {@code prevSeq} straight off the successor's CONTINUE -- which is
     * byte-for-byte the pair {@code LocalSequencer.start} hands to
     * {@code open -> crossFrom -> ChainReplay.inherited} to compute the offsets
     * it then commits at. Re-deriving the MECHANISM while copying the INPUT is
     * not independence: production and the checker made the same mistake and
     * agreed. MEASURED by round-1 review: mutating production to
     * {@code open(prevEpoch, 0)} -- every successor restarting every stream at 0
     * after its predecessor had acknowledged records, so I2, NFR-11 and
     * acceptance criterion 4 broken at every failover -- left {@code checkChain}
     * returning EMPTY for both chains and the clean-run simulation GREEN across
     * 18 takeovers.
     *
     * <p>⚠️ So the link is now CHECKED, from the other end. The protocol's rule
     * is that a chain may only continue from one that was sealed for it, so
     * this reads the predecessor's SEAL and reports a violation when there is
     * none, when {@code continuedAt} does not name this chain, or when
     * {@code prevSeq} disagrees with where the seal actually sits. The rule is
     * restated from the protocol, not delegated to the code that implements it.
     */
    static Map<RunKey, Long> inheritedOffsets(BinStore store, String prefix,
            List<ChainEntry> entries, long epoch, List<Violation> found, int depth)
            throws IOException {
        Map<RunKey, Long> inherited = new HashMap<>();
        if (entries.isEmpty()) {
            return inherited;
        }
        if (depth > 1000) {
            return inherited;
        }
        // ⚠️ "DID I HAVE A PREDECESSOR AT ALL" IS ALSO CHECKED, and round-2 review
        // found this door still open after the first fix closed "was my
        // predecessor sealed FOR me". Both of these used to return silently and
        // seed an EMPTY map, so a chain that simply omits or disowns its link
        // inherits nothing and every delta in it reads as a fresh start -- the
        // same defect as F1 one question earlier, and answered on the successor's
        // word in exactly the same way.
        if (!(entries.getFirst() instanceof Continue opening)) {
            if (entries.stream().anyMatch(e -> e instanceof CommitDelta)) {
                found.add(new Violation("link", "epoch " + epoch + " carries committed deltas "
                        + "but never wrote a CONTINUE, so its offsets rest on nothing"));
            }
            return inherited;
        }
        if (opening.prevEpoch() < 1) {
            // ⚠️ Epoch 0 is the reserved unleased chain, so naming it says
            // truthfully "there was no predecessor" -- TRUE for a first leader
            // and a LIE for anyone else, which is what this checks. A later
            // leader disowning its history inherits nothing and restarts every
            // stream, which is I2 arriving through the link rather than through
            // an offset.
            long ancestor = firstAncestorWithDeltas(store, prefix, epoch);
            if (ancestor > 0) {
                found.add(new Violation("link", "epoch " + epoch + " claims no predecessor, "
                        + "but epoch " + ancestor + " carries committed deltas"));
            }
            return inherited;
        }

        // ⚠️ TWO DIFFERENT QUESTIONS, and the first version of this conflated
        // them into one walk. "WHO SEALED ME" is answered by the IMMEDIATE
        // predecessor the CONTINUE names, and is where the link can be wrong.
        // "WHERE DO MY OFFSETS COME FROM" is answered by walking back through
        // chains that were abandoned before they were opened, because such a
        // chain carries no CONTINUE and so conveys no history of its own.
        // Answering the second with the first made every severed link look like
        // data corruption; answering the first with the second made a wrong link
        // invisible.
        checkLink(store, prefix, epoch, opening, found);

        inherited.putAll(offsetsSealedInto(store, prefix, opening.prevEpoch(), found, depth));
        return inherited;
    }

    /**
     * The offsets a chain inherits when its predecessor is {@code startFrom},
     * walking back over epochs that were never opened.
     *
     * <p>⚠️ EXTRACTED SO A NEVER-OPENED CHAIN CAN USE IT TOO (M4.13i). This walk
     * used to sit inside {@link #inheritedOffsets}, reachable only through a
     * CONTINUE -- so a chain sealed at slot 0, which carries no CONTINUE, had no
     * way to reach the very arithmetic that defines its base, and
     * {@code ReaderInvariants} declined to judge it. That was the sub-case M4's
     * acceptance criterion 3(a) is written about, and the measured cost was that
     * a reader restarting every stream at 0 across a failover came back
     * NOT-JUDGED rather than as I4.
     *
     * <p>⚠️ THE WALK IS THE PROTOCOL'S OWN ARITHMETIC: epochs advance by exactly
     * one per acquisition, so a leader that acquired and died before writing its
     * CONTINUE leaves a SEAL at slot 0 or nothing at all, and neither carries a
     * link. Stepping back one epoch at a time is therefore sound, and it is a
     * WALK rather than a decrement because several leaders can die in a row --
     * that is a lease fight, not a pathology.
     */
    static Map<RunKey, Long> offsetsSealedInto(BinStore store, String prefix, long startFrom,
            List<Violation> found, int depth) throws IOException {
        // ⚠️ NO DEPTH GUARD HERE, and its absence is deliberate rather than an
        // omission. One was carried over in the extraction and MEASURED dead:
        // deleting it left the whole suite green, because both callers have
        // already handled the condition -- `inheritedOffsets` returns on it with
        // the same `depth` before reaching this, and `checkReader` passes a
        // literal 0. The guard that matters is on the recursion, which is
        // `inheritedOffsets`', and `depth` is threaded through to reach it.
        Map<RunKey, Long> inherited = new HashMap<>();
        long from = startFrom;
        List<ChainEntry> prior = from >= 1 ? Invariants.readChain(store, prefix, from) : List.of();
        while (from >= 1 && Invariants.neverOpened(prior)) {
            from -= 1;
            prior = from >= 1 ? Invariants.readChain(store, prefix, from) : List.of();
        }
        if (from < 1 || prior.isEmpty()) {
            return inherited;
        }

        // ⚠️ FIRST SEAL AGAIN, same bug and same reason: folding to the LAST one
        // inherits everything a fenced writer appended between the two.
        long upTo = Long.MAX_VALUE;
        for (ChainEntry e : prior) {
            if (e instanceof Seal s) {
                upTo = s.sequence();
                break;
            }
        }
        // ⚠️ FOLDED TO THE PREDECESSOR'S OWN SEAL where it has one, not to the
        // successor's claim about it. Where the two disagree, `checkLink` has
        // already said so and the seal wins here: it is the entry the
        // predecessor's own writer raced for.
        inherited.putAll(inheritedOffsets(store, prefix, prior, from, found, depth + 1));
        for (ChainEntry e : prior) {
            if (e.sequence() > upTo) {
                break;
            }
            if (e instanceof CommitDelta delta) {
                for (RunCommit run : delta.allRuns()) {
                    inherited.merge(run.key(), run.lastOffset() + 1, Math::max);
                }
            }
        }
        return inherited;
    }

    /**
     * The highest epoch below this one whose chain carries committed deltas, or 0.
     *
     * <p>⚠️ Read from the store, not from any chain's claim about it -- the point
     * is to catch a chain whose CONTINUE disowns a history that demonstrably
     * exists.
     */
    private static long firstAncestorWithDeltas(BinStore store, String prefix, long epoch)
            throws IOException {
        for (long e = epoch - 1; e >= 1; e--) {
            List<ChainEntry> chain = Invariants.readChain(store, prefix, e);
            if (chain.stream().anyMatch(x -> x instanceof CommitDelta)) {
                return e;
            }
        }
        return 0;
    }

    /**
     * Is the chain this one claims to continue from actually SEALED for it?
     *
     * <p>⚠️ READ FROM THE PREDECESSOR, which is the whole point. Taking
     * {@code prevEpoch}/{@code prevSeq} off the successor's own CONTINUE copies
     * the pair {@code LocalSequencer.start} hands to {@code open}, so the
     * checker would make production's mistake and agree with it. MEASURED by
     * round-1 review: with the boundary taken on the successor's word, mutating
     * production to {@code open(prevEpoch, 0)} -- every stream restarting at 0
     * on every failover -- left the checker silent and the clean-run simulation
     * green across 18 takeovers.
     */
    private static void checkLink(BinStore store, String prefix, long epoch,
            Continue opening, List<Violation> found) throws IOException {
        List<ChainEntry> named = Invariants.readChain(store, prefix, opening.prevEpoch());
        // ⚠️ THE FIRST SEAL, NOT THE LAST, and this was a real masking bug: the
        // scan used to keep the last one while `checkChain`'s I5 arm keeps the
        // first, so the file held two definitions of "the seal". A chain with a
        // fenced writer's delta after its seal, and then a SECOND seal past that
        // -- reachable in production, because `ChainReplay.chainEnd` inspects only
        // the last key and so writes a second seal without noticing the first --
        // made the successor inherit the fenced writer's offsets and the boundary
        // report CLEAN. Measured: epoch 3 resuming at 8, past the real barrier at
        // slot 2, returned no violations at all.
        Seal closing = null;
        for (ChainEntry e : named) {
            if (e instanceof Seal s) {
                closing = s;
                break;
            }
        }
        if (closing == null) {
            // ⚠️ THE M4.16 DEFECT AT ITS SOURCE: a chain continued from one that
            // nobody sealed. Its writer met no barrier, so it may still be
            // committing at offsets this chain is about to hand out again.
            found.add(new Violation("link", "epoch " + epoch + " continues from epoch "
                    + opening.prevEpoch() + ", which is NOT SEALED -- its writer met no barrier"));
            return;
        }
        // ⚠️ `<= epoch`, NOT `!= epoch`, AND THE GAP MUST BE BURNED (ADR-0037).
        // A seal is write-once, so `continuedAt` names the FIRST successor that
        // fenced this chain; that one can burn and the next inherits the same
        // history legitimately. ⚠️ THE GAP IS READ, NOT TRUSTED -- the walk that
        // chose this ancestor already believes those epochs are burned, and a
        // checker taking its word could not disagree with it (M4.19).
        if (closing.continuedAt() > epoch) {
            found.add(new Violation("link", "epoch " + epoch + " continues from epoch "
                    + opening.prevEpoch() + ", whose SEAL names epoch " + closing.continuedAt()
                    + " -- a LATER epoch, so this chain is not that seal's successor"));
        } else {
            // ⚠️ FROM THE NAMED PREDECESSOR, not from `continuedAt`. Starting at
            // `continuedAt` skips the epochs between the predecessor and its own
            // seal's successor, which is where the same defect hides one epoch
            // lower -- an epoch opened LATE while the one above it stayed
            // burned. Strictly stronger at the same cost.
            for (long between = opening.prevEpoch() + 1; between < epoch; between++) {
                if (!Invariants.neverOpened(Invariants.readChain(store, prefix, between))) {
                    found.add(new Violation("link", "epoch " + epoch + " continues from epoch "
                            + opening.prevEpoch() + ", whose SEAL names epoch "
                            + closing.continuedAt() + ", but epoch " + between
                            + " in between WAS opened and is the real ancestor"));
                    break;
                }
            }
        }
        if (opening.prevSeq() != closing.sequence()) {
            found.add(new Violation("link", "epoch " + epoch + " claims epoch "
                    + opening.prevEpoch() + " ended at " + opening.prevSeq()
                    + " but its SEAL sits at " + closing.sequence()));
        }
    }
}
