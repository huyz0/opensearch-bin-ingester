// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * I1–I5 as predicates over what is actually in the store.
 *
 * <p>⚠️ READ FROM THE OBJECTS, NEVER FROM A LIVE {@code CommitLog}. Asking the
 * implementation whether it upheld its own invariants is the consistency trap
 * this repository has already paid for twice — a check that shares a defect with
 * the thing it checks agrees with it. So this walks the raw chain and decodes
 * bytes, and shares no state with any writer.
 *
 * <p>⚠️ NOT EVERY {@code invariant} STRING IS AN INVARIANT ID. Six kinds are
 * reported: {@code I1}, {@code I2}, {@code I5} name invariants;
 * {@code chain}, {@code gap} and {@code link} name structural defects that no
 * invariant covers by name. ⚠️ A consumer filtering on {@code startsWith("I")}
 * therefore drops {@code link} -- the only arm that catches either defect this
 * class was split out of M4.12 for. Filter on nothing; report them all.
 *
 * <p>⚠️ WHAT IS AND IS NOT CHECKED HERE, stated rather than implied:
 * <ul>
 *   <li><b>I1</b> — no sequence number written twice. Structural, and largely
 *       held by {@code putIfAbsent} rather than by this repository's code; it is
 *       checked anyway, because a backend whose write-once primitive slipped
 *       would break I1 with nothing here being wrong.</li>
 *   <li><b>I2</b> — offsets never reassigned. Checked WITHIN a chain AND across
 *       an epoch boundary, the latter since M4.6e shipped the cross-boundary
 *       read. ⚠️ THERE IS NO LONGER A BOOLEAN SAYING SO. A
 *       {@code crossEpochOffsetsChecked()} flag used to report it and was
 *       asserted in a test of its own -- a hard-coded {@code return true} with
 *       an assertion around it, which review measured as surviving the deletion
 *       of THREE separate pieces of real boundary examination. A claim that
 *       cannot go false is not evidence. What the boundary is examined by is
 *       now three behavioural fixtures -- offsets inherited THROUGH a chain
 *       that was never opened, offsets NOT inherited past the predecessor's
 *       SEAL, and two chains continuing from one predecessor -- each of which
 *       fails when the mechanism it names is removed.</li>
 *   <li><b>I5</b> — ⚠️ HALF OF IT. architecture.md defines I5 as two clauses:
 *       nothing acknowledged beyond a SEAL in its own chain, which is
 *       structural and IS checked here; and the ACK-ORDERING clause from
 *       ADR-0011 -- a commit is acked only after every lower-numbered write in
 *       its chain is confirmed -- which is about the ORDER a writer
 *       acknowledged in and leaves no trace in the stored bytes. ⚠️ THAT HALF
 *       IS NOW CHECKED (M4.11), by {@link #checkAckOrder}, which takes a TRACE
 *       rather than a store for exactly that reason: two runs producing
 *       byte-identical chains can differ in it. ⚠️ IT IS A SEPARATE ENTRY
 *       POINT, not part of {@link #checkChain}, because a caller holding only
 *       a store cannot supply the trace -- and a checkChain that silently
 *       skipped the clause would be the overclaim this list exists to avoid.
 *       ⚠️ NOTHING IN PRODUCTION EMITS A TRACE YET: `CommitLog` is strictly
 *       serial, so it satisfies the clause BY ACCIDENT rather than by
 *       construction, and M4.13 is where the simulation feeds a trace in.</li>
 *   <li><b>I3</b> — what a READER applies, which is not a property of the
 *       stored bytes alone. ⚠️ CHECKED (M4.13a), by
 *       {@link ReaderInvariants#checkReader}, and it is a SEPARATE ENTRY POINT
 *       for the same reason {@link #checkAckOrder} is: it needs the reader's own
 *       view, and a caller holding only a store cannot supply one.
 *       ⚠️ EXCEPT ON A CHAIN THAT WAS NEVER OPENED -- no objects at all, or a
 *       SEAL at slot 0 -- where that checker returns NOT-JUDGED rather than a
 *       verdict, and this list says so instead of reading CHECKED without
 *       qualification: a leader fenced at slot 0 whose reader keeps applying is
 *       the sub-case M4's acceptance criterion 3(a) is written about.
 *       **M4.13i** closes it.</li>
 *   <li><b>I4</b> — ⚠️ HALF OF IT, and the half is named rather than implied.
 *       The DROP clause -- a reader losing records the chain committed -- is
 *       checked by {@link ReaderInvariants#checkReader}. The REORDER clause is not: a
 *       next-offset map is a high-water mark, so runs folded in the wrong order
 *       land on the same number. What stands in for it is {@link #checkChain}'s
 *       I2 arm over the bytes, and a per-record consumer trace is what would
 *       close it -- **M4.13f**. ⚠️ AND IT CARRIES I3'S NEVER-OPENED EXCEPTION
 *       TOO (**M4.13i**), because {@code checkReader} declines before
 *       evaluating either invariant.</li>
 * </ul>
 */
public final class Invariants {

    private Invariants() {
    }

    /**
     * One step of a writer's acknowledgement trace (M4.11).
     *
     * <p>⚠️ THE ORDER A WRITER ACKED IN LEAVES NO TRACE IN THE STORE, which is
     * why I5's second clause needs this and cannot be a predicate over objects.
     * Two runs producing byte-identical chains can differ in whether one acked
     * a commit before a lower-numbered write was confirmed.
     *
     * <p>⚠️ THE EPOCH IS PART OF THE IDENTITY, and omitting it was measured
     * wrong in both directions: sequence numbers RESTART per chain
     * ({@code ChainReplay}: "OFFSETS CROSS, SEQUENCE NUMBERS DO NOT"), so one
     * set spanning the trace let chain A's confirmations satisfy chain B's
     * lookups -- a successor acking an unbacked write, silently -- while a
     * chain based above 0 reported false violations of a strictly serial
     * writer.
     */
    public record AckEvent(long epoch, long sequence, boolean isAck) {
        /** The store confirmed the write at {@code sequence} of {@code epoch}. */
        public static AckEvent confirmed(long epoch, long sequence) {
            return new AckEvent(epoch, sequence, false);
        }

        /** The writer told its caller that commit is durable. */
        public static AckEvent acked(long epoch, long sequence) {
            return new AckEvent(epoch, sequence, true);
        }
    }

    /**
     * I5's ACK-ORDERING clause: a commit is acked only after every
     * lower-numbered write IN ITS OWN CHAIN is confirmed (ADR-0011).
     *
     * <p>⚠️ PIPELINING IS PERMITTED AND THIS MUST NOT FORBID IT. ADR-0011 says
     * so in as many words: "Pipelining is permitted for throughput;
     * acknowledgement is not." The predicate is over ACKS, never over the order
     * writes were issued or confirmed in -- two writes may be outstanding
     * together, and confirming 1 before 0 is fine so long as 0 is acked first.
     *
     * <p>⚠️ THE BASE IS THE CHAIN'S OWN FLOOR — the lowest sequence the trace
     * shows for that epoch — NOT the literal 0. A chain's slot 0 holds its
     * {@code CONTINUE}, so its first COMMIT sits at 1, and review measured a
     * base-0 scan reporting two violations of a perfectly serial writer.
     * ⚠️ THE CONSEQUENCE IS STATED RATHER THAN HIDDEN: a write this trace never
     * mentions cannot be judged, so a checker fed a partial trace judges only
     * what it was given. A GAP INSIDE the observed range IS a violation,
     * because chain sequences are contiguous.
     *
     * <p>⚠️ AND THE CASE THAT BUYS: a chain whose FIRST observed write is the
     * lost one reads clean, because the floor moves up to the survivor. A fresh
     * leader that pipelines its first two commits, loses 1 and acks 2 is
     * exactly that shape. ⚠️ THE FIX IS NOT A CALLER-SUPPLIED BASE -- an
     * unchecked parameter is a second thing to get wrong -- it is for the
     * caller to emit a CONFIRMED event for the slot-0 {@code CONTINUE}, which
     * pins the floor at 0 and removes the hole. M4.13 owns that.
     *
     * <p>⚠️ UNTIL THEN THE MITIGATION IS PARTIAL, and saying which half is
     * covered matters: the structural residue of that case -- an empty slot, or
     * an ack past a {@code SEAL} -- is caught by {@link #checkChain}'s gap and
     * I5 arms. A sweep running BOTH checkers is not blind to it; one running
     * only this checker is.
     *
     * <p>⚠️ ONE ACK IS AT MOST ONE VIOLATION, and M4.13 pins exact counts.
     */
    public static List<Violation> checkAckOrder(List<AckEvent> trace) {
        Map<Long, Long> base = new HashMap<>();
        for (AckEvent e : trace) {
            base.merge(e.epoch(), e.sequence(), Math::min);
        }
        List<Violation> found = new ArrayList<>();
        Map<Long, java.util.Set<Long>> confirmed = new HashMap<>();
        Map<Long, Long> watermark = new HashMap<>();
        for (AckEvent e : trace) {
            java.util.Set<Long> seen =
                    confirmed.computeIfAbsent(e.epoch(), k -> new java.util.HashSet<>());
            if (!e.isAck()) {
                seen.add(e.sequence());
                // ⚠️ A WATERMARK, so a clean N-commit trace is O(N) rather than
                // the O(N^2) a per-ack rescan costs: review measured 110ms at
                // 5,000 commits against M4.13's budget of 60s for 1,000 seeds.
                long w = watermark.computeIfAbsent(e.epoch(), base::get);
                while (seen.contains(w)) {
                    w++;
                }
                watermark.put(e.epoch(), w);
                continue;
            }
            long w = watermark.computeIfAbsent(e.epoch(), k -> base.get(k));
            if (!seen.contains(e.sequence())) {
                found.add(new Violation("I5", "epoch " + e.epoch() + " acked sequence "
                        + e.sequence() + " while its own write was unconfirmed"));
            } else if (w < e.sequence()) {
                found.add(new Violation("I5", "epoch " + e.epoch() + " acked sequence "
                        + e.sequence() + " while the lower-numbered write " + w
                        + " was still unconfirmed"));
            }
        }
        return found;
    }

    /** One violated invariant, named so a failing seed reports which. */
    public record Violation(String invariant, String detail) {
    }



    /** Walks one epoch's chain and returns every violation it can observe. */
    public static List<Violation> checkChain(BinStore store, String prefix, long epoch)
            throws IOException {
        List<Violation> found = new ArrayList<>();
        List<ChainEntry> entries = readChain(store, prefix, epoch);

        Map<Long, Integer> seenAt = new HashMap<>();
        Seal seal = null;
        long expected = 0;
        Map<RunKey, Long> nextOffset =
                inheritedOffsets(store, prefix, entries, epoch, found, 0);

        for (int i = 0; i < entries.size(); i++) {
            ChainEntry e = entries.get(i);
            Integer prior = seenAt.put(e.sequence(), i);
            if (prior != null) {
                found.add(new Violation("I1",
                        "sequence " + e.sequence() + " appears at positions " + prior + " and " + i));
            }
            if (e.sequence() != expected) {
                // ⚠️ A GAP is not I1, but it is a broken chain: `commit` claims
                // slots consecutively, so a hole means an entry was lost or a
                // writer skipped ahead — and consecutiveness is what makes the
                // seal collide with a fenced leader in the first place.
                found.add(new Violation("chain",
                        "expected sequence " + expected + " but found " + e.sequence()));
            }
            expected = e.sequence() + 1;

            if (seal != null) {
                found.add(new Violation("I5",
                        "entry at sequence " + e.sequence() + " lies beyond the SEAL at "
                                + seal.sequence() + " in its own chain"));
            }
            switch (e) {
                case Seal s -> seal = s;
                case Continue ignored -> { }
                case CommitDelta delta -> {
                    for (RunCommit run : delta.runs()) {
                        long from = nextOffset.getOrDefault(run.key(), 0L);
                        // ⚠️ A REWIND AND A GAP ARE DIFFERENT DEFECTS, and the
                        // first version of this reported both as I2. I2 is
                        // "offsets are never REASSIGNED", so only resuming BELOW
                        // the high-water mark violates it -- two writers handing
                        // the same offset to different records. Resuming ABOVE it
                        // loses no data and duplicates nothing; it is a hole,
                        // which readers and NFR-11 care about for other reasons.
                        // Conflating them makes every severed CONTINUE read as
                        // data corruption and buries the real thing.
                        if (run.firstOffset() < from) {
                            found.add(new Violation("I2",
                                    "stream " + run.key() + " resumes at " + run.firstOffset()
                                            + " but the chain had assigned up to " + from));
                        } else if (run.firstOffset() > from) {
                            found.add(new Violation("gap",
                                    "stream " + run.key() + " resumes at " + run.firstOffset()
                                            + ", skipping " + (run.firstOffset() - from)));
                        }
                        nextOffset.put(run.key(),
                                Math.max(from, run.firstOffset() + run.recordCount()));
                    }
                }
            }
        }
        return found;
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

        long from = opening.prevEpoch();
        List<ChainEntry> prior = readChain(store, prefix, from);
        // ⚠️ STEP OVER CHAINS THAT WERE NEVER OPENED, by the arithmetic the
        // protocol guarantees: epochs advance by exactly one per acquisition. A
        // leader that acquired and died before writing its CONTINUE leaves a
        // SEAL at slot 0 or nothing at all, and neither carries a link.
        while (from >= 1 && neverOpened(prior)) {
            from -= 1;
            prior = from >= 1 ? readChain(store, prefix, from) : List.of();
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
                for (RunCommit run : delta.runs()) {
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
            List<ChainEntry> chain = readChain(store, prefix, e);
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
        List<ChainEntry> named = readChain(store, prefix, opening.prevEpoch());
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
        if (closing.continuedAt() != epoch) {
            found.add(new Violation("link", "epoch " + epoch + " continues from epoch "
                    + opening.prevEpoch() + ", whose SEAL names epoch " + closing.continuedAt()));
        }
        if (opening.prevSeq() != closing.sequence()) {
            found.add(new Violation("link", "epoch " + epoch + " claims epoch "
                    + opening.prevEpoch() + " ended at " + opening.prevSeq()
                    + " but its SEAL sits at " + closing.sequence()));
        }
    }

    /** A chain that carries no link: abandoned before its CONTINUE was written. */
    static boolean neverOpened(List<ChainEntry> entries) {
        if (entries.isEmpty()) {
            return true;
        }
        return entries.getFirst() instanceof Seal sealed && sealed.sequence() == 0;
    }

    static List<ChainEntry> readChain(BinStore store, String prefix, long epoch)
            throws IOException {
        CommitLog addressing = new CommitLog(store, prefix, epoch);
        LogKeys keys = new LogKeys(prefix, epoch);
        List<ChainEntry> out = new ArrayList<>();
        String after = null;
        while (true) {
            ListPage page = store.list(addressing.logPrefix(), after, 1000);
            for (ObjectStat stat : page.objects()) {
                // ⚠️ THE SAME ALLOW-LIST PRODUCTION USES, and NOT a caught
                // decode failure. `catch (IOException) { continue; }` here would
                // blind the invariant checker to genuinely corrupt bytes -- the
                // inversion `check-test-integrity.sh` exists to catch, and the
                // one M4.8b1's row names as the tempting repair.
                // ⚠️ THIS SITE WAS A LANDMINE FOR M4.13. The simulation is green
                // only because checkpointing never triggers in it; review
                // measured that forcing one pointer PUT per commit fails seven
                // simulation and invariant tests with "not a chain entry: bad
                // magic". The 1,000-seed run IS the milestone's completion
                // condition, so it would have detonated there.
                if (!keys.isEntryKey(stat.key())) {
                    continue;
                }
                try (InputStream in = store.get(stat.key())) {
                    out.add(ChainEntry.decode(in.readAllBytes()));
                }
            }
            if (page.nextStartAfter().isEmpty()) {
                return out;
            }
            after = page.nextStartAfter().get();
        }
    }
}
