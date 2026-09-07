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
import java.util.LinkedHashMap;
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
 * <p>⚠️ NOT EVERY {@code invariant} STRING IS AN INVARIANT ID. Seven kinds are
 * reported: {@code I1}, {@code I2}, {@code I5} name invariants;
 * {@code chain}, {@code gap}, {@code link} and {@code order} name structural
 * defects that no invariant covers by name. ⚠️ A consumer filtering on
 * {@code startsWith("I")} therefore drops {@code link} and {@code order} --
 * {@code link} is the only arm that catches either defect this class was split
 * out of M4.12 for. Filter on nothing; report them all.
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
 *       IS NOW CHECKED (M4.11), by {@link AckOrderInvariants#checkAckOrder},
 *       which takes a TRACE
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
 *       for the same reason {@link AckOrderInvariants#checkAckOrder} is: it needs
 *       the reader's own
 *       view, and a caller holding only a store cannot supply one.
 *       ⚠️ ON EVERY CHAIN, INCLUDING ONE THAT WAS NEVER OPENED (M4.13i). A
 *       leader fenced at slot 0 whose reader keeps applying is the sub-case M4's
 *       acceptance criterion 3(a) is written about, and it used to come back
 *       NOT-JUDGED. Its base is the backward walk of
 *       {@link #offsetsSealedInto}; an EMPTY chain inherits nothing instead,
 *       because production has not crossed at that point, and conflating the two
 *       was measured reporting a false I4 against production's own reader.</li>
 *   <li><b>I4</b> — "Uncommitted records may be reordered or dropped; committed
 *       ones may not". The DROP clause is checked by
 *       {@link ReaderInvariants#checkReader}, on every chain since M4.13i.
 *       ⚠️ THE REORDER CLAUSE HAS NO ARM OF ITS OWN (M4.13f). At the
 *       granularity the CHAIN carries -- which stream's records occupy which
 *       RUN of offsets -- it is carried by {@link #checkChain}'s I2 arm plus the
 *       chain being append-only: a run of offsets never reassigned, in entries
 *       never rewritten, is an order that cannot change. And a permutation
 *       WITHIN one delta is the clause's PERMITTED half, since every record in a
 *       delta is uncommitted until it lands -- {@code DefaultIngest} completes
 *       the appends' futures only after {@code sequencer.commit} returns.
 *       ⚠️ BELOW THE RUN, NOTHING HERE CAN SEE IT, and this list says so rather
 *       than reading as a clean conclusion. WHICH record sits at offset 4 versus
 *       5 lives in the SEGMENT object, not in the chain, so a segment rewritten
 *       with the same counts in permuted order reorders committed records with
 *       {@code checkChain} and {@code checkReader} both silent. No object in
 *       this protocol is rewritten today, which is why it is a residue rather
 *       than a hole -- but the residue is the segment bytes, and no checker here
 *       reads them.
 *       ⚠️ SO THE ARM M4.13f ADDED IS {@code order}, NOT I4: a delta whose
 *       segment order disagrees with its offset order is malformed, and worth
 *       reporting, but calling it I4 would claim an invariant that explicitly
 *       allows it.</li>
 * </ul>
 */
public final class Invariants {

    private Invariants() {
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
                CrossEpochInvariants.inheritedOffsets(store, prefix, entries, epoch, found, 0);

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
                    // ⚠️ `allRuns`, NEVER `runs()`: `runs()` REFUSES a delta
                    // carrying more than one segment, so every checker here
                    // threw on the batched commit M4's scope item 5 is about.
                    // An OFFSET is a stream fact, not a segment fact -- which is
                    // why `ChainReplay.fold` uses `allRuns()` too. M4.13n.
                    Map<RunKey, List<RunCommit>> bySegmentOrder = new LinkedHashMap<>();
                    for (RunCommit run : delta.allRuns()) {
                        bySegmentOrder.computeIfAbsent(run.key(), k -> new ArrayList<>()).add(run);
                    }
                    for (Map.Entry<RunKey, List<RunCommit>> stream : bySegmentOrder.entrySet()) {
                        List<RunCommit> inSegmentOrder = stream.getValue();
                        // ⚠️ `order`, NOT `I4`, AND THE DIFFERENCE IS THE WHOLE
                        // POINT (M4.13f). `commitSubmissions` walks a batch in
                        // list order against ONE shared cursor, so a delta whose
                        // segment order disagrees with its offset order is
                        // malformed. But I4 permits reordering UNCOMMITTED
                        // records, and every record in a delta is uncommitted
                        // until it lands -- so this is I4's permitted half, and
                        // the name joins `chain`, `gap` and `link` as a defect
                        // no invariant covers.
                        for (int r = 1; r < inSegmentOrder.size(); r++) {
                            if (inSegmentOrder.get(r).firstOffset()
                                    < inSegmentOrder.get(r - 1).firstOffset()) {
                                // ⚠️ NO SEGMENT INDEX: `r` walks this STREAM's
                                // runs, not the delta's segments, so the old
                                // wording named a "segment 1" that need not
                                // exist. The offsets say it without that.
                                found.add(new Violation("order", "stream " + stream.getKey()
                                        + " runs out of order within the delta at sequence "
                                        + e.sequence() + ": a run starting at "
                                        + inSegmentOrder.get(r).firstOffset()
                                        + " follows one starting at "
                                        + inSegmentOrder.get(r - 1).firstOffset()));
                                break;
                            }
                        }
                        // ⚠️ FOLDED IN OFFSET ORDER, NOT SEGMENT ORDER, which is
                        // what lets the three defects keep their own names:
                        // against a HIGH-WATER MARK a permutation and a
                        // reassignment are identical. M4.13f.
                        List<RunCommit> inOffsetOrder = new ArrayList<>(inSegmentOrder);
                        inOffsetOrder.sort(java.util.Comparator.comparingLong(RunCommit::firstOffset));
                        for (RunCommit run : inOffsetOrder) {
                            long from = nextOffset.getOrDefault(run.key(), 0L);
                            // ⚠️ A REWIND AND A GAP ARE DIFFERENT DEFECTS, and the
                            // first version reported both as I2. I2 is "offsets are
                            // never REASSIGNED", so only resuming BELOW the
                            // high-water mark violates it -- two writers handing the
                            // same offset to different records. Resuming ABOVE it
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
        }
        return found;
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
