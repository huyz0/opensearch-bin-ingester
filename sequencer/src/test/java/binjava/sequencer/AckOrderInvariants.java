// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.sequencer.Invariants.Violation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * I5's ACK-ORDERING clause: a commit is acknowledged only after every
 * lower-numbered write in its chain is confirmed (ADR-0011, M4.11).
 *
 * <p>⚠️ A SEPARATE FILE BECAUSE IT IS ON THE OTHER SIDE OF A SEAM THE 500-LINE
 * CAP FORCED M4.13a TO DRAW, and M4.13g is finishing that split rather than
 * making a new one. {@link Invariants} holds predicates over the STORED BYTES;
 * checkers that judge an OBSERVATION live beside {@link ReaderInvariants}. This
 * one takes a TRACE for exactly that reason -- two runs producing byte-identical
 * chains can differ in the order their writer acknowledged.
 *
 * <p>⚠️ NOTHING IN PRODUCTION EMITS A TRACE YET: {@code CommitLog} is strictly
 * serial, so it satisfies the clause BY ACCIDENT rather than by construction,
 * and M4.13 is where the simulation feeds one in.
 */
final class AckOrderInvariants {

    private AckOrderInvariants() {
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
    record AckEvent(long epoch, long sequence, boolean isAck) {
        /** The store confirmed the write at {@code sequence} of {@code epoch}. */
        static AckEvent confirmed(long epoch, long sequence) {
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
     * an ack past a {@code SEAL} -- is caught by {@link Invariants#checkChain}'s gap and
     * I5 arms. A sweep running BOTH checkers is not blind to it; one running
     * only this checker is.
     *
     * <p>⚠️ ONE ACK IS AT MOST ONE VIOLATION, and M4.13 pins exact counts.
     */
    static List<Violation> checkAckOrder(List<AckEvent> trace) {
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
}
