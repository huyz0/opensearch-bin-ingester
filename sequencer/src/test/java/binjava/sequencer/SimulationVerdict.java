// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * What one simulated run left in the store, judged (M4.13, split out at M5.7).
 *
 * <p>⚠️ EXTRACTED FROM {@link CommitProtocolSimulation} under code-structure
 * rule 1. NO LINE-COUNT TRIGGER IS CLAIMED (M5.54). The seam is real rather
 * than convenient:
 * everything left there DRIVES a fleet through a fault injector, and everything
 * here JUDGES the bytes that fleet left behind, reading the backing store
 * DIRECTLY so that no injected fault can reach a checker.
 *
 * @param violations every invariant the run broke, empty if none
 * @param highestEpoch the highest epoch whose chain actually exists
 * @param readersChecked how many production readers were driven — ⚠️ published
 *     so a test can see the loop FIRE. Review measured that deleting the
 *     {@code addAll} while leaving the call in place left the whole suite
 *     green, on the loop carrying I3 and I4's drop clause
 */
record SimulationVerdict(List<Invariants.Violation> violations, long highestEpoch,
        int readersChecked) {

    /**
     * Judges what {@code backing} holds after a run.
     *
     * <p>⚠️ EVERYTHING HELD LANDS BEFORE ANYTHING IS JUDGED. A write still in
     * the injector's queue when the checkers run is a write that never landed,
     * which is {@code withheldPut} — a different fault class, already modelled.
     * Draining here is what keeps {@code deferredPut} a DELAY.
     *
     * <p>⚠️ NOBODY IS ACTING WHILE THE CHECKERS RUN, and every partition is
     * lifted first. The checkers read {@code backing} directly so they are
     * already below the injector, but leaving an actor set would be a trap for
     * the next person who points a checker at the injected store: a checker
     * that could be partitioned reports violations describing the harness.
     */
    static SimulationVerdict of(MemoryBinStore backing, FaultInjectingStore faulty,
            Set<String> partitionedPods, List<AckOrderInvariants.AckEvent> acks,
            int pods, int rounds,
            Function<CommitLog, ReaderInvariants.ReaderView> viewFor) throws IOException {
        List<Invariants.Violation> violations = new ArrayList<>();
        long highest = 0;
        int readersChecked = 0;
        // ⚠️ ONE WALK PER EPOCH. The second `checkChain` this loop used to make
        // was a provably DEAD disjunct -- every violation is raised inside the
        // walk over entries, so a non-empty violation list already implies a
        // non-empty chain -- and it cost a full second LIST plus one GET per
        // entry in the phase M4.13's <60s budget lives in.
        long scanTo = pods * (long) rounds;
        faulty.drainPending();
        for (String pod : partitionedPods) {
            faulty.heal(pod);
        }
        partitionedPods.clear();
        faulty.actingAs(null);

        for (long epoch = 1; epoch <= scanTo; epoch++) {
            violations.addAll(Invariants.checkChain(backing, CommitProtocolSimulation.PREFIX,
                    epoch));
            if (CommitProtocolSimulation.hasChain(backing, epoch)) {
                highest = Math.max(highest, epoch);
                // ⚠️ I3 AND I4's DROP CLAUSE, asked of PRODUCTION'S OWN READER
                // at ITS OWN epoch (M4.13a). ⚠️ ONE CALL PER READER, NEVER SWEPT
                // OVER A RANGE: `checkChain` is safe at every epoch in a range
                // and this is not, which is why `ReaderView` carries the epoch.
                CommitLog reader = new CommitLog(backing, CommitProtocolSimulation.PREFIX, epoch);
                reader.recover();
                violations.addAll(ReaderInvariants.checkReader(
                        backing, CommitProtocolSimulation.PREFIX, viewFor.apply(reader)));
                readersChecked++;
            }
        }
        // ⚠️ AND I5's ACK-ORDERING CLAUSE, which no walk over the store can see.
        // The CONFIRMED half of this trace comes from `AckTraceStore` at the
        // moment each PUT lands (M4.50), so the two halves have independent
        // authors and a writer that acknowledges ahead of its own durable write
        // -- or ahead of a lower-numbered one still outstanding -- shows up.
        violations.addAll(AckOrderInvariants.checkAckOrder(acks));
        // ⚠️ THE SCAN BOUND IS ASSERTED, NOT ASSUMED. `highest` is computed by
        // the loop above, so it can never report an epoch outside the range it
        // scanned -- a run that burned more epochs than the bound would report
        // CLEAN about chains nobody read, which is the simulation's own
        // anti-vacuity risk arriving through the scan bound instead of through
        // the faults.
        if (CommitProtocolSimulation.hasChain(backing, scanTo + 1)) {
            throw new IllegalStateException("the run reached epoch " + (scanTo + 1)
                    + ", beyond the scanned bound " + scanTo
                    + " -- chains past it were never checked");
        }
        return new SimulationVerdict(violations, highest, readersChecked);
    }
}
