// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.io.IOException;
import java.util.List;

/**
 * What the store JUDGED while a leader released its lease politely (M5.26).
 *
 * <p>⚠️ SPLIT OUT OF {@code CommitProtocolSimulation} when it crossed 700 lines
 * -- code-structure.md rule 1, split rather than raise, the same way {@code
 * SimulationVerdict} and {@code ForwardingPods} were. The seam is real: a
 * release is the one call in the run whose ACTOR is the question, and folding
 * the reading into the driver is what let the first version measure nothing.
 *
 * <p>⚠️ IT READS THE STORE, NOT THE CALLER'S OWN ASSIGNMENT. The first version
 * of this compared {@code faulty.actingPod()} against the pod it had been set
 * to one statement earlier, in a single-threaded driver -- true by
 * construction. Both reviewers killed it with the same mutation: restore the
 * stale actor AFTER the count and the whole pre-M5.26 defect is back with the
 * sweep green. {@code FaultInjectingStore} already records every partition
 * refusal as {@code Injected("partition", "pod:X")}, so what it actually
 * refused, and who it blamed, is on the record without new machinery.
 */
final class GracefulReleaseMeter {

    /** A release that may fail, so the meter can see whether it did. */
    interface Release {
        void run() throws IOException;
    }

    private final FaultInjectingStore faulty;
    private int released;
    private int refusedForAnotherPod;

    GracefulReleaseMeter(FaultInjectingStore faulty) {
        this.faulty = faulty;
    }

    /**
     * Runs {@code release} with {@code leaderPod} acting, and records what the
     * store did.
     *
     * <p>⚠️ THE ACTOR IS SET HERE AND UNCONDITIONALLY, which is M5.26 itself.
     * Leaving it unset let the store judge a graceful release against whoever
     * acted last and refuse it for a partition injected somewhere else.
     */
    void observe(String leaderPod, Release release) {
        faulty.actingAs(leaderPod);
        int before = faulty.injected().size();
        int callsBefore = faulty.calls();
        boolean completed = true;
        try {
            release.run();
        } catch (IOException injected) {
            // a fault during release leaves the lease to expire, which is
            // exactly the ungraceful path
            completed = false;
        }
        if (completed && faulty.calls() > callsBefore) {
            // ⚠️ THE RELEASE THAT REACHED THE STORE, not the branch that was
            // entered and not merely a call that returned. MEASURED, both:
            // counting branch entries kept a floor at 1,713 with
            // `leader.close()` DELETED outright, and counting normal returns
            // kept it green with the close replaced by an empty lambda, which
            // returns perfectly well and hands nothing back.
            released++;
        }
        List<FaultInjectingStore.Injected> during =
                faulty.injected().subList(before, faulty.injected().size());
        for (FaultInjectingStore.Injected one : during) {
            // ⚠️ A REFUSAL NAMING ANOTHER POD IS THE DEFECT ITSELF. The
            // leader's OWN partition refusing its release is legitimate --
            // that is the ungraceful path. Being refused for somebody else's
            // is what naming no actor caused.
            if ("partition".equals(one.kind()) && !("pod:" + leaderPod).equals(one.key())) {
                refusedForAnotherPod++;
                break;
            }
        }
    }

    /**
     * Releases that returned AND reached the store.
     *
     * <p>⚠️ NOT "returned rather than throwing", which is the weaker rule
     * this rejected: an empty lambda returns perfectly well and hands nothing
     * back, and review MEASURED that version staying green with
     * {@code leader.close()} replaced by one. Simplifying the guard to
     * {@code if (completed)} reinstates exactly that hole.
     */
    int released() {
        return released;
    }

    /** Releases the store refused while blaming a pod other than the leader. */
    int refusedForAnotherPod() {
        return refusedForAnotherPod;
    }
}
