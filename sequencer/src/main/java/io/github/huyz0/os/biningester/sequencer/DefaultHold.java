// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * A default-path run of a walked index waits for its stream's decision
 * (ADR-0081 §5 step 3's tail; M13.33n): refused before anything is written,
 * so its retry is the wait -- a commit admitted now could take offsets a
 * recovery voids. The whole call is refused, since a request's runs split
 * across deltas is §8's (M13.34). Split out of {@link LocalSequencer} at its
 * 600-line ceiling.
 */
final class DefaultHold {

    /**
     * How long a held commit waits for its decision before it is refused
     * (M13.33n review round 1, P1): a takeover's usual span, so its retry --
     * a forwarded commit's inbox intent among them -- is the rare case.
     */
    static final Duration WAIT = Duration.ofSeconds(5);

    /** How often a waiting commit looks again. */
    static final Duration POLL = Duration.ofMillis(50);

    private DefaultHold() {
    }

    /**
     * Waits, OUTSIDE the sequencer's monitor -- the takeover's own commits
     * take it -- until no run of {@code requests} is held, or {@link #WAIT}.
     *
     * @throws IOException interrupted while waiting: nothing committed
     */
    static void await(FastLeaderTerm fast, List<CommitRequest> requests) throws IOException {
        for (long polls = WAIT.toMillis() / POLL.toMillis(); polls > 0 && held(fast, requests);
                polls--) {
            try {
                Thread.sleep(POLL);
            } catch (InterruptedException stopping) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for a takeover decision:"
                        + " nothing was committed", stopping);
            }
        }
    }

    private static boolean held(FastLeaderTerm fast, List<CommitRequest> requests) {
        try {
            refuseHeld(fast, requests);
            return false;
        } catch (IOException refused) {
            return true;
        }
    }

    /**
     * @param fast the term's fast-mode side, null before its start opened it
     * @throws IOException a run of {@code requests} is held: nothing committed
     */
    static void refuseHeld(FastLeaderTerm fast, List<CommitRequest> requests)
            throws IOException {
        if (fast == null) {
            return;
        }
        for (CommitRequest request : requests) {
            for (RunKey stream : request.recordCounts().keySet()) {
                if (fast.holdsDefault(stream)) {
                    throw new IOException("stream " + stream + " awaits its takeover decision:"
                            + " nothing was committed, retry");
                }
            }
        }
    }
}
