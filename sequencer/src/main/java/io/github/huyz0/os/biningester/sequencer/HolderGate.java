// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;
import java.util.Optional;

/**
 * Whether a pod still accepts copies, under one lock with what it accepts
 * (M13.28a; M13.26h's review, P10): a REPLICA is checked and journaled under
 * it, and a departure's flag and its empty-journal check are one step under
 * it -- so no entry is appended and acked after a departure found the
 * journal empty.
 */
public final class HolderGate {

    /** A step that may fail on the journal. */
    @FunctionalInterface
    public interface IoCall<T> {
        T call() throws IOException;
    }

    private boolean departing;

    /** Whether this pod still accepts copies: false from its first departure step on. */
    public synchronized boolean accepting() {
        return !departing;
    }

    /** Stops accepting copies. */
    public synchronized void depart() {
        departing = true;
    }

    /** Runs {@code call} if still accepting, under this gate; empty if departing. */
    public synchronized <T> Optional<T> whileAccepting(IoCall<T> call) throws IOException {
        if (departing) {
            return Optional.empty();
        }
        return Optional.of(call.call());
    }

    /**
     * Stops accepting copies and says whether {@code journal} holds nothing
     * -- one step, so no copy lands between the two.
     */
    public synchronized boolean departIfEmpty(FastJournal journal) {
        departing = true;
        return journal.held().isEmpty();
    }
}
