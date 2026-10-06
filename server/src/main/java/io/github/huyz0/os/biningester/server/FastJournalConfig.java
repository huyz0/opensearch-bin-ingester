// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.util.Objects;

/**
 * Where a pod keeps its fast journal and epoch file, and the journal's cap
 * (ADR-0082 §4, ADR-0083; M13.27i). A pod configured with none is diskless.
 *
 * @param directory the pod's {@code emptyDir}
 * @param capBytes the most held entry bytes the journal takes: a holder's cap,
 *     twice the leader's (ADR-0081 §5 step 7)
 */
public record FastJournalConfig(String directory, long capBytes) {

    /** A holder's cap: twice the leader's 256 MiB (ADR-0081 §7). */
    public static final long DEFAULT_CAP_BYTES = 512L << 20;

    public FastJournalConfig {
        Objects.requireNonNull(directory, "directory");
        if (directory.isBlank()) {
            throw new IllegalArgumentException("the fast journal's directory is blank");
        }
        if (capBytes < 1) {
            throw new IllegalArgumentException("the fast journal's cap is positive: " + capBytes);
        }
    }
}
