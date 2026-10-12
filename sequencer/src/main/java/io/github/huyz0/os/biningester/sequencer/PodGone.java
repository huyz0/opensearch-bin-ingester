// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;

/**
 * Whether an incarnation is GONE (ADR-0081 §5 step 4; M13.33c): its pod is
 * deleted, so the copies it held are lost. The Kubernetes pod lookup, an
 * injected seam.
 *
 * <p>⚠️ NOT READY, OUT OF THE ENDPOINTSLICE OR PARTITIONED IS NOT GONE, and
 * is waited for (obligation 3): a live copy counted lost would be voided.
 * An answer the lookup cannot give is an {@code IOException}, never a guess
 * -- the caller waits.
 */
@FunctionalInterface
public interface PodGone {

    /** Outside Kubernetes no pod is ever gone unless a seam says so (ADR-0081). */
    PodGone NEVER = incarnation -> false;

    /**
     * Whether {@code incarnation}'s pod is deleted.
     *
     * @throws IOException the lookup gave no answer; the caller waits
     */
    boolean gone(Roster.Incarnation incarnation) throws IOException;
}
