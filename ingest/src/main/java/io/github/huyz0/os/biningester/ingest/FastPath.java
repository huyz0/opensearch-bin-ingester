// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * A {@code wal=true} index's appends, through its term's leader (M13.29b;
 * ADR-0081 §2): answered at the offsets assigned, and only once the batch is
 * EXPOSED -- a producer is acked on nothing earlier.
 */
@FunctionalInterface
public interface FastPath {

    /** No fast path: every index is written the default way. */
    FastPath NONE = (stream, records) -> Optional.empty();

    /**
     * Writes {@code records} of {@code stream} fast.
     *
     * @return the append's offsets once EXPOSED, or empty when the leader
     *     takes no fast write (NOT_FAST): the default path writes the same
     *     records
     * @throws IOException the batch could not be written; the producer retries
     */
    Optional<AppendResult> append(RunKey stream, List<SegmentRecord> records)
            throws IOException;
}
