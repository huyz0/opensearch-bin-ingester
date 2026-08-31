// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.SegmentRecord;
import binjava.security.Principal;
import java.io.IOException;
import java.util.List;

/**
 * The library surface: append records, learn when they are durable.
 *
 * <p>⚠️ THIS IS THE PRIMARY API AND HTTP IS AN ADAPTER OVER IT (ADR-0019). The
 * four assumptions ADR-0018 calls unvalidated are all on the consumer side and
 * none of them needs a socket, so this is built and tested first;
 * {@code check-module.sh} then asserts that no module below {@code http}
 * resolves an HTTP dependency, which is what keeps the adapter thin rather than
 * a second implementation.
 *
 * <p>⚠️ BLOCKING, because virtual threads make a blocking call the concurrent
 * API. {@code append} returns when the records are durable, not when they are
 * accepted into a buffer.
 */
public interface Ingest extends AutoCloseable {

    /**
     * Appends records to one index and partition, returning once they are
     * durable.
     *
     * <p>⚠️ The {@link Principal} decides the trust domain, and a producer
     * cannot write outside the indices its credential names (ADR-0021). The
     * partition is EXPLICIT in M1: aliases and {@code os_routing} are ADR-0015
     * and land in M6.
     *
     * @throws IllegalArgumentException if the principal may not write to the index
     */
    AppendResult append(Principal principal, String index, int partition,
            List<SegmentRecord> records) throws IOException;

    /** Flushes anything buffered and releases resources. */
    @Override
    void close() throws IOException;
}
