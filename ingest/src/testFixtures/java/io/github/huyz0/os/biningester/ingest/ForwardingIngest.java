// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;

/**
 * A test double's base (M13.20, M12 harvest R18): the three members M12.2 made
 * abstract on {@link Ingest}, stated ONCE with the behaviour their removed
 * defaults had, where each double in the :http and :ingest tests copied
 * them.
 *
 * <p>⚠️ NOT PRODUCTION's ORDER. {@code buffered} runs once the append has
 * RETURNED -- the removed default's order -- where {@link DefaultIngest} runs
 * it once the records are held and before the durable wait (M11.7), which is
 * where the front door gives its lane permit back. A double whose test
 * observes admission across that wait overrides the buffered forms, as
 * {@code BulkServiceQuotaTest.Recording} and {@code LaneOvertakeTest}'s
 * witness do (M13.13).
 *
 * <p>⚠️ AND NO CATALOG: every name is its own index. A double that resolves
 * aliases overrides {@link #concreteIndex}.
 */
public abstract class ForwardingIngest implements Ingest {

    /** The lane form, then {@code buffered} once it returns, however it ends. */
    @Override
    public AppendResult append(Principal principal, String index, int partition, byte lane,
            RecordSource records, Runnable buffered) throws IOException {
        try {
            return append(principal, index, partition, lane, records);
        } finally {
            buffered.run();
        }
    }

    /** The routed lane form, then {@code buffered} once it returns, however it ends. */
    @Override
    public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            byte lane, RecordSource records, Runnable buffered) throws IOException {
        try {
            return appendRouted(principal, indexOrAlias, routing, lane, records);
        } finally {
            buffered.run();
        }
    }

    /** No catalog in a double: every name is its own index. */
    @Override
    public String concreteIndex(String indexOrAlias) {
        return indexOrAlias;
    }
}
