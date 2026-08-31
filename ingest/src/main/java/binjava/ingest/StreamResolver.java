// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.UUID;

/**
 * Maps an index NAME, which is what a producer sends, to the stream id the
 * segment format and the plugin agree on.
 *
 * <p>⚠️ The seam exists because the two ends name the same index differently: a
 * producer writes {@code POST /logs/_bulk}, while the plugin derives its stream
 * from the OpenSearch index UUID. Something has to join them, and leaving it
 * implicit is how a producer's write lands in a stream no consumer is reading.
 *
 * <p>⚠️ M1 resolves this from a mapping supplied by the caller. The durable
 * ordinal registry is M1.6c; this interface is what keeps that a change behind
 * one seam rather than through the whole write path.
 */
// SKELETON: replaced by M1.6c
@FunctionalInterface
public interface StreamResolver {

    /**
     * The stream id for {@code index}.
     *
     * @throws IllegalArgumentException if the index is not known
     */
    UUID streamIdOf(String index);
}
