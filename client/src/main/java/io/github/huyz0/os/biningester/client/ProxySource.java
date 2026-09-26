// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.IOException;

/**
 * Where a consumer gets the bytes of a segment it was pushed as {@code proxy}:
 * the ingester node's segment route (M10.2, FR-6, ADR-0073).
 *
 * <p>⚠️ **A SEAM, SO THE CONSUMER NEVER OPENS A SOCKET ITSELF.**
 * {@link HttpProxySource} is the production implementation; a node wraps it so
 * K runs of one segment cost one fetch rather than K.
 */
@FunctionalInterface
public interface ProxySource {

    /**
     * The whole segment.
     *
     * @throws IOException on any failure -- ⚠️ never an empty array, which
     *     would advance every run's offsets past records nobody read
     */
    byte[] fetch(String segmentKey) throws IOException;
}
