// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * One ingester node in the peer mesh (M5.8).
 *
 * <p>⚠️ THE AZ IS PART OF IDENTITY, not a lookup. A peer fetch is intra-AZ
 * only -- ADR-0012 measures a cross-AZ one at 419x the object-store GET it
 * would replace -- so every path that chooses a peer has to be able to scope by
 * AZ without asking anything else.
 */
public record Peer(String podId, String endpoint, String az) {

    public Peer {
        if (podId == null || podId.isBlank()) {
            throw new IllegalArgumentException("a peer needs a podId");
        }
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("a peer needs an endpoint");
        }
        if (az == null || az.isBlank()) {
            throw new IllegalArgumentException("a peer needs an az -- peer fetch is intra-AZ only");
        }
    }
}
