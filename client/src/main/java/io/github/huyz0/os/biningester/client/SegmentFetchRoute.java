// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

/**
 * The proxy segment route's shape, shared by the ingester that serves it and
 * the consumer that fetches from it (M10.1, ADR-0073).
 *
 * <p>⚠️ **DEFINED ON THE CONSUMER'S SIDE**, like the subscription paths
 * (M8.21): {@code http} depends on {@code client} and never the reverse, so
 * one definition here is read by both, and a rename cannot leave the two
 * disagreeing.
 */
public final class SegmentFetchRoute {

    /** {@code GET} answers the whole segment named by {@link #KEY_PARAM}. */
    public static final String PATH = "/seg";

    /** The segment's full object key, URL-encoded. */
    public static final String KEY_PARAM = "key";

    /**
     * The caller's own word for its zone, used for NFR-5 accounting only --
     * the same parameter, and the same trust, as a subscription poll's.
     */
    public static final String AZ_PARAM = HttpSubscriptionTransport.AZ_PARAM;

    private SegmentFetchRoute() {
    }
}
