// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.Capabilities;
import java.util.Objects;

/**
 * Everything the serving path needs to decide how a segment travels, and to
 * make that decision happen (M5.45a, FR-6).
 *
 * <p>⚠️ IT IS A PARAMETER RATHER THAN A FIELD OF {@link SubscriptionHub}, and
 * that is non-negotiable 7 rather than taste. The hub is the routing decision --
 * which subscriber hears about which run -- and a hub that HELD a
 * {@link SegmentProxy} would hold a {@code BinStore} through it, making the
 * whole class untestable without a store and giving it an I/O dependency it
 * makes no use of on the `inline` path.
 *
 * <p>⚠️ THE CONSUMER SUPPLIES NOTHING HERE. FR-6 puts the choice with the
 * ingester, and this record is assembled by {@code DefaultIngest} from the
 * backend's own prices and capabilities. Nothing a subscriber passes to
 * {@code subscribe} reaches {@link FetchPolicy}.
 *
 * @param policy how a segment's size, its fan-out and the backend's
 *     capabilities pick a mode
 * @param capabilities the backend's, which is what rules `direct` out on a
 *     store that cannot presign
 * @param proxy where `proxy` gets bytes this pod does NOT hold -- ⚠️
 *     REQUIRED, and an earlier draft made it nullable so that a deployment
 *     without one could "refuse rather than degrade to `inline`". Nothing ever
 *     constructed it with {@code null}, so the refusal was an untested branch
 *     guarding a state no caller could reach, and the refusal itself was a
 *     {@code RuntimeException} that {@code DefaultIngest.pushLoop} swallows as
 *     a slow subscriber. Requiring it here makes the bad state unrepresentable
 *     instead -- non-negotiable 9, rung 1 -- and a segment this pod does not
 *     hold has no other byte source anyway
 */
public record SegmentServing(FetchPolicy policy, Capabilities capabilities, SegmentProxy proxy,
        GrantIssuer issuer) {

    /**
     * A serving path with no issuer, which is every deployment that has not
     * enabled {@code direct}.
     *
     * <p>⚠️ NULL IS THE RIGHT ABSENCE HERE, not a no-op issuer: a
     * {@code GrantIssuer} cannot be CONSTRUCTED against a backend that cannot
     * presign -- its constructor is criterion 7's startup refusal -- so a pod
     * on either shipping backend has no issuer to hold, and pretending
     * otherwise would need a fake that mints something.
     */
    public SegmentServing(FetchPolicy policy, Capabilities capabilities, SegmentProxy proxy) {
        this(policy, capabilities, proxy, null);
    }

    public SegmentServing {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(capabilities, "capabilities");
        Objects.requireNonNull(proxy, "proxy");
    }

    /** How large a hand-off this serving path makes, in bytes. */
    public int chunkBytes() {
        return proxy.chunkBytes();
    }
}
