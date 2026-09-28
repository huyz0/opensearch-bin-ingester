// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
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

    /**
     * The serving path a pod builds from its config and its backend's own
     * prices and capabilities (moved out of {@code DefaultIngest} by M11.24a).
     */
    static SegmentServing forPod(IngestConfig config, BinStore store, IndexCostLedger costLedger) {
        Capabilities storeCapabilities = store.capabilities();
        // ⚠️ THE STARTUP REFUSAL, AND M5.43 IS WHAT GAVE IT A CALLER. Criterion
        // 7 asks that a deployment wanting `direct` against a backend that
        // cannot sign fail at STARTUP rather than at the first fetch, and until
        // this line nothing expressed "this deployment wants direct" -- so the
        // refusal `Capabilities.requirePresignedUrls` implements had no call
        // site anywhere in the tree.
        //
        // ⚠️ CONDITIONAL, NECESSARILY. Calling it unconditionally fails every
        // pod to start on both shipping backends, neither of which presigns;
        // calling it lazily puts the refusal back at the first fetch, which is
        // what it exists to prevent.
        if (config.directEnabled()) {
            storeCapabilities.requirePresignedUrls();
        }
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(
                        storeCapabilities.costs(), config.directEnabled())),
                storeCapabilities,
                // ⚠️ THE CACHE IS ON IN PRODUCTION, which is what makes
                // M5.40b a number rather than a capability. A repeat read
                // across publishes -- a late subscriber, an AZ replaying a
                // backlog -- costs no GET.
                new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES,
                        // ⚠️ FROM THE CONFIG, NOT FROM THE DEFAULT CONSTANT.
                        // A deployment that configures a larger segment than
                        // the default would otherwise exceed a fixed ceiling
                        // with EVERY segment, cache nothing, and say nothing.
                        SegmentCache.forSegmentsOf(config.maxSegmentBytes()), costLedger),
                // ⚠️ ONCE PER POD, NOT ONCE PER PUBLISH. An issuer per publish
                // would allocate on the serving path for every flush, and it
                // would put the TTL ceiling's configuration in a loop rather
                // than at one site. ⚠️ AND NULL WHEN `direct` IS OFF, because
                // the constructor REFUSES a backend that cannot presign -- so
                // over a backend that cannot sign there is no issuer to hold, which
                // is the same refusal the line above already made.
                config.directEnabled() ? new GrantIssuer(store) : null);
    }

    /** How large a hand-off this serving path makes, in bytes. */
    public int chunkBytes() {
        return proxy.chunkBytes();
    }
}
