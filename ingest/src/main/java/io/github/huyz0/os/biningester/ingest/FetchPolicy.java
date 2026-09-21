// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.format.FetchMode;
import java.util.Objects;

/**
 * Which {@link FetchMode} the INGESTER picks for one delivery (FR-6, M5.11).
 *
 * <p>⚠️ THE INGESTER CHOOSES AND THE CONSUMER CANNOT ASK. FR-6 says so, and
 * here it is unrepresentable rather than documented: {@link SegmentDelivery}
 * carries no mode and no preference, and no method below takes one.
 * {@code FetchPolicySeamTest} fails if either changes. ADR-0004 priced the
 * alternative -- a consumer demanding {@code DIRECT} at fan-out 300 -- at
 * $3,732/month.
 *
 * <p>⚠️ A PURE FUNCTION, holding no store, no socket and no clock
 * (non-negotiable 7). Everything the decision needs is already in the pod's
 * hands: the size of the batch it is about to push, whether the bytes are in its
 * own AZ, how many consumers are waiting, and whether it is shedding load.
 * A policy that had to ask the store how big a segment was would have moved
 * this decision into a layer that needs a fixture to test.
 *
 * <p>⚠️ ORDER IS LOAD-BEARING AND INLINE COMES FIRST. Checking the
 * {@code DIRECT} conditions before the inline bound would answer {@code
 * DIRECT} for every catch-up replay, including an 8 KiB one -- making the
 * escape hatch the default by ordering rather than by intent, which is M5's
 * SPEC's named failure mode for this row. Inline costs the consumer NO
 * request; direct costs one. So:
 *
 * <ol>
 *   <li>small enough to inline, against the bound for where the bytes
 *       are -> {@code INLINE};</li>
 *   <li>otherwise, if the backend can sign and EITHER the bytes are COLD and
 *       the segment fan-out is at or below the configured threshold (catch-up
 *       replay), OR the pod is shedding -> {@code DIRECT};</li>
 *   <li>otherwise {@code PROXY}, which costs the consumer nothing and the pod
 *       a stream it does not buffer (M5.12).</li>
 * </ol>
 */
public final class FetchPolicy {

    private final FetchPolicyConfig config;

    public FetchPolicy(FetchPolicyConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /** The configuration this policy decides against. */
    public FetchPolicyConfig config() {
        return config;
    }

    /**
     * Which mode serves {@code delivery} against a backend with
     * {@code capabilities}.
     *
     * <p>⚠️ {@code capabilities} IS READ FOR EXACTLY ONE THING: whether the
     * backend can presign. Without it the policy would name {@code DIRECT} for
     * every catch-up replay in the tree today, because neither shipping
     * backend can sign (ADR-0041) -- a mode nothing can serve, discovered at
     * the fetch rather than at the decision.
     */
    public FetchMode modeFor(SegmentDelivery delivery, Capabilities capabilities) {
        Objects.requireNonNull(delivery, "delivery");
        Objects.requireNonNull(capabilities, "capabilities");

        // ⚠️ TWO BOUNDS, NOT ONE, and which applies is a cost question rather
        // than a size one. Intra-AZ transfer is FREE, so the same-AZ cap
        // protects the push channel and the consumer's heap; cross-AZ is
        // billed, so that bound is cost model R12's crossover -- below it,
        // shipping the bytes costs less than the GET it saves. Collapsing them
        // to one number would either pay a fetch to avoid free bytes or inline
        // 100 MiB/s cross-AZ at ~$340/day.
        // ⚠️ THE CAP BOUNDS BOTH PATHS, and the billed path takes whichever
        // of the two bounds is smaller. The cap protects the push channel and the consumer's heap,
        // and neither cares which AZ the bytes came from -- so applying it to
        // the same-AZ path alone inverted the order whenever a low transfer
        // price derived a crossover above it. MEASURED at 1,000
        // micro-dollars/GB: a 400,000-byte crossover against a 262,144 cap
        // inlined 300 KiB when the bytes had to CROSS an AZ and answered
        // `PROXY` when they were local and FREE.
        long inlineBound = delivery.bytesInServingAz()
                ? config.inlineCapBytes()
                : Math.min(config.inlineCapBytes(), config.crossAzCrossoverBytes());
        if (delivery.batchBytes() <= inlineBound) {
            return FetchMode.INLINE;
        }

        if (capabilities.presignedUrls() && wantsDirect(delivery)) {
            return FetchMode.DIRECT;
        }
        return FetchMode.PROXY;
    }

    /**
     * The two conditions research doc 04 gives for {@code DIRECT}, and it is
     * an OR rather than an AND.
     *
     * <p>⚠️ "catch-up with fan-out 1, or the pod shedding load" -- a policy
     * implementing only the fan-out half is right on every ordinary delivery
     * and leaves a pod under pressure with no way to shed.
     *
     * <p>⚠️ THE PRESSURE ARM IS UNCONDITIONAL AND THE FAN-OUT ARM IS NOT, which
     * is doc 10 §4's table rather than an asymmetry invented here: its
     * load-shedding row reads fan-out "any", because a pod that is queueing
     * would rather pay a GET than keep the queue.
     *
     * <p>⚠️ A THRESHOLD OF ZERO TURNS THE FAN-OUT ARM OFF ENTIRELY, which is
     * what the {@code > 0} guard buys. ⚠️ IT IS NOT THE OPERATOR'S OFF SWITCH,
     * which this paragraph called it until M5.43: the PRESSURE arm above
     * returns before the threshold is ever consulted, so zeroing the dial left
     * {@code direct} reachable the moment the pod shed load.
     * {@code directEnabled} is the off switch, and it is checked ahead of both
     * arms. What zeroing the dial turns off is this arm, and only this arm:
     * without it, a threshold of 0 would still match a fan-out of 0 through
     * {@code 0 <= 0}, so setting the dial to "never" would elect {@code DIRECT}
     * for exactly the deliveries nobody is subscribed to. ⚠️ Under a threshold
     * of ONE, a fan-out of 0 DOES qualify, and that is deliberate rather than
     * overlooked: 0 is below 1, and a delivery with no subscriber is a
     * delivery nobody is served, so the mode chosen for it is moot. An earlier
     * draft of this paragraph read as though fan-out 0 never elected anything,
     * which review pointed out is the opposite of what the code does.
     */
    private boolean wantsDirect(SegmentDelivery delivery) {
        // ⚠️ BEFORE THE PRESSURE ARM, AND THAT ORDER IS THE POINT (M5.43). A
        // deployment says whether it does `direct` AT ALL; everything below is
        // the policy deciding when, and none of it should run for a deployment
        // that never asked. Putting this check after the pressure arm -- where
        // the fan-out dial sits -- is the defect this row exists to fix:
        // `directFanOutThreshold = 0` reads like an off switch and is not one,
        // because pressure returns true before the threshold is consulted, so
        // an operator who zeroed the dial still gets a signed URL the moment
        // the pod sheds load.
        //
        // ⚠️ AND IT IS WHAT GIVES M5.13's STARTUP REFUSAL A CALLER. If this is
        // false the policy never answers DIRECT, so no `GrantIssuer` is needed
        // and a pod on a backend that cannot sign starts normally; if it is
        // true, `DefaultIngest` builds one and the refusal happens at startup
        // rather than at the first fetch.
        if (!config.directEnabled()) {
            return false;
        }
        if (delivery.servingPodUnderPressure()) {
            return true;
        }
        // ⚠️ AND THE BYTES MUST BE COLD. Research doc 10 §4 writes the rule as
        // an iff -- "redirect iff N == 1 AND THE SEGMENT IS NOT ALREADY
        // CACHED" -- and round-1 review found this conjunct missing. Dropping
        // it is not a nuance: the pod that WROTE the segment still holds it,
        // so an 8 MiB flush served to a stream whose shard lives on one node
        // in that AZ has fanOut 1, and the policy would send a signed URL for
        // bytes it could stream from RAM for zero requests. That is ONE GET
        // PER STREAM PER FLUSH -- a rate scaling with streams, which
        // non-negotiable 6 forbids by name -- and it turns M5's budgeted
        // "2 GETs/segment at 3 AZs, flat in node count" into 2 plus one per
        // subscribing node. Only `presignedUrls=false` on both shipping
        // backends kept it out of the tree.
        return !delivery.bytesInServingAz()
                && config.directFanOutThreshold() > 0
                && delivery.segmentFanOut() <= config.directFanOutThreshold();
    }
}
