// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * Everything the fetch-mode decision is allowed to depend on (M5.11).
 *
 * <p>⚠️ THERE IS NO MODE HERE, AND THAT ABSENCE IS THE POINT. FR-6 says the
 * INGESTER chooses; a consumer that could ask for {@code DIRECT} at fan-out 300
 * reproduces the $3,732/month design ADR-0004 rejected, and M5's SPEC names
 * "a policy that lets the consumer choose" as this row's falsifier. A
 * preference field on this record -- or an overload of {@link
 * FetchPolicy#modeFor} taking one -- is all it would take, so the absence is
 * asserted by {@code FetchPolicySeamTest} rather than left to review. Same
 * discipline as M5.9 deleting {@code Membership.inAz(String)}: the bad state
 * is unrepresentable, not discouraged.
 *
 * <p>⚠️ AND EVERY FIELD IS ALREADY KNOWN TO THE POD, which is what keeps
 * {@link FetchPolicy} free of a store, a socket and a clock (non-negotiable
 * 7). The size comes from the segment the pod just wrote or prefetched; the
 * AZ answer from {@code Membership.self()} against where the bytes are; the
 * fan-out from the subscriber registry; the pressure flag from the pod's own
 * load shedding. Nothing here needs a request to compute.
 *
 * @param batchBytes how many bytes would travel INLINE -- this stream's share
 *     of the commit, not the whole segment. ⚠️ ROUND-1 REVIEW CAUGHT THIS
 *     NAMED AND DOCUMENTED AS THE SEGMENT, which is a different number by
 *     three orders of magnitude and would have made criterion 4 true only
 *     inside the test file. A segment bundles ~1,600 runs and is 8 MiB by
 *     default ({@code IngestConfig}), up to ~32 MiB for one bulk body -- past
 *     BOTH inline bounds on every real flush, so {@code INLINE} would never
 *     have fired in production while every test passed. The contract decides
 *     on the batch: doc 04 §2c's event carries {@code byteStart}/{@code
 *     byteLen} plus an optional inline payload and reasons about "a trickle
 *     index's 8 KiB batch", and {@code Delivery} is already documented as
 *     "one stream's share of a commit"
 * @param bytesInServingAz whether the bytes are already in the serving pod's
 *     AZ -- its own buffer, or its AZ cache. ⚠️ Intra-AZ transfer is free and
 *     cross-AZ is not, which is the entire reason there are two inline
 *     thresholds instead of one (research doc 04, cost model R12)
 * @param segmentFanOut how many consumers in the serving pod's AZ want THIS
 *     SEGMENT. ⚠️ PER SEGMENT, NOT PER STREAM, and the name carries it because
 *     round-2 review found the two counts sitting adjacent in this record at
 *     different granularities with nothing to tell them apart. Doc 10 §4 says
 *     "the pod knows how many of its AZ's subscribers want a given SEGMENT",
 *     and that is this number. ⚠️ THE SUBSCRIBER LIST IS NOT IT:
 *     {@code SubscriptionHub.subscribers} is keyed per {@code RunKey}, so its
 *     size is the consumers of ONE STREAM -- about 1, since a shard has one
 *     primary per AZ. Reaching for it inside a per-run loop would answer
 *     {@code DIRECT} for every run of a cold segment: ~1,600 GETs for one
 *     8 MiB object against a budget of 1, which is one GET per shard per
 *     flush. M5's SPEC rejects that in as many words -- "fetch-on-demand per
 *     consumer ... makes the read rate scale with consumers, which is exactly
 *     what NFR-4 forbids". ⚠️ ONE, not zero, is the interesting value:
 *     catch-up replay
 * @param servingPodUnderPressure whether the pod is shedding load, which is
 *     the other condition research doc 04 gives for {@code DIRECT}
 */
public record SegmentDelivery(long batchBytes, boolean bytesInServingAz, int segmentFanOut,
        boolean servingPodUnderPressure) {

    public SegmentDelivery {
        if (batchBytes < 0) {
            throw new IllegalArgumentException(
                    "a batch of " + batchBytes + " bytes is not a batch");
        }
        if (segmentFanOut < 0) {
            throw new IllegalArgumentException(
                    "a fan-out of " + segmentFanOut + " is not a fan-out");
        }
    }
}
