// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.CostTable;

/**
 * The dials the fetch-mode policy reads. Validated at construction, never
 * later -- the shape {@link IngestConfig} establishes.
 *
 * <p>⚠️ THIS RECORD COMPUTES NOTHING AT DECISION TIME. Every value here is
 * settled once, at startup, so {@link FetchPolicy#modeFor} stays a pure
 * function of its arguments (non-negotiable 7). {@link #derivedFrom} is the
 * one place arithmetic happens and it runs at construction.
 *
 * <p>⚠️ THE CROSS-AZ CROSSOVER IS DERIVED, NOT A CONSTANT, and that is the
 * whole reason this record exists rather than three literals in the policy.
 * Cost model R12 defines it as **(one GET) / ($ per GB)** ≈ 19.5 KiB: below
 * it, shipping bytes across an AZ costs less than the GET it saves; above it,
 * send coordinates. A backend with a different GET price has a different
 * crossover, and {@link #derivedFrom} moves it.
 *
 * <p>⚠️ ONLY HALF OF IT COMES FROM {@link CostTable}, and M5's SPEC is amended
 * to say so (sdd.md rule 8) rather than worked around. {@code CostTable}
 * prices REQUESTS -- puts, gets, lists -- and holds no per-byte transfer
 * price, so the $/GB half is configuration. ⚠️ {@code CostTable} was
 * deliberately NOT extended to carry one: it is a component of {@code
 * Capabilities}, which is `wire-format-change` contract 5 read by every
 * backend, and cross-AZ egress is a property of the DEPLOYMENT rather than of
 * the store. Extending it would make every backend declare a number it does
 * not know, for a contract change nothing else needed.
 *
 * @param inlineCapBytes the ceiling on BOTH paths, and the largest segment
 *     inlined when the bytes are already in the serving AZ. It protects the
 *     push channel and the consumer's heap, neither of which cares which AZ
 *     the bytes came from.
 * @param crossAzCrossoverBytes an upper bound on the segment inlined when the
 *     bytes are only in another AZ -- the LARGEST such segment is
 *     {@code min(inlineCapBytes, crossAzCrossoverBytes)}, because the cap
 *     bounds both paths. Below this, shipping the bytes costs less than the
 *     GET it saves (cost model R12); above it the GET is cheaper.
 * @param directFanOutThreshold at or below this many consumers, a segment too
 *     large to inline is served by {@code DIRECT} rather than {@code PROXY}.
 *     ⚠️ CONFIGURATION, NOT A CONSTANT: the fan-out at which `direct` wins is
 *     measurement M3 and is deferred to M9, so this ships as a dial the same
 *     way M4 shipped the lease TTL. Defaults to 1 -- catch-up replay
 */
public record FetchPolicyConfig(long inlineCapBytes, long crossAzCrossoverBytes,
        int directFanOutThreshold) {

    /** Research doc 04's default: intra-AZ is free, so the cap is generous. */
    public static final long DEFAULT_INLINE_CAP_BYTES = 256L * 1024L;

    /**
     * $0.02 per GB, the figure cost model R12 derives the ≈19.5 KiB crossover
     * from, in micro-dollars.
     *
     * <p>⚠️ PER DECIMAL GB (10^9 bytes), not per GiB, because that is how the
     * bill is computed. Using 2^30 here would move the crossover to ~21.0 KiB
     * and silently disagree with the number every document in this repository
     * quotes.
     */
    public static final long DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB = 20_000L;

    public FetchPolicyConfig {
        if (inlineCapBytes <= 0) {
            throw new IllegalArgumentException(
                    "an inline cap of " + inlineCapBytes + " inlines nothing ever");
        }
        if (crossAzCrossoverBytes < 0) {
            throw new IllegalArgumentException(
                    "a negative cross-AZ crossover (" + crossAzCrossoverBytes + ") is not a size");
        }
        if (directFanOutThreshold < 0) {
            throw new IllegalArgumentException(
                    "a negative fan-out threshold (" + directFanOutThreshold + ") names no fan-out");
        }
    }

    /**
     * The configuration a backend's own prices imply.
     *
     * <p>⚠️ THE DERIVATION IS COST MODEL R12 AND NOTHING ELSE: crossover =
     * (one GET) / (price per byte). With {@code getPerThousand} in
     * micro-dollars per 1,000 requests and {@code crossAzMicroDollarsPerGb} in
     * micro-dollars per 10^9 bytes, that reduces to {@code getPerThousand *
     * 1_000_000 / crossAzMicroDollarsPerGb} -- the two thousands cancel. At
     * S3's $0.0004/1,000 GETs and $0.02/GB it gives exactly 20,000 bytes,
     * which is the 19.53 KiB the corpus quotes.
     *
     * <p>⚠️ A FREE BACKEND CROSSES OVER AT ZERO, and that is the right answer
     * rather than a degenerate one: {@link CostTable#free()} reports zeros for
     * the local-FS and in-memory backends, so a GET costs nothing and there is
     * never a reason to spend bytes to avoid one. Nothing is inlined
     * cross-AZ; the same-AZ cap is unaffected, which is the only path those
     * backends are ever on.
     *
     * @throws IllegalArgumentException if the transfer price is not positive --
     *     dividing by it is the derivation, and a zero would mean bytes move
     *     between AZs for free, which is the one thing ADR-0012 measured false
     */
    public static FetchPolicyConfig derivedFrom(CostTable costs, long crossAzMicroDollarsPerGb,
            long inlineCapBytes, int directFanOutThreshold) {
        if (crossAzMicroDollarsPerGb <= 0) {
            throw new IllegalArgumentException(
                    "cross-AZ transfer at " + crossAzMicroDollarsPerGb + " micro-dollars/GB would "
                            + "make bytes free to move between AZs -- ADR-0012 measures 419x");
        }
        // ⚠️ multiplyExact, not `*`. A GET price large enough to overflow is a
        // misconfiguration, and silently wrapping it would produce a NEGATIVE
        // crossover -- which the canonical constructor refuses, but with a
        // message blaming the crossover rather than the price that produced it.
        long crossover = Math.multiplyExact(costs.getPerThousand(), 1_000_000L)
                / crossAzMicroDollarsPerGb;
        return new FetchPolicyConfig(inlineCapBytes, crossover, directFanOutThreshold);
    }

    /** {@link #derivedFrom} with research doc 04's and cost model R12's defaults. */
    public static FetchPolicyConfig defaultsFor(CostTable costs) {
        return derivedFrom(costs, DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB,
                DEFAULT_INLINE_CAP_BYTES, 1);
    }
}
