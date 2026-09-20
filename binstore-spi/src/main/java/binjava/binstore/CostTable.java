// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

/**
 * What a backend's requests cost, in micro-dollars per 1,000 requests.
 *
 * <p>⚠️ Present from the FIRST version of {@link Capabilities}, not added later.
 * Re-adding a component to a record changes its canonical constructor, which is
 * a signature change rather than an addition — and M1.3's cost meter is the very
 * next task that needs it.
 *
 * <p>⚠️ Micro-dollars per 1,000 because the real numbers are fractions of a cent
 * and floating point has no place in a budget that gates a commit. A local
 * filesystem reports zeros: modelled, not billed.
 */
public record CostTable(long putPerThousand, long getPerThousand, long listPerThousand) {

    /**
     * Research 02 §1's AWS us-east-1 S3 Standard prices, the DEFAULT the cost
     * meter models with when a deployment names none.
     *
     * <p>⚠️ A DEFAULT, NOT A CONSTANT THE CODE IS BUILT ON. Every provider has
     * this shape with different numbers, and research 02 §1 says not to
     * hard-code AWS ratios; this factory exists so the prices are written down
     * once, with their source, rather than appearing as literals wherever a
     * bill is modelled.
     *
     * <p>⚠️ A LIST COSTS WHAT A PUT COSTS — 12.5 GETs. That is the single most
     * consequential ratio in the cost model (cost rules 2 and 15), and it is
     * why the two are separate components rather than one "write" price.
     */
    public static CostTable awsS3Standard() {
        return new CostTable(5_000L, 400L, 5_000L);
    }

    /** No provider, no bill — the local-FS and in-memory backends. */
    public static CostTable free() {
        return new CostTable(0, 0, 0);
    }
}
