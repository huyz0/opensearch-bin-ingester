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

    /** No provider, no bill — the local-FS and in-memory backends. */
    public static CostTable free() {
        return new CostTable(0, 0, 0);
    }
}
