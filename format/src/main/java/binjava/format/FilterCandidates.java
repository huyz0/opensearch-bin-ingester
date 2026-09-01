// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Set;

/**
 * Picks the shortest-fitting membership filter for one segment (ADR-0003;
 * research doc 02 §7). Pure: given the segment's own distinct-index ordinals
 * and how many indices are registered in total, no I/O.
 *
 * <p>⚠️ CHEAPEST-FIRST ORDER, per the design: {@code A} costs nothing beyond a
 * size comparison; {@code Z}/{@code R} need the same bitmap either way;
 * {@code B} is tried only once the exact encodings are known not to fit.
 * {@code N} is the honest answer when nothing fits, never a thrown exception —
 * a segment touching all 10,000 registered indices is exactly the case
 * NFR-12 (M2.8) exists to prove degrades cleanly rather than overflows.
 */
public final class FilterCandidates {

    private FilterCandidates() {}

    /** research doc 02 §5: ~900 characters left of the 1024-byte key budget for the filter. */
    static final int BUDGET_BYTES = 900;

    /** research doc 02 §3: 10% target FPR at this project's expected scale, ~4.79 bits/item, k=3. */
    private static final double BLOOM_BITS_PER_ITEM = 4.79;
    private static final int BLOOM_K = 3;

    /**
     * @param memberOrdinals the distinct index ordinals actually present in
     *     this segment. Never empty in practice (a segment always carries at
     *     least one index's records), but an empty set degrades to {@code N}
     *     rather than throwing.
     * @param totalRegisteredIndices how many indices are registered in total
     *     (typically the caller's locally cached count -- see {@code
     *     IndexOrdinalRegistry#registeredCount()}). A count that is too LOW
     *     can only make {@code A} wrongly chosen when it should not have
     *     been, which is safe: {@code A} answers "might be present" for
     *     everything, so this is a false POSITIVE, the same error class a
     *     Bloom filter already tolerates by design -- never a false negative
     *     for an index actually present.
     */
    public static MembershipFilter chooseShortestFitting(
            Set<Integer> memberOrdinals, int totalRegisteredIndices) {
        if (memberOrdinals.isEmpty()) {
            return new MembershipFilter.None();
        }
        if (memberOrdinals.size() == totalRegisteredIndices) {
            return new MembershipFilter.All();
        }

        BitSet bits = new BitSet();
        for (int ordinal : memberOrdinals) {
            bits.set(ordinal);
        }

        MembershipFilter.ExactBitmap exact = new MembershipFilter.ExactBitmap(bits);
        if (fits(exact)) {
            return exact;
        }
        MembershipFilter.RunLength runLength = new MembershipFilter.RunLength(bits);
        if (fits(runLength)) {
            return runLength;
        }

        int n = memberOrdinals.size();
        int m = (int) Math.ceil(n * BLOOM_BITS_PER_ITEM);
        MembershipFilter.Bloom bloom = MembershipFilter.Bloom.of(memberOrdinals, m, BLOOM_K);
        if (fits(bloom)) {
            return bloom;
        }

        return new MembershipFilter.None();
    }

    private static boolean fits(MembershipFilter filter) {
        return filter.encode().getBytes(StandardCharsets.UTF_8).length <= BUDGET_BYTES;
    }
}
