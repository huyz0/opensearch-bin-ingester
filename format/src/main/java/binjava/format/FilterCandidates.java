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
     * @param budgetBytes how many bytes the FILTER component may occupy.
     *     ⚠️ M2.8 (round-1 test review): this was a fixed 900-byte constant
     *     here, chosen once against research doc 02 §0's ASSUMED ~90-byte
     *     fixed key overhead -- reproducibly wrong for a real, unremarkable
     *     segment whose prefix/pod id push the true fixed part past that
     *     assumption, at index counts nowhere near this project's 10,000
     *     pathological scale. Callers building a real key must compute the
     *     true remaining budget from that key's own fields (see {@link
     *     SegmentKey#filterBudgetBytes}), never assume one.
     */
    public static MembershipFilter chooseShortestFitting(
            Set<Integer> memberOrdinals, int totalRegisteredIndices, int budgetBytes) {
        if (memberOrdinals.isEmpty()) {
            return new MembershipFilter.None();
        }
        if (memberOrdinals.size() == totalRegisteredIndices) {
            // ⚠️ Unconditional: A's payload is the single character "A", so
            // it fits any budgetBytes >= 1 a real key could ever compute (a
            // budget below that would mean the FIXED part of the key alone
            // already exceeds MAX_KEY_BYTES -- a configuration error key()
            // itself refuses, not something a filter choice could fix).
            return new MembershipFilter.All();
        }

        BitSet bits = new BitSet();
        for (int ordinal : memberOrdinals) {
            bits.set(ordinal);
        }

        MembershipFilter.ExactBitmap exact = new MembershipFilter.ExactBitmap(bits);
        if (fits(exact, budgetBytes)) {
            return exact;
        }
        MembershipFilter.RunLength runLength = new MembershipFilter.RunLength(bits);
        if (fits(runLength, budgetBytes)) {
            return runLength;
        }

        int n = memberOrdinals.size();
        int m = (int) Math.ceil(n * BLOOM_BITS_PER_ITEM);
        MembershipFilter.Bloom bloom = MembershipFilter.Bloom.of(memberOrdinals, m, BLOOM_K);
        if (fits(bloom, budgetBytes)) {
            return bloom;
        }

        return new MembershipFilter.None();
    }

    private static boolean fits(MembershipFilter filter, int budgetBytes) {
        return filter.encode().getBytes(StandardCharsets.UTF_8).length <= budgetBytes;
    }
}
