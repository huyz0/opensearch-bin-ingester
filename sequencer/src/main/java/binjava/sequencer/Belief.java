// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.Version;
import binjava.format.Lease;
import java.util.Objects;

/**
 * What this instance believes, as ONE value.
 *
 * <p>⚠️ Three separate fields let a state exist that means nothing —
 * ambiguity over a belief that is not held — and M4.3d had to keep the
 * three in step by hand at six assignment sites. Here {@code null} is the
 * whole of "holds nothing", so there is no such state to maintain.
 *
 * @param version the version the term was last observed at, or NULL when
 *     the acquiring write's outcome was never observed at all — see
 *     {@link #verified()}
 * @param ambiguous the last conditional write's outcome is UNKNOWN, so
 *     {@code version} may be stale, or absent — see {@link LeaseManager#renew()}
 */
record Belief(Lease lease, Version version, boolean ambiguous) {

    /**
     * ⚠️ THE COUPLING IS ENFORCED, not documented. This record's own javadoc
     * says three loose fields "let a state exist that means nothing" — and
     * {@code (lease, null, false)} is exactly such a state: a version-less
     * belief that does not know it is uncertain. The write sites gate on
     * {@code ambiguous()}, not on {@code verified()}, so that state reaches
     * {@code putIfMatch} as a null version and throws an unchecked
     * {@code NullPointerException} out of {@code release()} — the SIGTERM
     * path, where the pod then exits without writing the expired lease and the
     * successor waits out a full TTL.
     *
     * <p>⚠️ Rung 1 of the {@code gate-design} ladder: a test notices this only
     * if someone writes one, and the constructor notices it always.
     */
    Belief {
        Objects.requireNonNull(lease, "lease");
        if (version == null && !ambiguous) {
            throw new IllegalArgumentException(
                    "a belief with no version has not been verified, so it is always ambiguous");
        }
    }

    Belief uncertain() {
        return new Belief(lease, version, true);
    }

    /**
     * The candidate term of an acquisition whose outcome was never
     * observed (M4.3g).
     *
     * <p>⚠️ There is no version, because no response came back — so this
     * is strictly weaker than {@link #uncertain()}, where a version was
     * once known and may merely be stale.
     */
    static Belief unverified(Lease candidate) {
        return new Belief(candidate, null, true);
    }

    /**
     * Whether this term was ever OBSERVED to be held.
     *
     * <p>⚠️ "My write may have landed" is not holding the term, and
     * {@code held()} must keep saying empty for it — M4.3e's property is
     * that {@code held()} never reports a term this instance does not
     * hold.
     */
    boolean verified() {
        return version != null;
    }
}
