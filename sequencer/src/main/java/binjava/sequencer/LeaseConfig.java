// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * The values a {@link LeaseManager} needs, validated once at construction.
 *
 * <p>⚠️ SEPARATE FROM THE PROTOCOL, and not only to make {@code LeaseManager}
 * fit under the file-size limit. These are about the VALUES — a pod name that
 * is present, durations that are positive and coherent with each other — and
 * none of them is about the store protocol that is the rest of that class.
 * What is checked here cannot be handed to the protocol wrong, which is rung 1
 * rather than a guard the protocol has to remember to run.
 *
 * <p>⚠️ BUT NOT EVERYTHING IS CHECKED, and the gap is worth naming rather than
 * leaving a reader to infer coverage from the paragraph above. {@code prefix}
 * is null-checked and nothing more, so {@code ""} is accepted and
 * {@link #leaseKey()} then returns {@code /ctl/lease/0.json} — the bucket root,
 * shared with every other cluster whose prefix is also unset, where two
 * clusters contend for one lease object and each can fence the other out of a
 * commit chain it has no relationship with. Exactly the fail-late deployment
 * error the {@code podId} guard below exists to close, one field over. M4.3o
 * owns it; this task is a refactor and adding a guard here would make it a
 * behaviour change.
 *
 * <p>⚠️ TTL and the renew interval are CONFIGURATION, not constants.
 * Measurement M1 settles their values at M8 from real GC-pause data, and this
 * milestone must not hardcode what M8 will measure.
 *
 * @param prefix the object-store prefix this cluster's objects live under
 * @param podId which node this is — ⚠️ a wire value, written into the lease
 * @param endpoint where peers reach this node's sequencer
 * @param ttl how long a lease stays valid without renewal
 * @param renewInterval how often the holder should renew
 */
public record LeaseConfig(String prefix, String podId, String endpoint,
        Duration ttl, Duration renewInterval) {

    public LeaseConfig {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(renewInterval, "renewInterval");
        if (podId.isBlank()) {
            // ⚠️ EARLY. An unset POD_NAME otherwise constructs fine and fails
            // at the first acquire, as an unchecked IllegalArgumentException
            // out of Lease's constructor, from a method declared `throws
            // IOException` -- the fail-late class M4.3's own review closed
            // inside `Lease.decode`.
            throw new IllegalArgumentException("podId is never blank");
        }
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }
        if (renewInterval.isZero() || renewInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "renewInterval must be positive: " + renewInterval);
        }
        if (renewInterval.compareTo(ttl) >= 0) {
            // ⚠️ Renewing no more often than the lease lives guarantees losing
            // it. ⚠️ This bound is NECESSARY, not sufficient: at ttl=10s,
            // renewInterval=9s is accepted and there a single missed renew is
            // still terminal. The corpus's TTL/3 is what makes one miss
            // survivable, but the RATIO is measurement M1's to settle at M8 --
            // so this refuses the incoherent case and leaves the policy to the
            // milestone that has the data.
            throw new IllegalArgumentException(
                    "renewInterval must be shorter than ttl: " + renewInterval + " >= " + ttl);
        }
    }

    /**
     * The object a manager under this configuration contends for.
     *
     * <p>⚠️ Slot 0 because ADR-0007 fixes S = 1 — but the slot is IN THE KEY,
     * so raising S is configuration rather than a key-grammar change. Same
     * reason CommitLog's chain path has carried slot 0 since M1.
     *
     * <p>⚠️ {@code Locale.ROOT}: an object key is a wire value and must not
     * depend on the process's locale.
     */
    public String leaseKey() {
        return String.format(Locale.ROOT, "%s/ctl/lease/%d.json", prefix, 0);
    }
}
