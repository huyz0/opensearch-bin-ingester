// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import binjava.format.Lease;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Acquires, renews and releases {@code <prefix>/ctl/lease/<slot>.json} — the
 * CAS'd read/write side of {@link Lease}, which is pure encode/decode. The
 * same split {@code IndexOrdinalRegistry} has from {@code IndexRegistry}, and
 * this class is the thing that actually touches {@link BinStore}.
 *
 * <p>⚠️ THE LEASE IS LIVENESS AND EFFICIENCY, NOT SAFETY (ADR-0002). Safety
 * comes from the epoch in the object path plus the write-once chain, so a
 * clock skew that expires a lease early costs a failover, never a lost write.
 * That is why the corpus says to challenge aggressively: the cost of being
 * wrong is bounded.
 *
 * <p>⚠️ TWO ACQUISITION PATHS, and they are not interchangeable. The FIRST
 * acquisition uses {@code putIfAbsent}, because {@code putIfMatch} on an
 * absent key <em>throws</em> — the SPI states that explicitly, and
 * deliberately does not fold it into the lost-race empty {@code Optional}
 * ("there is no version to have moved from"). Every later transition —
 * taking over an expired lease, renewing, releasing — is {@code putIfMatch}.
 *
 * <p>⚠️ RELEASE DOES NOT DELETE THE OBJECT. Deleting it would lose the epoch
 * counter, so the next acquirer would start again at the first term and REUSE an
 * epoch a previous term already wrote objects under — exactly what fencing
 * exists to prevent. (The first term is 1, not 0: epoch 0 is reserved for the
 * unleased chain, M4.4b.) Release writes an already-expired lease instead, keeping the
 * counter and making the failover immediate rather than TTL-bound.
 *
 * <p>⚠️ NOT THREAD-SAFE BY ACCIDENT — {@code synchronized}, for the same
 * reason {@code IndexOrdinalRegistry} is: the read-decide-CAS cycle must not
 * interleave with itself.
 *
 * <p>⚠️ TTL and the renew interval are CONFIGURATION, not constants.
 * Measurement M1 settles their values at M8 from real GC-pause data, and this
 * milestone must not hardcode what M8 will measure.
 */
public final class LeaseManager {

    private final BinStore store;
    private final String key;
    private final String podId;
    private final String endpoint;
    private final Duration ttl;
    private final Duration renewInterval;
    private final Clock clock;

    private Lease held;
    private Version heldVersion;

    public LeaseManager(BinStore store, String prefix, String podId, String endpoint,
            Duration ttl, Duration renewInterval, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(prefix, "prefix");
        this.podId = Objects.requireNonNull(podId, "podId");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.renewInterval = Objects.requireNonNull(renewInterval, "renewInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
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
        // ⚠️ Slot 0 because ADR-0007 fixes S = 1 — but the slot is IN THE KEY,
        // so raising S is configuration rather than a key-grammar change. Same
        // reason CommitLog's chain path has carried slot 0 since M1.
        // ⚠️ Locale.ROOT: an object key is a wire value and must not depend on
        // the process's locale.
        this.key = String.format(Locale.ROOT, "%s/ctl/lease/%d.json", prefix, 0);
    }

    /** The object this manager contends for. */
    public String key() {
        return key;
    }

    /** What this instance believes it holds, or empty if it holds nothing. */
    public synchronized Optional<Lease> held() {
        return Optional.ofNullable(held);
    }

    /** How often the holder should renew — configuration, see the class javadoc. */
    public Duration renewInterval() {
        return renewInterval;
    }

    /**
     * Takes the lease if it is unheld or expired.
     *
     * @return the lease now held, or empty if someone else holds an unexpired
     *     one or won the race for it
     * @throws IOException the store was unreachable, or the lease object could
     *     not be parsed — ⚠️ NOT treated as unheld, see below
     */
    public synchronized Optional<Lease> tryAcquire() throws IOException {
        Optional<ObjectStat> stat = store.stat(key);
        if (stat.isEmpty()) {
            // ⚠️ putIfAbsent, not putIfMatch — see the class javadoc.
            // ⚠️ EPOCH 1, not 0 (M4.4b). Epoch 0 is RESERVED for "no lease":
            // `CommitLog`'s 2-arg constructor writes it for every caller that
            // has no lease yet, `DefaultIngest` among them. If the first
            // leadership term were also 0, wiring the lease into the commit
            // path would put a first leader's chain byte-identical to what
            // those callers already write — `putIfAbsent` would still buy I1,
            // but I3's "readers of the new epoch never look there" would be
            // VOID, because it would not be a different epoch. Reserving 0
            // makes every leased chain provably disjoint from the unleased one.
            Lease fresh = new Lease(1, podId, endpoint, clock.millis() + ttl.toMillis());
            return adopt(fresh, store.putIfAbsent(key, Body.ofBytes(fresh.encode())));
        }
        // ⚠️ A corrupt lease propagates as IOException rather than being
        // treated as unheld. Taking over bytes nobody can parse is how two
        // nodes end up sequencing at once.
        Lease current = read();
        if (!current.isExpiredAt(clock.millis())) {
            return Optional.empty();
        }
        Lease taken = current.takenOverBy(podId, endpoint, clock.millis() + ttl.toMillis());
        return adopt(taken, store.putIfMatch(key, Body.ofBytes(taken.encode()),
                stat.get().version()));
    }

    /**
     * Extends this instance's own term.
     *
     * @return the renewed lease, or empty if this instance does not hold one or
     *     has been fenced — ⚠️ an empty result is how a holder LEARNS it is
     *     fenced, and it must then stop sequencing immediately. It never
     *     consults a clock to decide that
     */
    public synchronized Optional<Lease> renew() throws IOException {
        if (held == null) {
            return Optional.empty();
        }
        Lease renewed = held.renewedUntil(clock.millis() + ttl.toMillis());
        // ⚠️ ONE conditional write, with no re-read: the holder already knows
        // its own version. A read per renew would double the lease's request
        // rate for nothing.
        Optional<Version> won = store.putIfMatch(key, Body.ofBytes(renewed.encode()), heldVersion);
        if (won.isEmpty()) {
            // ⚠️ Fenced. Drop the local belief too, so a caller that ignores
            // the empty result cannot keep renewing against a stale version.
            held = null;
            heldVersion = null;
            return Optional.empty();
        }
        held = renewed;
        heldVersion = won.get();
        return Optional.of(renewed);
    }

    /**
     * Gives up the lease voluntarily, so a successor need not wait out the TTL.
     *
     * <p>⚠️ Writes an EXPIRED lease rather than deleting the object — see the
     * class javadoc for why deleting would break fencing. A no-op when nothing
     * is held (a {@code SIGTERM} on a non-holder is entirely normal), and
     * harmless when already fenced, because the conditional write simply loses.
     */
    public synchronized void release() throws IOException {
        if (held == null) {
            return;
        }
        // ⚠️ Expiry is INCLUSIVE, so `now` is already expired.
        Lease expired = held.renewedUntil(clock.millis());
        store.putIfMatch(key, Body.ofBytes(expired.encode()), heldVersion);
        held = null;
        heldVersion = null;
    }

    private Lease read() throws IOException {
        try (InputStream in = store.get(key)) {
            return Lease.decode(in.readAllBytes());
        }
    }

    private Optional<Lease> adopt(Lease candidate, Optional<Version> won) {
        if (won.isEmpty()) {
            // ⚠️ Drop any prior belief too, exactly as `renew` does. Leaving a
            // superseded lease in `held` would make `held()` report a term
            // this instance does not hold.
            held = null;
            heldVersion = null;
            return Optional.empty();
        }
        held = candidate;
        heldVersion = won.get();
        return Optional.of(candidate);
    }
}
