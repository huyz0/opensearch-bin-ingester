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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
 * <p>⚠️ AN AMBIGUOUS CONDITIONAL WRITE IS NOT A FAILED ONE. A {@code putIfMatch}
 * that returns empty definitively lost; one that THROWS may have landed and
 * lost only its response, leaving the cached {@link Version} stale. Treating
 * that as "fenced" would make a healthy leader stand down, and treating it as
 * "renewed" would let a genuinely fenced one keep sequencing — so it is treated
 * as neither: the belief is kept, a flag is set, and the next call re-reads
 * before trusting its version. See {@code refreshed}.
 *
 * <p>⚠️ THAT COVERS {@code renew} AND {@code release} ONLY, and the gap is
 * bigger than the one it closes rather than smaller. An ambiguous
 * <em>acquisition</em> is still treated as a flat failure: {@code tryAcquire}
 * lets the {@code IOException} escape before {@code adopt} runs, so a node
 * whose acquiring write LANDED holds the lease on the store and cannot
 * discover it — its own {@code tryAcquire} refuses because what it reads is
 * unexpired, and {@code renew} refuses because it believes it holds nothing.
 * The cluster then has no sequencer for a full TTL and that node recovers only
 * by burning an epoch, where an ambiguous renew now recovers on the very next
 * call. M4.3g owns it. ⚠️ Do not read this class as ambiguity-safe.
 *
 * <p>⚠️ And the refresh NARROWS the renew window rather than closing it: it
 * gets one attempt, so a write still queued inside the object store when
 * {@code refreshed} does its {@code stat} lands afterwards, and the next
 * conditional write loses with the flag already cleared — the original
 * self-fence, from a rarer starting point. Safety is untouched either way,
 * because a lost CAS is always the safe direction.
 *
 * <p>⚠️ NOT THREAD-SAFE BY ACCIDENT — a lock, for the same reason
 * {@code IndexOrdinalRegistry} needs one: the read-decide-CAS cycle must not
 * interleave with itself.
 *
 * <p>⚠️ A {@code ReentrantLock} RATHER THAN A MONITOR, because the lock is held
 * across object-store I/O and java-style.md rule 6 says so. A monitor cannot be
 * given up: one hung PUT would park every other caller with no timeout and no
 * interruptibility. The bound is {@code renewInterval} — derived, not a new
 * constant — because waiting longer than one renew interval for the lock
 * guarantees missing the renew anyway, so there is nothing to buy by waiting
 * longer.
 *
 * <p>⚠️ {@code held()} TAKES NO LOCK AT ALL. It is what a caller gates on, so
 * queueing it behind a write in flight would make "am I the leader?" hang on a
 * slow PUT. The belief is one immutable {@link Belief} in a volatile field,
 * swapped whole — which also makes the old three-field invariant
 * ("{@code held == null} implies {@code ambiguous == false}") unrepresentable
 * rather than merely maintained.
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

    /**
     * What this instance believes, as ONE value.
     *
     * <p>⚠️ Three separate fields let a state exist that means nothing —
     * ambiguity over a belief that is not held — and M4.3d had to keep the
     * three in step by hand at six assignment sites. Here {@code null} is the
     * whole of "holds nothing", so there is no such state to maintain.
     *
     * @param ambiguous the last conditional write's outcome is UNKNOWN, so
     *     {@code version} may be stale — see {@link #renew()}
     */
    private record Belief(Lease lease, Version version, boolean ambiguous) {

        Belief uncertain() {
            return new Belief(lease, version, true);
        }
    }

    /** ⚠️ VOLATILE, so {@link #held()} can read it without taking the lock. */
    private volatile Belief belief;

    private final ReentrantLock lock = new ReentrantLock();

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

    /**
     * What this instance believes it holds, or empty if it holds nothing.
     *
     * <p>⚠️ TAKES NO LOCK. A caller gates on this, and a hung PUT elsewhere
     * must not make asking hang too — see the class javadoc.
     */
    public Optional<Lease> held() {
        Belief b = belief;
        return b == null ? Optional.empty() : Optional.of(b.lease());
    }

    /**
     * Takes the lock, or reports why not.
     *
     * <p>⚠️ AN IOException, never a quiet {@code false}. {@code tryAcquire} and
     * {@code renew} both have a "nothing happened" return value that means
     * something ELSE — empty from {@code renew} means FENCED, and a caller must
     * stop sequencing on it — so a lock timeout must not borrow that signal.
     * Failing to take a lock held across object-store I/O is evidence of slow
     * I/O, not of having lost the lease.
     *
     * <p>⚠️ THE BOUND IS THE CALLER'S, because the right one is not the same for
     * all three. For {@code tryAcquire} and {@code renew} it is
     * {@code renewInterval}: waiting longer than one renew interval guarantees
     * missing the renew anyway, and giving up early costs a retry. For
     * {@code release} that argument INVERTS — giving up early costs exactly the
     * thing release exists to buy — so its bound is the {@code ttl}, which is
     * what a successor pays when the release does not happen. Waiting up to
     * that is never worse than not waiting.
     *
     * <p>⚠️ Nanoseconds, so a sub-millisecond bound is not truncated to a
     * single non-blocking attempt while the message quotes the duration asked
     * for.
     */
    private void lockOrFail(String what, Duration bound) throws IOException {
        boolean taken;
        try {
            taken = lock.tryLock(bound.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for the lease lock to " + what, e);
        }
        if (!taken) {
            throw new IOException("could not take the lease lock to " + what
                    + " within " + bound + "; a store call is still in flight");
        }
    }

    /** How often the holder should renew — configuration, see the class javadoc. */
    public Duration renewInterval() {
        return renewInterval;
    }

    /**
     * Takes the lease if it is unheld or expired.
     *
     * @return the lease now held, or empty — ⚠️ WHICH IS NOT THE SAME AS "not
     *     the leader". Empty means there was nothing to acquire, and that
     *     covers three cases: someone else holds an unexpired lease, someone
     *     else won the race for it, and THIS INSTANCE ALREADY HOLDS IT. A
     *     caller that reads empty as "stand down" makes a healthy leader
     *     abandon a live term for the sole offence of asking twice — a
     *     supervisor tick or a restarted election loop is enough. Gate on
     *     {@link #held()}, which after M4.3e no longer reports a term this
     *     instance does not hold
     * @throws IOException the store was unreachable, the lease object could
     *     not be parsed, or the lock could not be taken within
     *     {@code renewInterval} — ⚠️ NONE of them treated as unheld, see below
     */
    public Optional<Lease> tryAcquire() throws IOException {
        lockOrFail("acquire", renewInterval);
        try {
            return acquireLocked();
        } finally {
            lock.unlock();
        }
    }

    private Optional<Lease> acquireLocked() throws IOException {
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
            // ⚠️ CONDITIONAL, never blanket (M4.3e). Every other path clears
            // the belief when it learns it is fenced, and `adopt`'s comment
            // says why: leaving a superseded lease in `held` would make
            // `held()` report a term this instance does not hold. But this
            // instance may legitimately be looking at its OWN unexpired lease,
            // and clearing there would make a healthy leader forget its term
            // for the sole offence of having asked. Same (epoch, podId)
            // identity `refreshed` uses, and for the same reason.
            Belief b = belief;
            if (b != null && !isOwnTerm(b.lease(), current)) {
                belief = null;
            }
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
     * @throws IOException the store was unreachable, or the lock could not be
     *     taken within {@code renewInterval} — ⚠️ NOT the same as empty, and
     *     the two are not the same as each other either: an IOException out of
     *     the conditional write leaves the belief AMBIGUOUS, while one out of
     *     the lock leaves it untouched, because nothing was written
     */
    public Optional<Lease> renew() throws IOException {
        lockOrFail("renew", renewInterval);
        try {
            return renewLocked();
        } finally {
            lock.unlock();
        }
    }

    private Optional<Lease> renewLocked() throws IOException {
        // ⚠️ ONE snapshot, then work from it. Correct either way today --
        // every write to `belief` happens under this lock -- but re-reading a
        // volatile field for each use makes that a property a reader has to
        // reconstruct, and one unlocked writer added later would silently make
        // `lease()` and `version()` a TORN PAIR: an expired lease written
        // against a version from a different belief.
        Belief mine = belief;
        if (mine == null) {
            return Optional.empty();
        }
        // ⚠️ Owed only after an ambiguous write, never on the healthy path.
        if (mine.ambiguous()) {
            mine = refreshed(mine);
            if (mine == null) {
                return Optional.empty();
            }
        }
        Lease renewed = mine.lease().renewedUntil(clock.millis() + ttl.toMillis());
        // ⚠️ ONE conditional write, with no re-read: the holder already knows
        // its own version. A read per renew would double the lease's request
        // rate for nothing.
        Optional<Version> won;
        try {
            won = store.putIfMatch(key, Body.ofBytes(renewed.encode()), mine.version());
        } catch (IOException e) {
            // ⚠️ AMBIGUOUS, NOT FAILED. The write may have landed and lost only
            // its response, in which case `heldVersion` is now stale and the
            // next renew would lose to THIS INSTANCE'S OWN bytes. Belief is
            // kept -- being unable to reach the store is not evidence of being
            // fenced -- but the next renew must re-read before trusting the
            // version it cached.
            belief = mine.uncertain();
            throw e;
        }
        if (won.isEmpty()) {
            // ⚠️ Fenced. Drop the local belief too, so a caller that ignores
            // the empty result cannot keep renewing against a stale version.
            belief = null;
            return Optional.empty();
        }
        belief = new Belief(renewed, won.get(), false);
        return Optional.of(renewed);
    }

    /**
     * Whether {@code current} is the term this instance believes it holds.
     *
     * <p>⚠️ BOTH HALVES, and the epoch is the load-bearing one: a StatefulSet
     * reuses pod names, so the realistic successor to a dead {@code podA} is a
     * restarted {@code podA}, and matching on the holder alone would let a
     * fenced process claim its successor's term. The podId half guards an
     * out-of-protocol lease at a matching epoch under another holder — no
     * in-protocol path mints one, because {@code takenOverBy} advances the
     * epoch and restamps the holder together, but {@link Lease} deliberately
     * keeps foreign objects parseable.
     *
     * <p>⚠️ The belief is a PARAMETER rather than a read of {@code held},
     * which is deliberate: {@code held} is mutable and nullable, so a
     * precondition stated in prose would be checked by nobody. Passing it
     * makes each call site state which belief it means, and a site that has
     * not established one cannot reach here by forgetting to look — the
     * difference between rung 1 and rung 7 of the {@code gate-design} ladder.
     */
    private boolean isOwnTerm(Lease mine, Lease current) {
        return current.epoch() == mine.epoch() && current.holderPodId().equals(podId);
    }

    /**
     * Re-reads the lease after an ambiguous write and re-establishes the
     * version, if this instance's term still stands.
     *
     * <p>⚠️ KEYED ON (epoch, podId), NOT merely on the version being readable.
     * Adopting whatever is there would hand a genuinely fenced holder a live
     * version and let two nodes sequence at once — the one thing the lease
     * exists to make unlikely. {@code takenOverBy} advances the epoch by
     * exactly one and stamps the new holder, so a successor's term can never
     * be mistaken for this one's.
     *
     * <p>⚠️ RETURNS THE NEW BELIEF rather than a boolean plus a field write, so
     * a caller cannot act on the stale snapshot it passed in. That was possible
     * while this returned {@code true} and left the caller to re-read the field.
     *
     * @param mine what the caller believes, and what its own later reads use
     * @return the refreshed belief if the term still stands, or null if this
     *     instance was genuinely superseded — in which case the belief is
     *     dropped exactly as a lost conditional write drops it
     */
    private Belief refreshed(Belief mine) throws IOException {
        Optional<ObjectStat> stat = store.stat(key);
        if (stat.isPresent()) {
            Lease current = read();
            if (isOwnTerm(mine.lease(), current)) {
                Belief fresh = new Belief(current, stat.get().version(), false);
                belief = fresh;
                return fresh;
            }
        }
        // ⚠️ An ABSENT lease also lands here. Nothing legitimately deletes the
        // object -- `release` writes an expired lease precisely so the epoch
        // counter survives -- so its absence means someone did something this
        // protocol does not model, and continuing to sequence on that basis is
        // the failure mode worth avoiding.
        belief = null;
        return null;
    }

    /**
     * Gives up the lease voluntarily, so a successor need not wait out the TTL.
     *
     * <p>⚠️ Writes an EXPIRED lease rather than deleting the object — see the
     * class javadoc for why deleting would break fencing. A no-op when nothing
     * is held (a {@code SIGTERM} on a non-holder is entirely normal), and
     * harmless when already fenced, because the conditional write simply loses.
     *
     * <p>⚠️ Refreshes first after an ambiguous write, because a release that
     * writes nothing is indistinguishable from no release at all: the successor
     * waits out a TTL this method exists to spare it.
     */
    public void release() throws IOException {
        // ⚠️ AHEAD OF THE LOCK, deliberately. This is the documented no-op, it
        // touches no I/O, and `belief` is volatile precisely so it can be read
        // without the lock. Behind the lock, a pod that never held the lease
        // and happens to have an election thread inside a slow `tryAcquire`
        // would block its whole shutdown path and then throw about a lock it
        // had no reason to want.
        if (belief == null) {
            return;
        }
        lockOrFail("release", ttl);
        try {
            releaseLocked();
        } finally {
            lock.unlock();
        }
    }

    private void releaseLocked() throws IOException {
        Belief mine = belief;
        if (mine == null) {
            // ⚠️ Re-checked under the lock: the fast path above raced.
            return;
        }
        // ⚠️ Expiry is INCLUSIVE, so `now` is already expired.
        // ⚠️ Same refresh as `renew`, and for the same harm rather than for
        // symmetry: releasing with a stale version writes NOTHING, so the
        // still-unexpired lease has to be waited out -- the exact TTL-long
        // gap release exists to avoid.
        // ⚠️ This method CONSUMES the flag without ever producing it, and the
        // asymmetry with `renew` is deliberate rather than an omission: an
        // ambiguous release is idempotent in effect. If its write landed, the
        // expired lease is already on the store and a retry's lost CAS changes
        // nothing; if it was lost, the version never moved and the retry wins.
        if (mine.ambiguous()) {
            mine = refreshed(mine);
            if (mine == null) {
                return;
            }
        }
        Lease expired = mine.lease().renewedUntil(clock.millis());
        store.putIfMatch(key, Body.ofBytes(expired.encode()), mine.version());
        belief = null;
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
            belief = null;
            return Optional.empty();
        }
        belief = new Belief(candidate, won.get(), false);
        return Optional.of(candidate);
    }
}
