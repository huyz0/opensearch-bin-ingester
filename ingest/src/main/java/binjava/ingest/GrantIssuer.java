// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.SignedUrl;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Issues the short-lived grant a consumer uses in {@code direct} mode (FR-6,
 * M5.13, ADR-0041).
 *
 * <p>⚠️ IT REFUSES TO EXIST AT ALL against a backend that cannot presign, and
 * that is the whole shape of criterion 7's "refused at STARTUP, not at first
 * use": if the backend is one of the two that ship today, constructing this
 * fails.
 *
 * <p>⚠️ BOTH HALVES OF THAT LANDED, AND THIS PARAGRAPH USED TO SAY NEITHER
 * HAD. M5.43 added {@code IngestConfig.directEnabled} and
 * {@code FetchPolicyConfig.directEnabled}, so a deployment can say it wants
 * {@code direct}; M5.45d constructs this class in {@code DefaultIngest} under
 * that flag, once per pod, which is the call site "at startup" needed. What is
 * still true is the ORDER: the refusal happens when the pod is built, not when
 * a consumer first asks. ⚠️ And
 * the two halves of the system disagree about what to do when the backend
 * cannot sign: {@code FetchPolicy.modeFor} silently DEGRADES to {@code PROXY},
 * while this class REFUSES TO EXIST. Wire it unconditionally and every pod
 * fails to start on both shipping backends; wire it lazily and the refusal is
 * back at first fetch, which is what it was built to prevent. ⚠️ M5.43 owns
 * that decision and the setting it needs; this class is the enforcement, not
 * the wiring. The alternative is an {@code UnsupportedOperationException} at the
 * first consumer fetch, blaming a backend that was honest about what it cannot
 * do — the exact failure {@code Capabilities.requirePresignedUrls()} was
 * written for in M5.10 and, until this class, had no production caller.
 *
 * <p>⚠️ THE TTL CEILING IS ENFORCED HERE AND DECIDED ELSEWHERE.
 * {@code BinStore.presign} rejects a non-positive TTL and nothing more, so
 * {@code Duration.ofDays(30)} was a legal grant, and M5.10's own review
 * recorded that the bound was enforced by nobody. ⚠️ It was never UNDECIDED,
 * though: ADR-0010 fixes it at ≤60 s and security.md rule 3's "short TTL"
 * points there through rule 8. This class is the enforcement point, not the
 * decision — see {@link #DEFAULT_CEILING} for the draft that got that backwards
 * and what it cost.
 *
 * <p>⚠️ AND THE GRANT IS NEVER LOGGED, TRACED OR PUT IN AN ERROR MESSAGE
 * (security.md rule 4). {@link SignedUrl} makes that hold for anything that
 * prints the object; what this class adds is the discipline on the FAILURE
 * path, which is where a URL escapes without anyone deciding to log it. A
 * refusal here names the KEY and never the grant — because diagnosing "which
 * segment failed" has to stay possible, or rule 4 is obeyed by making the
 * system undebuggable and someone quietly breaks it back.
 *
 * <p>⚠️ HOW THE GRANT REACHES THE CONSUMER IS NOT THIS CLASS'S. That is the
 * subscription protocol's, and ADR-0041 says so in as many words: M5.14 owns
 * it, with its ADR and its golden files.
 */
public final class GrantIssuer {

    /**
     * Sixty seconds — the bound ADR-0010 decided, not a number chosen here.
     *
     * <p>⚠️ IT IS QUOTED, NOT INVENTED, and an earlier draft of this class got
     * that badly wrong: it shipped five minutes as a default and an hour as the
     * maximum, and justified them by saying "no research document fixes it".
     * ADR-0010 (accepted, FR-6) fixes it in a table row — "Client -> object
     * store (`direct`) | short-lived signed URL, ≤60 s, read-only, scoped to
     * key" — and research doc 10 §6 agrees less precisely at "short TTL
     * (seconds)". Five minutes was 5x the decided bound and an hour was 60x.
     * ⚠️ Widening a settled security bound is an ADR, not a task, and
     * non-negotiable 2 forbids moving a threshold in the weakening direction at
     * all. Round-1 review caught it; the number is ADR-0010's.
     *
     * <p>⚠️ AND THE M4 ANALOGY THAT DRAFT LEANED ON WAS INVERTED. {@code
     * LeaseConfig} carries no default because measurement M1 settles the lease
     * TTL at M8; the grant TTL is not on the deferred list — M3, the fan-out
     * threshold, is the only entry `direct` owns there. A deferred constant and
     * a decided one are opposite situations.
     */
    public static final Duration DEFAULT_CEILING = Duration.ofSeconds(60);

    /**
     * Also sixty seconds: ADR-0010's bound is a MAXIMUM, so nothing may be
     * configured above it.
     *
     * <p>⚠️ CONFIGURATION MAY ONLY SHORTEN. A deployment that wants a tighter
     * grant passes a smaller ceiling and this class honours it; a deployment
     * that wants a longer one needs an ADR reopening ADR-0010, which is the
     * point of the two being equal rather than the maximum being a roomier
     * number someone picked.
     *
     * <p>⚠️ THE BOUND HAS TO BE OURS BECAUSE THE BACKEND'S IS USELESS HERE: an
     * Azure user-delegation key is valid up to SEVEN DAYS (ADR-0041), so a
     * backend will happily sign far beyond anything this system should hand a
     * consumer.
     */
    public static final Duration MAX_CEILING = Duration.ofSeconds(60);

    private final BinStore store;
    private final Duration ceiling;

    /** With {@link #DEFAULT_CEILING}. */
    public GrantIssuer(BinStore store) {
        this(store, DEFAULT_CEILING);
    }

    /**
     * @throws IllegalStateException if the backend cannot presign — the
     *     startup refusal criterion 7 asks for
     * @throws IllegalArgumentException if {@code ceiling} is not positive or
     *     exceeds {@link #MAX_CEILING}
     */
    public GrantIssuer(BinStore store, Duration ceiling) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(ceiling, "ceiling");
        // ⚠️ BEFORE ANYTHING ELSE, so the message a misconfigured deployment
        // sees is about the capability rather than about a TTL it also got
        // wrong. `requirePresignedUrls` throws IllegalStateException naming
        // ADR-0041 and the `direct` fetch mode.
        store.capabilities().requirePresignedUrls();
        if (ceiling.isNegative() || ceiling.isZero()) {
            throw new IllegalArgumentException(
                    "a grant ceiling of " + ceiling + " issues nothing usable");
        }
        if (ceiling.compareTo(MAX_CEILING) > 0) {
            throw new IllegalArgumentException(
                    "a grant ceiling of " + ceiling + " exceeds the " + MAX_CEILING
                            + " maximum -- security.md rule 3 requires a SHORT ttl, and a "
                            + "configurable bound that can be set to a week is not one");
        }
        this.ceiling = ceiling;
    }

    /** The longest grant this issuer will mint. */
    public Duration ceiling() {
        return ceiling;
    }

    /**
     * A grant for exactly {@code segmentKey}, lasting at most {@link
     * #ceiling()}.
     *
     * <p>⚠️ ONE GRANT PER (NODE, SEGMENT), NEVER ONE PER RUN -- and M5.39 asked
     * this row to say where that rule lives, so it says so here. Each grant
     * becomes ONE CONSUMER-SIDE GET, and unlike the proxy path that GET is
     * invisible to the ingester's own {@code CountingBinStore}: nothing in this
     * process can measure the cost of issuing too many. A caller looping the
     * per-{@code RunKey} subscriber map -- which is how {@code SegmentProxy}'s
     * caller is already shaped -- would mint K grants for one node holding K
     * runs of a ~1,600-run segment, a rate scaling with shards per node that
     * non-negotiable 6 forbids by name. ⚠️ THE API DOES NOT RESIST IT: this
     * method mints fresh every call and reuses no unexpired grant for the same
     * key.
     *
     * <p>⚠️ THE WIRING THAT RESPECTS IT IS {@code SegmentServingPath.deliver},
     * WHICH HOISTS THE MINT ABOVE BOTH LOOPS (M5.45d). An earlier draft of this
     * sentence handed it to M5.43, which shipped the two {@code directEnabled}
     * settings and no minting at all, so a reader following the pointer found
     * nothing and could reasonably conclude the rule was unowned -- and then
     * moving the mint back inside the target loop reads as a tidy-up. That is
     * the mutation round-1 review measured leaving the whole module green.
     * ⚠️ M5.65 MOVED {@code deliver} out of {@code SubscriptionHub} into
     * {@code SegmentServingPath}, and this paragraph moved with the pointer --
     * which is the whole reason the sentence it replaces was written.
     *
     * <p>⚠️ THE TTL IS CLAMPED, NOT REFUSED, and the direction is deliberate: a
     * caller asking for longer than the ceiling gets the ceiling, because the
     * alternative is a fetch that fails rather than a fetch that is merely
     * shorter-lived than someone hoped. A caller asking for a non-positive TTL
     * IS refused, because that is not a shorter grant, it is an unusable one.
     *
     * <p>⚠️ THE FAILURE PATH NAMES THE KEY AND NEVER THE GRANT. The message
     * this method adds carries {@code segmentKey} — an object name, not a
     * credential — and nothing else. In particular it does NOT interpolate the
     * cause's message, which is the amplifier round-1 review named: writing
     * {@code + ": " + signingFailed.getMessage()} would copy whatever the
     * backend chose to say into a message this class vouches for.
     *
     * <p>⚠️ AN EARLIER DRAFT ARGUED THIS WAS SAFE BECAUSE "the cause was raised
     * BEFORE a URL existed". That is a claim about arbitrary backend code this
     * class has never seen — all it knows is that {@code presign} did not
     * return — and it covered only one of the two things security.md rule 4
     * names: a signing failure is the LIKELIEST place in the system for a
     * CREDENTIAL to surface in a message.
     *
     * <p>⚠️ SO THE CAUSE IS CHAINED BUT NEVER QUOTED, and the residual risk is
     * stated rather than argued away: a backend whose own exception message
     * carries a URL or a credential defeats rule 4 through the chain, and that
     * is the BACKEND's defect. The obligation is now written where a backend
     * author reads it, in {@code BinStore.presign}'s javadoc, and M5.42 owns
     * making {@code PresignConformance} check it — which it cannot today,
     * because M5.37 records that its capable half runs only against a
     * stand-in.
     */
    public SignedUrl grantFor(String segmentKey, Duration requested) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(requested, "requested");
        if (requested.isNegative() || requested.isZero()) {
            throw new IllegalArgumentException(
                    "a grant of " + requested + " for " + segmentKey + " is not usable");
        }
        Duration ttl = requested.compareTo(ceiling) > 0 ? ceiling : requested;
        try {
            return store.presign(segmentKey, ttl);
        } catch (IOException signingFailed) {
            throw new IOException("could not sign a grant for " + segmentKey, signingFailed);
        }
    }

    /**
     * A grant for {@code segmentKey} lasting exactly {@link #ceiling()}.
     *
     * <p>⚠️ THE MAXIMUM IS ALSO THE DEFAULT HERE, and round-1 review named the
     * consequence: shortening the ceiling for one deployment shortens every
     * grant it issues, and there is no way to ask for less without passing a
     * duration. That is the RIGHT default for a security bound -- the shortest
     * useful grant is the safest one, and the ceiling is already ADR-0010's
     * 60 s rather than a roomy number -- but it means the two-argument form is
     * the one a caller wanting a tighter grant must use.
     */
    public SignedUrl grantFor(String segmentKey) throws IOException {
        return grantFor(segmentKey, ceiling);
    }
}
