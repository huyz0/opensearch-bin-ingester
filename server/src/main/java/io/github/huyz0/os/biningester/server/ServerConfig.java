// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * Everything one ingester node needs to be built (M8.1).
 *
 * <p>⚠️ **DATA, PARSED ONCE, AT THE ROOT.** Every seam this project spent eight
 * milestones injecting is constructed from this record and nowhere else, which
 * is what makes the I/O and the clock real in exactly one module.
 *
 * <p>⚠️ **THIS RECORD DOES NOT PARSE AND DOES NOT DEFAULT** — M8.26 owns
 * reading it out of a file or an environment and refusing a bad key with a
 * message naming it. What it does here is refuse a null and a blank, so that an
 * unset variable fails where it is introduced rather than at the first write.
 *
 * @param podId this node's identity in the lease, and ⚠️ the thing a takeover is
 *     attributed to — a duplicate across two pods is two leaders that each
 *     believe they hold one lease
 * @param podUid this pod's immutable Kubernetes UID, written into new leases
 * @param az this pod's availability zone, as a LABEL (M9.2, NFR-5). ⚠️ **IT IS
 *     WHAT MAKES A CROSS-AZ BYTE COUNTABLE**: every peer transport compares a
 *     peer's label with this one, so a pod whose zone is wrong reports a
 *     plausible NFR-5 number for a fleet it is not in
 * @param trustDomain the cluster a producer's {@code Principal} must match
 * @param prefix the key prefix everything this pod writes lives under, ⚠️ shared
 *     by the writer and the commit log: two prefixes is a commit log nothing
 *     reads, with every append still returning an offset
 * @param store which backend, by name
 * @param leaseTtl how long a lease outlives its holder's last renewal
 * @param leaseRenewInterval how often the holder renews. ⚠️ Measurement M1
 *     (M8.27) sized the defaults of these two against pauses of 2-20 s; see
 *     {@code ServerProperties.DEFAULT_LEASE_TTL}
 * @param endpoint where peers reach this pod's sequencer
 * @param ingest the write path's own tunables, which have their own defaults
 * @param httpPort the port the front door listens on, ⚠️ **0 meaning "ask the
 *     kernel"** — which is what a test wants and what a deployment must not
 *     have, since a peer reaching {@link #endpoint()} would be dialling a port
 *     nobody can predict. It is configuration rather than a constant for the
 *     same reason the endpoint is
 * @param producerSubject who an unauthenticated producer is taken to be until
 *     authentication lands. ⚠️ **A SKELETON, AND VISIBLE AS ONE**: M1.7's
 *     {@code BulkService} resolves no credential, so this is the identity
 *     stamped on every write this node accepts
 * @param allowedIndices which indices that producer may write to. ⚠️ **EMPTY
 *     PERMITS NOTHING** ({@code Principal}, ADR-0021), and this record does NOT
 *     widen that: a composition root that defaulted the allow-list to "all"
 *     would turn the one security property the write path actually has into a
 *     comment
 * @param retention the floor, the ceiling and the loop's interval (M8.5)
 * @param membership where to watch the ingester Service's endpoints for the
 *     early lease challenge, or empty for none (M8.13)
 * @param costTopKInterval how often the top-K cost line is logged; zero for
 *     never (M11.5)
 * @param quotas the per-index admission quotas; none by default (M11.8)
 * @param adminCost whether {@code GET /admin/cost} is served; off unless
 *     configured (M12.6)
 */
public record ServerConfig(String podId, String az, String trustDomain, String prefix,
        StoreConfig store, Duration leaseTtl, Duration leaseRenewInterval, String endpoint, IngestConfig ingest,
        int httpPort, String producerSubject, Set<String> allowedIndices,
        RetentionConfig retention, java.util.Optional<MembershipConfig> membership,
        String podUid, Duration costTopKInterval,
        io.github.huyz0.os.biningester.ingest.IndexQuotas.Config quotas, boolean adminCost) {

    // ⚠️ ONLY THE CANONICAL CONSTRUCTOR (M13.6b, M12 harvest R5): six older
    // ones each filled in a setting their callers predated, and a new caller
    // reaching for the shortest got every default without saying so.

    /**
     * The identity every write this node accepts is attributed to.
     *
     * <p>⚠️ **BUILT HERE SO THERE IS ONE OF IT.** The trust domain a producer
     * is checked against and the one an operator configured are the same
     * string by this record's own constructor guard; a second {@code Principal}
     * assembled at a call site is how the two start to differ.
     */
    public Principal principal() {
        return new Principal(trustDomain, producerSubject, allowedIndices);
    }

    public ServerConfig {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(podUid, "podUid");
        Objects.requireNonNull(az, "az");
        Objects.requireNonNull(trustDomain, "trustDomain");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        Objects.requireNonNull(leaseRenewInterval, "leaseRenewInterval");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(ingest, "ingest");
        Objects.requireNonNull(producerSubject, "producerSubject");
        Objects.requireNonNull(retention, "retention");
        Objects.requireNonNull(membership, "membership");
        Objects.requireNonNull(costTopKInterval, "costTopKInterval");
        Objects.requireNonNull(quotas, "quotas");
        if (costTopKInterval.isNegative()) {
            throw new IllegalArgumentException("costTopKInterval is never negative: "
                    + costTopKInterval);
        }
        // ⚠️ COPIED, then the record hands it on to `Principal`, which copies
        // again. Belt and braces on purpose: an allow-list a caller can still
        // `add()` to is a privilege escalation with no code change at the call
        // site, and this record outlives every construction site.
        allowedIndices = Set.copyOf(Objects.requireNonNull(allowedIndices, "allowedIndices"));
        requireNotBlank(podId, "podId");
        // ⚠️ REQUIRED BY THE RECORD AS BY THE PARSER (M13.27f): every term's
        // roster names its leader by its UID (ADR-0081 §1), so a pod without
        // one could take the lease and then start no term.
        requireNotBlank(podUid, "podUid");
        // ⚠️ BLANK IS REFUSED HERE TOO, not only at the parser: a zone of
        // spaces makes every peer cross-AZ and every measurement of NFR-5 a
        // number about nothing.
        requireNotBlank(az, "az");
        requireNotBlank(producerSubject, "producerSubject");
        if (httpPort < 0 || httpPort > 65535) {
            throw new IllegalArgumentException("httpPort is not a port: " + httpPort);
        }
        requireNotBlank(trustDomain, "trustDomain");
        requireNotBlank(prefix, "prefix");
        requireNotBlank(endpoint, "endpoint");
        if (!trustDomain.equals(ingest.trustDomain())) {
            // ⚠️ ONE TRUST DOMAIN, NOT TWO THAT DRIFT. The producer's principal
            // is checked against the ingest config's domain; this record's is
            // what an operator reads. Two values means a pod that refuses every
            // producer it was configured to accept, and the message it gives
            // names the domain the operator did NOT set.
            throw new IllegalArgumentException("trustDomain " + trustDomain
                    + " disagrees with the ingest config's " + ingest.trustDomain());
        }
    }

    private static void requireNotBlank(String value, String name) {
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is blank");
        }
    }
}
