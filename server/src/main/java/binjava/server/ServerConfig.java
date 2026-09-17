// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.ingest.IngestConfig;
import java.time.Duration;
import java.util.Objects;

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
 * @param trustDomain the cluster a producer's {@code Principal} must match
 * @param prefix the key prefix everything this pod writes lives under, ⚠️ shared
 *     by the writer and the commit log: two prefixes is a commit log nothing
 *     reads, with every append still returning an offset
 * @param store which backend, by name
 * @param leaseTtl how long a lease outlives its holder's last renewal
 * @param leaseRenewInterval how often the holder renews. ⚠️ Measurement M1
 *     (M8.27) is what sizes these two against a realistic pause; until then
 *     they are a configured guess and the SPEC says so
 * @param endpoint where peers reach this pod's sequencer
 * @param ingest the write path's own tunables, which have their own defaults
 */
public record ServerConfig(String podId, String trustDomain, String prefix, StoreConfig store,
        Duration leaseTtl, Duration leaseRenewInterval, String endpoint, IngestConfig ingest) {

    public ServerConfig {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(trustDomain, "trustDomain");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(leaseTtl, "leaseTtl");
        Objects.requireNonNull(leaseRenewInterval, "leaseRenewInterval");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(ingest, "ingest");
        requireNotBlank(podId, "podId");
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
