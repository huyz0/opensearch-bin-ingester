// SPDX-License-Identifier: Apache-2.0
package binjava.security;

import java.util.Objects;
import java.util.Set;

/**
 * Who a producer is, and therefore which trust domain its writes belong to.
 *
 * <p>⚠️ The trust domain is resolved FROM the credential, never presented
 * alongside it (ADR-0021). There is no separate "which cluster is this for?"
 * field to get wrong, and a producer cannot write into a domain its credential
 * does not name. The domain selects the bucket prefix, the object-store
 * identity, the ordinal registry and the segment stream — segments are never
 * bundled across trust domains.
 */
public record Principal(String trustDomain, String subject, Set<String> allowedIndices) {

    public Principal {
        Objects.requireNonNull(trustDomain, "trustDomain");
        Objects.requireNonNull(subject, "subject");
        // ⚠️ Copy, then wrap unmodifiable. Holding the caller's Set would let a
        // later add() widen the allow-list of a principal already authorised —
        // a privilege escalation with no code change at the call site.
        allowedIndices = Set.copyOf(allowedIndices);
    }

    /**
     * Whether this principal may write to {@code index}.
     *
     * <p>⚠️ An empty allow-list permits NOTHING. The opposite default turns a
     * misconfiguration into a silent cross-index write, and ADR-0021 makes
     * writing outside the list a 403 rather than a buffered write.
     */
    public boolean canWriteTo(String index) {
        return allowedIndices.contains(index);
    }
}
