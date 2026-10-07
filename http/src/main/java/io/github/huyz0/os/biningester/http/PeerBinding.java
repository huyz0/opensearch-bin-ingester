// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Whether a peer request's claimed identity is its client certificate's
 * (ADR-0084 decision 8; M13.52g): the certificate names a pod of THIS fleet --
 * its prefix the receiving pod's own -- and the pod the request claims to be.
 *
 * <p>⚠️ **UNDER {@code off} IT BINDS NOTHING**: there is no certificate, and
 * decision 5's warning says so at every start.
 */
public final class PeerBinding {

    private final String prefix;

    private PeerBinding(String prefix) {
        this.prefix = prefix;
    }

    /** Binds nothing: {@code peer.tls = off}. */
    public static PeerBinding off() {
        return new PeerBinding(null);
    }

    /** Binds every request to a certificate under {@code prefix}, this pod's fleet. */
    public static PeerBinding within(String prefix) {
        return new PeerBinding(Objects.requireNonNull(prefix, "prefix"));
    }

    /** Why a request from {@code chain} may not claim sender UID {@code uid}; empty when it may. */
    public Optional<String> refusalForUid(Optional<Certificate[]> chain, String uid) {
        return refusal(chain, named -> named.podUid().equals(uid), "sender UID " + uid);
    }

    /** Why it may not claim {@code pod.id} {@code podId}; empty when it may. */
    public Optional<String> refusalForPodId(Optional<Certificate[]> chain, String podId) {
        return refusal(chain, named -> named.podId().equals(podId), "pod " + podId);
    }

    /** Why it may not claim the incarnation {@code uid} of {@code podId}; empty when it may. */
    public Optional<String> refusalForIncarnation(Optional<Certificate[]> chain, String uid,
            String podId) {
        return refusal(chain, named -> named.podUid().equals(uid) && named.podId().equals(podId),
                "incarnation " + uid + " of pod " + podId);
    }

    private Optional<String> refusal(Optional<Certificate[]> chain,
            Predicate<PeerIdentity> claims, String claim) {
        if (prefix == null) {
            return Optional.empty();
        }
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty() || chain.get().length == 0
                || !(chain.get()[0] instanceof X509Certificate certificate)) {
            return Optional.of("no client certificate may claim " + claim);
        }
        PeerIdentity named;
        try {
            named = PeerIdentity.of(certificate);
        } catch (IllegalArgumentException none) {
            return Optional.of("the client certificate names no pod: " + none.getMessage());
        }
        if (!named.prefix().equals(prefix)) {
            return Optional.of("the client certificate's fleet " + named.prefix()
                    + " is not this pod's, " + prefix);
        }
        if (!claims.test(named)) {
            return Optional.of("the client certificate names " + named.podName() + " ("
                    + named.podUid() + "), which may not claim " + claim);
        }
        return Optional.empty();
    }
}
