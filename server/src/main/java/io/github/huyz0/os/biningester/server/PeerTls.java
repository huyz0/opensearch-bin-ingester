// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;

/**
 * What {@code peer.tls = mutual} read at start (ADR-0084; M13.52b): the pod's
 * certificate chain and key, presented on both sides of every peer connection,
 * and the trust domain's CA, the only one a peer's chain may end at.
 */
public record PeerTls(List<X509Certificate> chain, PrivateKey key, List<X509Certificate> ca) {

    public PeerTls {
        chain = List.copyOf(chain);
        Objects.requireNonNull(key, "key");
        ca = List.copyOf(ca);
        if (chain.isEmpty() || ca.isEmpty()) {
            throw new IllegalArgumentException("a peer chain and a CA hold a certificate each");
        }
    }

    /** Who this pod's own certificate names (ADR-0084 decision 8). */
    public io.github.huyz0.os.biningester.http.PeerIdentity identity() {
        return io.github.huyz0.os.biningester.http.PeerIdentity.of(chain.get(0));
    }

    /**
     * Refuses a certificate that does not name THIS pod (ADR-0084 decision 8;
     * M13.52f): its trust domain, its {@code pod.uid}, and a pod name whose
     * dashes removed are its {@code pod.id}. ⚠️ A mis-issued certificate fails
     * here, at the pod that holds it, rather than at every peer it dials.
     *
     * @throws ConfigurationException naming {@code peer.tls.cert} and what the
     *     certificate says
     */
    void requireNames(String trustDomain, String podId, String podUid) {
        io.github.huyz0.os.biningester.http.PeerIdentity named;
        try {
            named = identity();
        } catch (IllegalArgumentException none) {
            throw new ConfigurationException(ServerProperties.PEER_TLS_CERT
                    + " names no pod: " + none.getMessage() + " (ADR-0084 decision 8)", none);
        }
        if (!named.trustDomain().equals(trustDomain) || !named.podUid().equals(podUid)
                || !named.podId().equals(podId)) {
            throw new ConfigurationException(ServerProperties.PEER_TLS_CERT + " names "
                    + named.prefix() + "/" + named.podName() + "/" + named.podUid()
                    + ", not this pod: " + ServerProperties.TRUST_DOMAIN + " = " + trustDomain
                    + ", " + ServerProperties.POD_ID + " = " + podId + " (its pod name, dashes "
                    + "removed), " + ServerProperties.POD_UID + " = " + podUid
                    + " (ADR-0084 decision 8)");
        }
    }

    /** ⚠️ NEVER THE KEY: a record's default text would print it. */
    @Override
    public String toString() {
        return "PeerTls[chain=" + chain.size() + " certificate(s), key=<redacted>, ca="
                + ca.size() + " certificate(s)]";
    }
}
