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

    /** ⚠️ NEVER THE KEY: a record's default text would print it. */
    @Override
    public String toString() {
        return "PeerTls[chain=" + chain.size() + " certificate(s), key=<redacted>, ca="
                + ca.size() + " certificate(s)]";
    }
}
