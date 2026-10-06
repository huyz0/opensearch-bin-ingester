// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.helidon.common.tls.Tls;
import java.util.List;
import java.util.Optional;

/**
 * The TLS every pod-to-pod client presents (ADR-0084 decision 3; M13.52d):
 * this pod's chain and key, trust in the trust domain's CA alone, TLS 1.3.
 *
 * <p>⚠️ NO HOSTNAME CHECK (ADR-0084 decision 4): a peer is any pod the
 * domain's CA certified -- an EndpointSlice address or a pod name rarely
 * appears in a pod certificate's SAN -- and binding a certificate to the pod
 * a frame names is M13.52e's.
 */
final class PeerClientTls {

    private PeerClientTls() {
    }

    /** The clients' TLS for {@code read}, none for a plaintext ({@code off}) fleet. */
    static Optional<Tls> of(Optional<PeerTls> read) {
        return read.map(tls -> Tls.builder()
                .enabledProtocols(List.of("TLSv1.3"))
                .privateKey(tls.key())
                .privateKeyCertChain(tls.chain())
                .trust(tls.ca())
                .endpointIdentificationAlgorithm(Tls.ENDPOINT_IDENTIFICATION_NONE)
                .build());
    }
}
