// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.util.Objects;
import java.util.Optional;

/**
 * The peer listener's settings (ADR-0084; M13.52b): whether its four routes
 * take mutual TLS, its port, and the PEM files {@code mutual} reads at start.
 *
 * @param port the peer listener's port, fleet-wide; 0 an ephemeral one, for a
 *     test
 * @param files the pod's certificate chain, its key and the trust domain's
 *     CA -- present exactly when {@code mode} is {@code MUTUAL}
 */
public record PeerConfig(Mode mode, int port, Optional<Files> files) {

    /** {@code peer.tls}: no default, so an insecure listener is a choice written down. */
    public enum Mode { MUTUAL, OFF }

    /** The three PEM files {@code mutual} reads, as paths. */
    public record Files(String cert, String key, String ca) {
        public Files {
            Objects.requireNonNull(cert, "cert");
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(ca, "ca");
        }
    }

    public PeerConfig {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(files, "files");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("the peer port is in 0..65535: " + port);
        }
        if ((mode == Mode.MUTUAL) != files.isPresent()) {
            throw new IllegalArgumentException("mutual TLS reads its three files, and off none");
        }
    }

    /** The plaintext listener on {@code port}: a single node, a development fleet, a test. */
    public static PeerConfig off(int port) {
        return new PeerConfig(Mode.OFF, port, Optional.empty());
    }
}
