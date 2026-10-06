// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The peer files are read by the start itself (M13.52b review round 1, T1,
 * T2, T5): a node whose {@code mutual} CA is missing never starts, an empty
 * certificate file is refused by its key, and the key never reaches a text.
 */
class MainRunPeerTlsTest {

    @TempDir
    Path dir;

    private static String fixture(String name) throws Exception {
        return Path.of(MainRunPeerTlsTest.class.getResource("/peer-tls/" + name).toURI())
                .toString().replace('\\', '/');
    }

    @Test
    void aNODEWhoseMutualCAIsMissingNeverStarts() throws Exception {
        String missing = dir.resolve("absent-ca.pem").toString().replace('\\', '/');
        Path config = dir.resolve("node.properties");
        Files.writeString(config, String.join("\n",
                "pod.id=pod1", "pod.uid=uid-pod1", "pod.az=az-a", "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a", "store.kind=memory",
                "endpoint=http://localhost:0", "http.port=0",
                "producer.subject=producer-1", "producer.allowed-indices=logs",
                "peer.tls=mutual", "peer.port=0",
                "peer.tls.cert=" + fixture("pod1.pem"), "peer.tls.key=" + fixture("pod1.key"),
                "peer.tls.ca=" + missing));

        assertThatThrownBy(() -> Main.run(config.toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CA)
                .hasMessageContaining(missing);
    }

    @Test
    void anEMPTYCertificateFileIsRefusedByItsKey() throws Exception {
        Path empty = Files.writeString(dir.resolve("empty.pem"), "");

        assertThatThrownBy(() -> Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 9443,
                Optional.of(new PeerConfig.Files(empty.toString(), fixture("pod1.key"),
                        fixture("ca.pem")))), message -> { }))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT);
    }

    @Test
    void theKEYNeverReachesTheTextOfWhatWasRead() throws Exception {
        PeerTls tls = Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 9443,
                Optional.of(new PeerConfig.Files(fixture("pod1.pem"), fixture("pod1.key"),
                        fixture("ca.pem")))), message -> { }).orElseThrow();
        String encoded = java.util.Base64.getEncoder().encodeToString(tls.key().getEncoded());

        assertThat(tls.toString()).contains("redacted")
                .doesNotContain(encoded.substring(0, 24))
                .doesNotContain(String.valueOf(List.of(tls.key())));
    }
}
