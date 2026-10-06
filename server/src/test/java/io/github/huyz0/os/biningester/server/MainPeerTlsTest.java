// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The peer certificate files read and checked at start (ADR-0084; M13.52b):
 * a missing or unreadable one refuses the start, naming its key and path;
 * {@code off} reads nothing and warns, naming the four routes it leaves open.
 */
class MainPeerTlsTest {

    private static String fixture(String name) throws URISyntaxException {
        return Path.of(MainPeerTlsTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
    }

    private static PeerConfig mutual(String cert, String key, String ca) {
        return new PeerConfig(PeerConfig.Mode.MUTUAL, 9443,
                Optional.of(new PeerConfig.Files(cert, key, ca)));
    }

    @Test
    void MUTUALReadsThePodsChainItsKeyAndTheCA() throws Exception {
        List<String> warned = new ArrayList<>();

        Optional<PeerTls> tls = Main.peerTls(mutual(fixture("pod1.pem"), fixture("pod1.key"),
                fixture("ca.pem")), warned::add);

        assertThat(tls).hasValueSatisfying(read -> {
            assertThat(read.chain()).hasSize(1);
            assertThat(read.chain().get(0).getSubjectX500Principal().getName())
                    .contains("CN=pod1");
            assertThat(read.key().getAlgorithm()).isEqualTo("EC");
            assertThat(read.ca()).hasSize(1);
        });
        assertThat(warned).isEmpty();
    }

    @Test
    void aMISSINGFileRefusesTheStartNamingItsKeyAndPath() throws Exception {
        String missing = fixture("ca.pem").replace("ca.pem", "absent.pem");

        assertThatThrownBy(() -> Main.peerTls(mutual(fixture("pod1.pem"), fixture("pod1.key"),
                missing), message -> { }))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CA)
                .hasMessageContaining(missing);
    }

    @Test
    void aKEYFileHoldingNoKeyRefusesTheStart() throws Exception {
        assertThatThrownBy(() -> Main.peerTls(mutual(fixture("pod1.pem"), fixture("pod1.pem"),
                fixture("ca.pem")), message -> { }))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_KEY);
    }

    @Test
    void aCERTFileHoldingNoCertificateRefusesTheStart() throws Exception {
        assertThatThrownBy(() -> Main.peerTls(mutual(fixture("pod1.key"), fixture("pod1.key"),
                fixture("ca.pem")), message -> { }))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT);
    }

    @Test
    void OFFReadsNothingAndWarnsNamingTheFourRoutes() {
        List<String> warned = new ArrayList<>();

        Optional<PeerTls> tls = Main.peerTls(PeerConfig.off(0), warned::add);

        assertThat(tls).isEmpty();
        assertThat(warned).singleElement().asString()
                .contains("/ctl/commit", "/ctl/drain", "/ctl/durable-segment", "/ctl/fast");
    }
}
