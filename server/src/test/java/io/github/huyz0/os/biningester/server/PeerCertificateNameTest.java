// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.http.PeerIdentity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A pod's certificate names this pod, or the pod does not start (ADR-0084
 * decision 8; M13.52f): its one {@code spiffe://} URI SAN ends in the pod's
 * name and UID, under a prefix naming its fleet.
 */
class PeerCertificateNameTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(PeerCertificateNameTest.class.getResource("/peer-tls/" + name).toURI())
                .toString().replace('\\', '/');
    }

    private Map<String, String> settings(String cert, String podId, String podUid)
            throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, podId);
        settings.put(ServerProperties.POD_UID, podUid);
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "local-fs");
        settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString()
                .replace('\\', '/'));
        settings.put(ServerProperties.ENDPOINT, "https://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "mutual");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.PEER_TLS_CERT, fixture(cert + ".pem"));
        settings.put(ServerProperties.PEER_TLS_KEY, fixture(cert + ".key"));
        settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        return settings;
    }

    private IngesterNode start(String cert, String podId, String podUid) throws Exception {
        ServerConfig config = ServerProperties.parse(settings(cert, podId, podUid));
        return IngesterNode.start(config, Clock.systemUTC(), System::nanoTime, Optional::empty,
                List.of(), Main.peerTls(config.peer(), message -> { }));
    }

    @Test
    void aDASHEDPodNameNamesThePodIdWithoutIt() throws Exception {
        node = start("pod1", "pod1", "uid-pod1");

        assertThat(node.peerPort()).as("started: pod-1 names pod1").isPositive();
    }

    @Test
    void aCERTIFICATEWithNoSpiffeSanIsRefusedBeforeAnyTermIsTaken() throws Exception {
        assertThatThrownBy(() -> node = start("nosan", "pod1", "uid-pod1"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("0 spiffe:// URI SANs");
        assertThat(Files.exists(dir.resolve("store")) ? Files.list(dir.resolve("store")).toList()
                : List.of()).as("nothing written: no lease taken").isEmpty();
    }

    @Test
    void aCERTIFICATEWithTwoSpiffeSansIsRefused() {
        assertThatThrownBy(() -> node = start("twosan", "pod1", "uid-pod1"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("2 spiffe:// URI SANs");
    }

    @Test
    void anOTHERTrustDomainIsRefused() {
        assertThatThrownBy(() -> node = start("wrongdomain", "pod1", "uid-pod1"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("spiffe://cluster-b/ns/test/pod/pod-1/uid-pod1")
                .hasMessageContaining(ServerProperties.TRUST_DOMAIN);
    }

    @Test
    void anOTHERUidIsRefused() {
        assertThatThrownBy(() -> node = start("pod1", "pod1", "uid-pod9"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("uid-pod9").hasMessageContaining("uid-pod1");
    }

    @Test
    void anOTHERPodNameIsRefused() {
        assertThatThrownBy(() -> node = start("pod2", "pod1", "uid-pod2"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("pod-2").hasMessageContaining(ServerProperties.POD_ID);
    }

    @Test
    void aPREFIXThatIsOnlyTheTrustDomainIsRefused() {
        assertThatThrownBy(() -> node = start("bareprefix", "pod1", "uid-pod1"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT)
                .hasMessageContaining("names no fleet");
    }

    @Test
    void theIDENTITYSplitsThePrefixFromTheNameAndUid() throws Exception {
        PeerTls read = Main.peerTls(ServerProperties.parse(settings("otherns", "pod1",
                "uid-pod1")).peer(), message -> { }).orElseThrow();

        assertThat(read.identity()).isEqualTo(new PeerIdentity(
                "spiffe://cluster-a/ns/other/pod", "pod-1", "uid-pod1"));
        assertThat(read.identity().trustDomain()).isEqualTo("cluster-a");
        assertThat(read.identity().podId()).isEqualTo("pod1");
    }

    @Test
    void MAINRefusesToStartANodeWhoseCertificateNamesAnotherPod() throws Exception {
        Path config = dir.resolve("node.properties");
        List<String> lines = new java.util.ArrayList<>();
        settings("pod2", "pod1", "uid-pod1").forEach((key, value) -> lines.add(key + "=" + value));
        Files.writeString(config, String.join("\n", lines));

        assertThatThrownBy(() -> node = Main.run(config.toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CERT);
    }
}
