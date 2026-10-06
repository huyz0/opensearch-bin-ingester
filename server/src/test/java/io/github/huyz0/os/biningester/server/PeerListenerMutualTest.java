// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The peer listener under {@code peer.tls = mutual} (ADR-0084; M13.52c): a
 * client presenting a certificate from the trust domain's CA is served; one
 * presenting none, or one from another CA, is refused at the handshake --
 * and the frame that would depose the pod, sent without a certificate,
 * leaves its fence where it was.
 */
class PeerListenerMutualTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(PeerListenerMutualTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
    }

    private static PeerTls read(String cert, String key) throws Exception {
        return Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 0, Optional.of(
                new PeerConfig.Files(fixture(cert), fixture(key), fixture("ca.pem")))),
                message -> { }).orElseThrow();
    }

    private IngesterNode start() throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "https://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "mutual");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.PEER_TLS_CERT, fixture("pod1.pem"));
        settings.put(ServerProperties.PEER_TLS_KEY, fixture("pod1.key"));
        settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC(),
                System::nanoTime, Optional::empty, List.of(),
                Optional.of(read("pod1.pem", "pod1.key")));
    }

    /** A client trusting the domain's CA, presenting {@code identity} if any. */
    private static HttpClient client(Optional<PeerTls> identity) throws Exception {
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", read("pod2.pem", "pod2.key").ca().get(0));
        TrustManagerFactory trusts = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trusts.init(trust);
        KeyManagerFactory keys = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        KeyStore own = KeyStore.getInstance("PKCS12");
        own.load(null, null);
        char[] password = "test".toCharArray();
        if (identity.isPresent()) {
            own.setKeyEntry("me", identity.get().key(), password,
                    identity.get().chain().toArray(new java.security.cert.Certificate[0]));
        }
        keys.init(own, password);
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keys.getKeyManagers(), trusts.getTrustManagers(), null);
        return HttpClient.newBuilder().sslContext(context)
                .connectTimeout(Duration.ofSeconds(5)).build();
    }

    private HttpResponse<byte[]> post(HttpClient client, byte[] body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("https://localhost:"
                        + node.peerPort() + "/ctl/fast"))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static byte[] join(long epoch) {
        return FastFrame.encode(epoch, POD.podUid(), "uid-pod1",
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    @Test
    void aCLIENTWithACertificateFromTheDomainsCAIsServed() throws Exception {
        node = start();

        HttpResponse<byte[]> answer = post(client(Optional.of(read("pod2.pem", "pod2.key"))),
                join(0));

        assertThat(answer.statusCode()).isEqualTo(200);
        assertThat(FastFrame.decode(answer.body()).header().senderUid()).isEqualTo("uid-pod1");
    }

    @Test
    void aCLIENTWithoutACertificateIsRefusedAtTheHandshake() throws Exception {
        node = start();
        HttpClient anonymous = client(Optional.empty());

        assertThatThrownBy(() -> post(anonymous, join(0))).isInstanceOf(IOException.class);
    }

    @Test
    void aCERTIFICATEFromAnotherCAIsRefusedAtTheHandshake() throws Exception {
        node = start();
        PeerTls foreign = new PeerTls(
                Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 0, Optional.of(
                        new PeerConfig.Files(fixture("foreign.pem"), fixture("foreign.key"),
                                fixture("foreign-ca.pem")))), message -> { })
                        .orElseThrow().chain(),
                Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 0, Optional.of(
                        new PeerConfig.Files(fixture("foreign.pem"), fixture("foreign.key"),
                                fixture("foreign-ca.pem")))), message -> { })
                        .orElseThrow().key(),
                read("pod2.pem", "pod2.key").ca());
        HttpClient impostor = client(Optional.of(foreign));

        assertThatThrownBy(() -> post(impostor, join(0))).isInstanceOf(IOException.class);
    }

    @Test
    void theDEPOSINGFrameWithoutACertificateLeavesTheFenceWhereItWas() throws Exception {
        node = start();
        HttpClient anonymous = client(Optional.empty());
        assertThatThrownBy(() -> post(anonymous, join(Long.MAX_VALUE)))
                .as("the attack of M13.52, refused").isInstanceOf(IOException.class);

        HttpResponse<byte[]> answer = post(client(Optional.of(read("pod2.pem", "pod2.key"))),
                join(0));

        assertThat(FastFrame.decode(answer.body()).header().epoch())
                .as("the fence still at the node's own term, never Long.MAX_VALUE")
                .isLessThan(Long.MAX_VALUE).isPositive();
    }
}
