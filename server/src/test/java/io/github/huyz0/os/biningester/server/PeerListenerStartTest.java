// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.CatchUpService;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
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
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The peer listener as a node starts it (M13.52c review round 1, T1-T3, P2,
 * P3): Main's certificates reach it; a {@code mutual} node started without
 * them refuses; a peer port already held refuses the start by its own key;
 * TLS 1.2 is refused; and the consumer's catch-up and fetch stay where they
 * were.
 */
class PeerListenerStartTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

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
        return Path.of(PeerListenerStartTest.class.getResource("/peer-tls/" + name).toURI())
                .toString().replace('\\', '/');
    }

    private static Map<String, String> settings(String tls, String peerPort) throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, (tls.equals("mutual") ? "https" : "http")
                + "://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, tls);
        settings.put(ServerProperties.PEER_PORT, peerPort);
        if (tls.equals("mutual")) {
            settings.put(ServerProperties.PEER_TLS_CERT, fixture("pod1.pem"));
            settings.put(ServerProperties.PEER_TLS_KEY, fixture("pod1.key"));
            settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        }
        return settings;
    }

    private static HttpClient tlsClient(String protocol) throws Exception {
        PeerTls own = Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 0, Optional.of(
                new PeerConfig.Files(fixture("pod2.pem"), fixture("pod2.key"),
                        fixture("ca.pem")))), message -> { }).orElseThrow();
        KeyStore keys = KeyStore.getInstance("PKCS12");
        keys.load(null, null);
        keys.setKeyEntry("me", own.key(), "t".toCharArray(),
                own.chain().toArray(new java.security.cert.Certificate[0]));
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", own.ca().get(0));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keys, "t".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        SSLParameters only = new SSLParameters();
        only.setProtocols(new String[] {protocol});
        return HttpClient.newBuilder().sslContext(context).sslParameters(only)
                .connectTimeout(Duration.ofSeconds(5)).build();
    }

    private static HttpResponse<byte[]> join(HttpClient client, String scheme, int port)
            throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(scheme + "://localhost:" + port
                        + "/ctl/fast")).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(FastFrame.encode(0, POD.podUid(),
                        "uid-pod1", new FastFrame.Join(POD, FastFrame.Held.NONE)))).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void MAINsCertificatesReachThePeerListener() throws Exception {
        Path config = dir.resolve("node.properties");
        List<String> lines = new java.util.ArrayList<>();
        settings("mutual", "0").forEach((key, value) -> lines.add(key + "=" + value));
        Files.writeString(config, String.join("\n", lines));
        node = Main.run(config.toString());

        assertThat(join(tlsClient("TLSv1.3"), "https", node.peerPort()).statusCode())
                .as("served over mutual TLS with the certificates Main read").isEqualTo(200);
        assertThatThrownBy(() -> join(HttpClient.newHttpClient(), "http", node.peerPort()))
                .as("never in plaintext").isInstanceOf(IOException.class);
    }

    @Test
    void aMUTUALNodeStartedWithoutItsCertificatesRefuses() throws Exception {
        ServerConfig config = ServerProperties.parse(settings("mutual", "0"));

        assertThatThrownBy(() -> node = IngesterNode.start(config, Clock.systemUTC()))
                .hasMessageContaining("MUTUAL");
        assertThat(node).isNull();
    }

    @Test
    void aPEERPortAlreadyHeldRefusesTheStartByItsOwnKey() throws Exception {
        try (ServerSocket held = new ServerSocket(0)) {
            ServerConfig config = ServerProperties.parse(
                    settings("off", Integer.toString(held.getLocalPort())));

            assertThatThrownBy(() -> node = IngesterNode.start(config, Clock.systemUTC()))
                    .hasMessageContaining(ServerProperties.PEER_PORT)
                    .hasMessageContaining(Integer.toString(held.getLocalPort()));
        }
    }

    @Test
    void aTLS12ClientIsRefusedAtTheHandshake() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings("mutual", "0")),
                Clock.systemUTC(), System::nanoTime, Optional::empty, List.of(),
                Main.peerTls(ServerProperties.parse(settings("mutual", "0")).peer(),
                        message -> { }));

        assertThatThrownBy(() -> join(tlsClient("TLSv1.2"), "https", node.peerPort()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void theCONSUMERsCatchUpAndFetchStayOnTheProducerPort() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings("off", "0")),
                Clock.systemUTC());
        String catchUp = CatchUpService.PATH.replace("{indexUuid}", "AAAAAAAAAAAAAAAAAAAAAA")
                .replace("{partition}", "0");

        for (int port : new int[] {node.port(), node.peerPort()}) {
            WebClient client = WebClient.builder().baseUri("http://localhost:" + port).build();
            try (HttpClientResponse up = client.post(catchUp).submit(new byte[] {1});
                    HttpClientResponse fetch = client.get(SegmentFetchRoute.PATH).request()) {
                boolean producer = port == node.port();
                assertThat(up.status().code() == 404).as("catch-up on %s", port)
                        .isEqualTo(!producer);
                assertThat(fetch.status().code() == 404).as("/seg on %s", port)
                        .isEqualTo(!producer);
            }
        }
    }
}
