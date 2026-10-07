// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitRequestFrame;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
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
 * Every peer route binds its caller to the client certificate (ADR-0084
 * decision 8; M13.52g): a correctly chained certificate for the wrong pod, or
 * for this pod's name under another fleet, is refused {@code 403}; the fast
 * route refuses before the fence admits; under {@code off} nothing is bound.
 */
class PeerRouteBindingTest {

    private static final Roster.Incarnation POD2 =
            new Roster.Incarnation("pod2", "uid-pod2", "az-b", "");
    private static final Roster.Incarnation POD1 =
            new Roster.Incarnation("pod1", "uid-pod1", "az-a", "");

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(PeerRouteBindingTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
    }

    private static PeerTls read(String name) throws Exception {
        return Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 9443, Optional.of(
                new PeerConfig.Files(fixture(name + ".pem"), fixture(name + ".key"),
                        fixture("ca.pem")))), message -> { }).orElseThrow();
    }

    private void start(String tls) throws Exception {
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
        settings.put(ServerProperties.PEER_PORT, "0");
        if (tls.equals("mutual")) {
            settings.put(ServerProperties.PEER_TLS_CERT, fixture("pod1.pem"));
            settings.put(ServerProperties.PEER_TLS_KEY, fixture("pod1.key"));
            settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        }
        ServerConfig config = ServerProperties.parse(settings);
        node = IngesterNode.start(config, Clock.systemUTC(), System::nanoTime, Optional::empty,
                List.of(), Main.peerTls(config.peer(), message -> { }));
    }

    /** A client trusting the domain's CA and presenting {@code name}'s certificate. */
    private static HttpClient client(String name) throws Exception {
        PeerTls own = read(name);
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
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return HttpClient.newBuilder().sslContext(context)
                .connectTimeout(Duration.ofSeconds(5)).build();
    }

    private HttpResponse<byte[]> post(HttpClient client, String scheme, String path,
            byte[] body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(scheme + "://localhost:"
                        + node.peerPort() + path)).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static byte[] join(long epoch, String senderUid, Roster.Incarnation as) {
        return FastFrame.encode(epoch, senderUid, "uid-pod1",
                new FastFrame.Join(as, FastFrame.Held.NONE));
    }

    private static byte[] commit(String pod) {
        return new CommitRequestFrame(pod, "inc", 0, "seg/" + pod,
                Map.of(new RunKey(new java.util.UUID(1, 1), 0), 1)).encode();
    }

    @Test
    void aFRAMEClaimingAnotherSendersUidIsRefusedAndLeavesTheFenceUnmoved() throws Exception {
        start("mutual");
        HttpClient pod2 = client("pod2");

        assertThat(post(pod2, "https", "/ctl/fast", join(Long.MAX_VALUE, "uid-pod1", POD1))
                .statusCode()).as("pod2's certificate, uid-pod1's frame").isEqualTo(403);

        HttpResponse<byte[]> own = post(pod2, "https", "/ctl/fast", join(0, "uid-pod2", POD2));
        assertThat(own.statusCode()).isEqualTo(200);
        assertThat(FastFrame.decode(own.body()).header().epoch())
                .as("the fence never Long.MAX_VALUE").isLessThan(Long.MAX_VALUE);
    }

    @Test
    void aNONJoinFrameClaimingAnotherSendersUidIsRefused() throws Exception {
        start("mutual");

        // ⚠️ A HELD REPORT carries no incarnation: only the sender UID binds it.
        assertThat(post(client("pod2"), "https", "/ctl/fast", FastFrame.encode(Long.MAX_VALUE,
                "uid-pod1", "uid-pod1", new FastFrame.HeldReport(FastFrame.Held.NONE)))
                .statusCode()).isEqualTo(403);
    }

    @Test
    void aJOINClaimingAnotherIncarnationIsRefused() throws Exception {
        start("mutual");
        HttpClient pod2 = client("pod2");

        assertThat(post(pod2, "https", "/ctl/fast", join(0, "uid-pod2",
                new Roster.Incarnation("pod9", "uid-pod2", "az-b", ""))).statusCode())
                .as("another pod.id").isEqualTo(403);
        assertThat(post(pod2, "https", "/ctl/fast", join(0, "uid-pod2",
                new Roster.Incarnation("pod2", "uid-pod9", "az-b", ""))).statusCode())
                .as("another UID").isEqualTo(403);
    }

    @Test
    void aCOMMITClaimingAnotherPodIsRefused() throws Exception {
        start("mutual");
        HttpClient pod2 = client("pod2");

        assertThat(post(pod2, "https", "/ctl/commit", commit("pod1")).statusCode())
                .isEqualTo(403);
        assertThat(post(pod2, "https", "/ctl/commit", commit("pod2")).statusCode())
                .as("its own pod: past the binding").isNotEqualTo(403);
    }

    @Test
    void aDRAINByAnotherRequesterOrByNoneIsRefused() throws Exception {
        start("mutual");
        HttpClient pod2 = client("pod2");

        assertThat(post(pod2, "https", "/ctl/drain?pod=pod1", new byte[0]).statusCode())
                .as("another requester").isEqualTo(403);
        assertThat(post(pod2, "https", "/ctl/drain", new byte[0]).statusCode())
                .as("no requester").isEqualTo(403);
        assertThat(post(pod2, "https", "/ctl/drain?pod=pod2", new byte[0]).statusCode())
                .as("its own: past the binding").isNotEqualTo(403);
    }

    @Test
    void thisPODsNameUnderAnotherFleetIsRefusedByEveryRoute() throws Exception {
        start("mutual");
        HttpClient other = client("otherns");

        assertThat(post(other, "https", "/ctl/fast", join(0, "uid-pod1", POD1)).statusCode())
                .as("/ctl/fast").isEqualTo(403);
        assertThat(post(other, "https", "/ctl/commit", commit("pod1")).statusCode())
                .as("/ctl/commit").isEqualTo(403);
        assertThat(post(other, "https", "/ctl/drain?pod=pod1", new byte[0]).statusCode())
                .as("/ctl/drain").isEqualTo(403);
    }

    @Test
    void underOFFNothingIsBound() throws Exception {
        start("off");
        HttpClient plain = HttpClient.newHttpClient();

        assertThat(post(plain, "http", "/ctl/commit", commit("anyone")).statusCode())
                .isNotEqualTo(403);
        assertThat(post(plain, "http", "/ctl/drain", new byte[0]).statusCode())
                .isNotEqualTo(403);
        assertThat(post(plain, "http", "/ctl/fast", join(0, "uid-anyone",
                new Roster.Incarnation("anyone", "uid-anyone", "az-b", ""))).statusCode())
                .isNotEqualTo(403);
    }
}
