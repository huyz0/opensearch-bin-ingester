// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Peer TLS fixtures read, and a JDK client presenting one (ADR-0084; tests only). */
final class PeerTestClients {

    private PeerTestClients() {
    }

    static String fixture(String name) throws Exception {
        return Path.of(PeerTestClients.class.getResource("/peer-tls/" + name).toURI())
                .toString();
    }

    /** {@code name}'s certificate and key, under the domain's CA. */
    static PeerTls read(String name) throws Exception {
        return Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 9443, Optional.of(
                new PeerConfig.Files(fixture(name + ".pem"), fixture(name + ".key"),
                        fixture("ca.pem")))), message -> { }).orElseThrow();
    }

    /** A client trusting the domain's CA and presenting {@code name}'s certificate. */
    static HttpClient client(String name) throws Exception {
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

    /** A {@code mutual} node as pod1, its peer listener on an ephemeral port. */
    static IngesterNode mutualPod1() throws Exception {
        java.util.Map<String, String> settings = new java.util.HashMap<>();
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
        ServerConfig config = ServerProperties.parse(settings);
        return IngesterNode.start(config, java.time.Clock.systemUTC(), System::nanoTime,
                Optional::empty, java.util.List.of(),
                Main.peerTls(config.peer(), message -> { }));
    }
}
