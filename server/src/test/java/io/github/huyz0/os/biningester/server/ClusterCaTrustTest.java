// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.helidon.common.tls.Tls;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code membership.ca-file} reaches the watch a node starts (M8.51, NFR-9).
 *
 * <p>⚠️ **THE SETTING, THE READ AND THE WIRING ARE THREE PLACES TO DROP IT**:
 * a key parsed and never read, a file read and never handed on, a list handed
 * to a constructor that ignores it. Each case below is one of them, and the
 * last starts a whole node against a TLS API server it can trust only through
 * the file.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ClusterCaTrustTest {

    private static final String PASS = "changeit";

    @TempDir
    Path dir;

    private WebServer api;
    private IngesterNode node;
    private volatile boolean stopped;

    @AfterEach
    void stop() throws Exception {
        stopped = true;
        if (node != null) {
            node.close();
        }
        if (api != null) {
            api.stop();
        }
    }

    private static Map<String, String> settings() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    /** A self-signed localhost key pair, and its certificate as a PEM file. */
    private Path[] mint() throws Exception {
        Path keystore = dir.resolve("api.p12");
        Path pem = dir.resolve("ca.crt");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        for (String[] command : new String[][] {
                {keytool, "-genkeypair", "-alias", "api", "-keyalg", "EC", "-groupname",
                        "secp256r1", "-dname", "CN=localhost", "-ext",
                        "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2", "-keystore",
                        keystore.toString(), "-storepass", PASS, "-storetype", "PKCS12"},
                {keytool, "-exportcert", "-rfc", "-alias", "api", "-keystore",
                        keystore.toString(), "-storepass", PASS, "-file", pem.toString()}}) {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            assertThat(p.waitFor()).as("the premise: keytool ran").isZero();
        }
        return new Path[] {keystore, pem};
    }

    private String tlsApi(Path keystore) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            store.load(in, PASS.toCharArray());
        }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, PASS.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        api = WebServer.builder().port(0).tls(Tls.builder().sslContext(context).build())
                .routing(HttpRouting.builder().get(
                        "/apis/discovery.k8s.io/v1/namespaces/ingest/endpointslices",
                        (req, res) -> {
                            try (OutputStream out = res.outputStream()) {
                                out.write("{\"type\":\"BOOKMARK\",\"object\":{}}\n"
                                        .getBytes(StandardCharsets.UTF_8));
                                out.flush();
                                while (!stopped) {
                                    Thread.sleep(50);
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            } catch (java.io.IOException gone) {
                                // the watch closed its end
                            }
                        })).build().start();
        return "https://localhost:" + api.port();
    }

    @Test
    void theCAFileIsPARSEDAndNeedsAnAPIServer() {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.MEMBERSHIP_API, "https://kubernetes.default.svc");
        settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");
        settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");
        settings.put(ServerProperties.MEMBERSHIP_CA_FILE, "/var/run/ca.crt");
        assertThat(ServerProperties.parse(settings).membership().orElseThrow().caFile())
                .contains("/var/run/ca.crt");

        Map<String, String> alone = settings();
        alone.put(ServerProperties.MEMBERSHIP_CA_FILE, "/var/run/ca.crt");
        assertThatThrownBy(() -> ServerProperties.parse(alone))
                .as("a CA for a watch that will never run is a half-written setting")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MEMBERSHIP_CA_FILE);
    }

    @Test
    void mainREADSThePEMAndREFUSESWhatIsNotOneNamingTheKeyAndPath() throws Exception {
        Path pem = mint()[1];
        assertThat(Main.certificatesIn(pem.toString())).hasSize(1);

        Path garbage = Files.writeString(dir.resolve("garbage.crt"), "not a certificate");
        assertThatThrownBy(() -> Main.certificatesIn(garbage.toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MEMBERSHIP_CA_FILE)
                .hasMessageContaining("garbage.crt");
        assertThatThrownBy(() -> Main.certificatesIn(dir.resolve("absent.crt").toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("absent.crt");
    }

    @Test
    void aNODEStartedWithTheCATrustsItsTLSAPIServer() throws Exception {
        Path[] minted = mint();
        Map<String, String> settings = settings();
        settings.put(ServerProperties.MEMBERSHIP_API, tlsApi(minted[0]));
        settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");
        settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");
        ServerConfig config = ServerProperties.parse(settings);

        node = IngesterNode.start(config, Clock.systemUTC(), Optional::empty,
                Main.certificatesIn(minted[1].toString()));

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (node.watchConnections() == 0) {
            assertThat(System.nanoTime()).as("⚠️ THE NODE's WATCH CONNECTED OVER TLS")
                    .isLessThan(deadline);
            Thread.sleep(20);
        }
    }
}
