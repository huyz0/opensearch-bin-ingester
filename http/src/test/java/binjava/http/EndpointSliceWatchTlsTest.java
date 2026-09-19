// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.Lease;
import io.helidon.common.tls.Tls;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The watch trusts the cluster's own CA, and only because it was given it
 * (M8.51, NFR-9).
 *
 * <p>⚠️ **AN IN-CLUSTER API SERVER's CERTIFICATE IS SIGNED BY THE CLUSTER CA**,
 * which is not in the JVM's default trust store. Without a way to trust it the
 * watch fails its handshake on every connect, produces no evidence, and
 * failover falls back to the TTL -- NFR-9 unmet in a real cluster while every
 * plain-HTTP test is green. So the fake here speaks TLS, under a certificate
 * minted for the case.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class EndpointSliceWatchTlsTest {

    private static final Lease POD0 = new Lease(3, "pod0", "http://10.0.0.1:8080",
            Long.MAX_VALUE);
    private static final String PASS = "changeit";

    @TempDir
    Path dir;

    private WebServer server;
    private EndpointSliceWatch watch;
    private volatile boolean stopped;

    @AfterEach
    void stop() {
        stopped = true;
        if (watch != null) {
            watch.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    /** A self-signed certificate for localhost, minted by the JDK's own keytool. */
    private Path keystore() throws Exception {
        Path keystore = dir.resolve("api.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin",
                "keytool").toString(), "-genkeypair", "-alias", "api", "-keyalg", "EC",
                "-groupname", "secp256r1", "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
                "-keystore", keystore.toString(), "-storepass", PASS, "-storetype", "PKCS12")
                .redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertThat(p.waitFor()).as("the premise: keytool minted a certificate").isZero();
        return keystore;
    }

    private static KeyStore load(Path keystore) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            store.load(in, PASS.toCharArray());
        }
        return store;
    }

    /** A TLS endpoint that serves one ADDED event and then a removal. */
    private String tlsApi(Path keystore) throws Exception {
        KeyManagerFactory keys = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keys.init(load(keystore), PASS.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        server = WebServer.builder().port(0).tls(Tls.builder().sslContext(context).build())
                .routing(HttpRouting.builder().get(
                        "/apis/discovery.k8s.io/v1/namespaces/ingest/endpointslices",
                        (req, res) -> {
                            try (OutputStream out = res.outputStream()) {
                                out.write((EndpointSliceViewTest.event("ADDED", "pod0",
                                        "10.0.0.1", true, false) + "\n"
                                        + EndpointSliceViewTest.empty("MODIFIED") + "\n")
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
        return "https://localhost:" + server.port();
    }

    private static X509Certificate certificateOf(Path keystore) throws Exception {
        // ⚠️ THROUGH PEM BYTES, as the ca.crt file of a service account holds it
        byte[] der = load(keystore).getCertificate("api").getEncoded();
        String pem = "-----BEGIN CERTIFICATE-----\n"
                + java.util.Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(der) + "\n-----END CERTIFICATE-----\n";
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(
                new java.io.ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
    }

    private static void await(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("never: " + what);
            }
            Thread.sleep(20);
        }
    }

    @Test
    void aWATCHGivenTheClusterCAConnectsAndFEEDSTheView() throws Exception {
        Path keystore = keystore();
        EndpointSliceView view = new EndpointSliceView();
        watch = new EndpointSliceWatch(tlsApi(keystore), "ingest", "ingester", Optional::empty,
                view, List.of(certificateOf(keystore))).start();

        await(() -> view.holderGone(POD0), "⚠️ EVIDENCE, OVER TLS, FROM A TRUSTED CA");
        assertThat(watch.connections()).isEqualTo(1);
    }

    @Test
    void aWATCHNotGivenItFAILSItsHandshakeAndSeesNothing() throws Exception {
        Path keystore = keystore();
        EndpointSliceView view = new EndpointSliceView();
        watch = new EndpointSliceWatch(tlsApi(keystore), "ingest", "ingester", Optional::empty,
                view, List.of()).start();

        await(() -> watch.failures() >= 1, "a failed connect");
        assertThat(watch.connections())
                .as("the premise the row states: the JVM's own trust store refuses it")
                .isZero();
        assertThat(view.holderGone(POD0)).isFalse();
    }
}
