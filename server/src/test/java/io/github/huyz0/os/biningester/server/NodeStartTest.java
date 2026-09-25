// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.helidon.webclient.api.WebClient;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The process starts from a config FILE and serves {@code _bulk} (M8.4, FR-1).
 *
 * <p>⚠️ **THIS IS {@code Main.run}, NOT A HAND-BUILT GRAPH.** The path a test
 * takes and the path a deployment takes are the same call, off a real file —
 * which is the property ADR-0052 §3 puts this row before the chaos matrix for:
 * a matrix that kills a process nothing else exercises proves nothing about the
 * one that ships.
 *
 * <p>⚠️ **OVER {@code memory}, AND THE STORE ASSERTION IS THEREFORE WEAK.** A
 * node that acked from its accumulator and answered a read from the same JVM
 * would be green here, which is exactly criterion 1's named mutation. The
 * strong form is {@code AssembledWriteReadIT}, which looks at a real bucket
 * through a SECOND, independently-opened client. What this file buys that the
 * IT does not is that it runs with no Docker: the wiring — file to config to
 * graph to listener to 202 — is checked on every {@code ./gradlew test}.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NodeStartTest {

    private static final String INDEX = "logs";

    @TempDir
    Path dir;

    private Path configFile(String... extra) throws Exception {
        Path file = dir.resolve("node.properties");
        StringBuilder text = new StringBuilder(String.join("\n",
                "pod.id=pod1",
                "pod.uid=uid-pod1",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=memory",
                // ⚠️ THE PEER ENDPOINT AND THE LISTENER ARE DIFFERENT SETTINGS
                // and this file is where they are allowed to disagree: nothing
                // here forwards, and port 0 is what keeps two runs from
                // colliding (testing.md rule 16).
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                ""));
        for (String line : extra) {
            text.append(line).append('\n');
        }
        Files.write(file, text.toString().getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String indexUuid() {
        UUID uuid = UUID.randomUUID();
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(buffer.array());
    }

    private static String bulkBody(int records) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"doc-").append(i).append("\",\"_version\":1}}\n")
                    .append("{\"n\":").append(i).append("}\n");
        }
        return body.toString();
    }

    @Test
    void aNODEStartsFromAFileACCEPTSAWriteAndTheSEGMENTIsInTheStoreItWasCONFIGUREDWith()
            throws Exception {
        try (IngesterNode node = Main.run(configFile().toString())) {
            // ⚠️ THE KERNEL'S PORT, ASKED OF THE SERVER. A node that echoed the
            // configured 0 back would have this test dial port 0.
            assertThat(node.port()).isGreaterThan(0);

            node.assembly().catalog().register(
                    new IndexRegistration(indexUuid(), INDEX, List.of(), 4, 4, 1, 1));

            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();
            assertThat(client.post("/" + INDEX + "/_bulk").queryParam("partition", "3")
                    .submit(bulkBody(100)).status().code())
                    .as("⚠️ 202 ONLY AFTER DURABLE. `BulkService` answers once `append` has "
                            + "RETURNED, and `append` returns once the segment AND its commit "
                            + "delta are in the store")
                    .isEqualTo(202);

            assertThat(node.assembly().store()
                    .list("bins/cluster-a/data/", null, 100).objects())
                    .as("⚠️ A SEGMENT UNDER THE CONFIGURED PREFIX. A listener wired to an "
                            + "ingest over a different store answers 202 just as happily")
                    .isNotEmpty();
            assertThat(node.assembly().store()
                    .list("bins/cluster-a/ctl/log/", null, 100).objects())
                    .as("⚠️ AND THE COMMIT LOG, which is the sequencer's own path and not "
                            + "one the writer touches -- the two halves share a prefix")
                    .isNotEmpty();
        }
    }

    @Test
    void aWRITEToAnIndexTheCONFIGUREDPrincipalMayNOTTouchIsREFUSED() throws Exception {
        // ⚠️ THE ONE SECURITY PROPERTY THE WRITE PATH HAS. A root that defaulted
        // the allow-list to "all" would answer 202 here, and nothing downstream
        // would ever say so.
        try (IngesterNode node = Main.run(configFile().toString())) {
            node.assembly().catalog().register(
                    new IndexRegistration(indexUuid(), "secrets", List.of(), 4, 4, 1, 1));

            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();
            assertThat(client.post("/secrets/_bulk").queryParam("partition", "0")
                    .submit(bulkBody(1)).status().code())
                    .isEqualTo(403);
        }
    }

    @Test
    void theCOMMITEndpointIsSERVEDAndAnswersFromTheTERMThisNodeHOLDS() throws Exception {
        // ⚠️ A ROUTE THAT IS NOT REGISTERED ANSWERS 404, AND A 404 IS NOT A 409.
        // `HttpSequencerTransport` maps 409 to "not the leaseholder" and
        // anything else to an AMBIGUOUS `IOException` -- so a front door that
        // forgot to register the commit service turns every forwarded commit
        // into an outcome nobody knows, which is the one state a caller may not
        // resend from.
        try (IngesterNode node = Main.run(configFile().toString())) {
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();

            assertThat(client.post(io.github.huyz0.os.biningester.http.HttpSequencerTransport.PATH)
                    .submit(new byte[] {1, 2, 3}).status().code())
                    .as("⚠️ NOT 404. The route exists; the BYTES are refused")
                    .isNotEqualTo(404);
        }
    }

    @Test
    void theSUBSCRIPTIONEndpointIsSERVEDOnTheSAMEListener() throws Exception {
        // ⚠️ ONE LISTENER, because the `endpoint` a lease publishes is ONE
        // address: the peer forwards to it and the consumer subscribes to it.
        try (IngesterNode node = Main.run(configFile().toString())) {
            String uuid = indexUuid();
            node.assembly().catalog().register(
                    new IndexRegistration(uuid, INDEX, List.of(), 4, 4, 1, 1));

            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();
            // ⚠️ THE PATH CARRIES THE STREAM's UUID, NOT THE INDEX's BASE64URL
            // ONE. `RunKey.ofIndexUuid` owns that decode (M7.2) and a second
            // decoder that disagreed would not throw -- it would make two
            // streams for one index and the consumer's would never move.
            String stream = io.github.huyz0.os.biningester.format.RunKey.ofIndexUuid(uuid, 0).indexId().toString();
            assertThat(client.get("/sub/" + stream + "/0")
                    .queryParam("wait", "1").queryParam("sub", "s-1")
                    .request().status().code())
                    .as("⚠️ A LONG POLL THAT FINDS NOTHING IS STILL AN ANSWER, not a 404")
                    .isEqualTo(200);
        }
    }

    @Test
    void aMISSINGConfigFileIsREFUSEDAsAConfigurationExceptionRatherThanAnIOException() {
        // ⚠️ THE TYPE IS THE EXIT CODE. `Main` prints a message and exits 3 for
        // a `ConfigurationException` and a STACK TRACE for anything else; an
        // operator who mistyped a mount path must get the first.
        assertThatThrownBy(() -> Main.run(dir.resolve("absent.properties").toString()))
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void aFRONTDoorThatCannotBINDRELEASESTheTermItAlreadyTook() throws Exception {
        // ⚠️ THE ORDER IS TAKE-THE-TERM, THEN BIND, and between those two lines
        // the lease names a node that is about to fail to start. Without the
        // unwind in `IngesterNode.start`, a port already in use -- the ordinary
        // trigger, and exactly what a botched rollout produces -- leaves the
        // term held and RENEWED for the life of the JVM: the fleet stops
        // committing and nothing says why.
        //
        // ⚠️ `local-fs`, NOT `memory`, BECAUSE THE EVIDENCE MUST OUTLIVE THE
        // NODE. A `memory` store belongs to the assembly that opened it and
        // goes with it, so there would be nothing left to read the lease out of.
        int taken;
        try (ServerSocket socket = new ServerSocket(0)) {
            taken = socket.getLocalPort();

            Path root = dir.resolve("store");
            Files.createDirectories(root);
            Path file = dir.resolve("blocked.properties");
            Files.write(file, String.join("\n",
                    "pod.id=pod1",
                    "pod.uid=uid-pod1",
                    "pod.az=az-a",
                    "trust.domain=cluster-a",
                    "store.prefix=bins/cluster-a",
                    "store.kind=local-fs",
                    "store.root=" + root.toString().replace('\\', '/'),
                    "endpoint=http://pod1:8080",
                    "http.port=" + taken,
                    "producer.subject=producer-1",
                    "producer.allowed-indices=logs",
                    "").getBytes(StandardCharsets.UTF_8));

            assertThatThrownBy(() -> Main.run(file.toString()))
                    .as("the port is held by this test's own socket")
                    .isInstanceOf(RuntimeException.class);

            try (BinStore store = LocalFsBinStore.at(root.toString())) {
                // ⚠️ ON THE LEASE OBJECT'S OWN EXPIRY, which a release moves
                // into the past. The holder's NAME stays -- that is how a
                // successor knows whom it replaced -- so its absence cannot
                // carry this.
                assertThat(expiryOf(heldBy(store)))
                        .as("⚠️ THE TERM IS RELEASED. A node that failed to bind and kept "
                                + "its lease is renewed for ever by a process that serves "
                                + "nothing")
                        .isLessThanOrEqualTo(System.currentTimeMillis());
            }
        }

        // ⚠️ AND THE PORT IS FREE AFTERWARDS, so the failed start left no
        // listener of its own behind: an operator restarting into the same wall
        // would read it as the first failure repeating.
        try (ServerSocket reclaimed = new ServerSocket(taken)) {
            assertThat(reclaimed.getLocalPort()).isEqualTo(taken);
        }
    }

    /** What the lease object in {@code store} currently says, or {@code ""}. */
    private static String heldBy(BinStore store) throws Exception {
        var page = store.list("bins/cluster-a/ctl/lease/", null, 10);
        if (page.objects().isEmpty()) {
            return "";
        }
        try (var in = store.get(page.objects().get(0).key())) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** {@code expiresAtMillis} out of the lease object's JSON. */
    private static long expiryOf(String leaseJson) {
        var matcher = java.util.regex.Pattern.compile("\"expiresAtMillis\":(\\d+)")
                .matcher(leaseJson);
        if (!matcher.find()) {
            throw new AssertionError("no expiresAtMillis in: " + leaseJson);
        }
        return Long.parseLong(matcher.group(1));
    }

}
