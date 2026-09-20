// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.format.IndexRegistration;
import binjava.http.DrainGate;
import binjava.http.HealthService;
import binjava.http.SubscriptionService;
import binjava.server.ShutdownSequence.Step;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A real node shuts down in research 08 §7's order with a subscriber, a
 * request inside the door and a held term (M8.7, criterion 5).
 *
 * <p>⚠️ **THE INTERVAL IS PINNED AT 5 s, SO A REQUEST INSIDE THE DOOR IS
 * WAITING FOR A FLUSH THAT IS NOT DUE.** Its 202 arriving well inside those 5
 * s is what shows the drain flushed it, rather than waiting for the flusher.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NodeShutdownTest {

    @TempDir
    Path dir;

    private Path configFile(Path root) throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                // ⚠️ `local-fs`, because the evidence (the segment and the
                // released lease) must outlive the node.
                "store.kind=local-fs",
                "store.root=" + root,
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                ServerProperties.INTERVAL_FLOOR + "=PT5S",
                "").getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String bulkLine(String id) {
        return "{\"index\":{\"_id\":\"" + id + "\",\"_version\":1}}\n{\"n\":1}\n";
    }

    @Test
    void aNODEWithASubscriberARequestInsideAndATermShutsDownInORDERAndLosesNOTHING()
            throws Exception {
        Path root = dir.resolve("store");
        Files.createDirectories(root);
        IngesterNode node = Main.run(configFile(root).toString());
        CompletableFuture<Integer> poll;
        CompletableFuture<Integer> inside;
        long closing;
        try {
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();
            UUID uuid = UUID.randomUUID();
            var buffer = java.nio.ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            String indexUuid = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(buffer.array());
            assertThat(client.post(SubscriptionService.REGISTER_PATH)
                    .submit(new IndexRegistration(indexUuid, "logs", List.of(), 4, 4, 1, 1)
                            .encode())
                    .status().code()).isEqualTo(204);
            // ⚠️ A FIRST WRITE, SO THE NODE HOLDS A TERM TO RELEASE. It elects
            // on its first commit.
            assertThat(client.post("/logs/_bulk").queryParam("partition", "0")
                    .submit(bulkLine("first")).status().code()).isEqualTo(202);
            assertThat(node.assembly().leading()).as("the premise: a term is held").isTrue();

            poll = CompletableFuture.supplyAsync(() -> {
                try (var response = client.get("/sub/" + uuid + "/0")
                        .queryParam("wait", "30").queryParam("sub", "s1").request()) {
                    return response.status().code();
                }
            });
            inside = CompletableFuture.supplyAsync(() -> client.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(bulkLine("inside")).status().code());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (node.gate().bulkInFlight() == 0
                    || node.gate().awaitNoPollers(Duration.ZERO) == 0) {
                assertThat(System.nanoTime()).as("the poll and the bulk never both arrived")
                        .isLessThan(deadline);
                Thread.onSpinWait();
            }
            closing = System.nanoTime();
        } finally {
            node.close();
        }

        assertThat(poll.get(20, TimeUnit.SECONDS))
                .as("⚠️ THE SUBSCRIBER WAS TOLD TO GO, with a 503 it reconnects on")
                .isEqualTo(503);
        assertThat(inside.get(20, TimeUnit.SECONDS))
                .as("⚠️ THE REQUEST INSIDE WAS ANSWERED, NOT CUT OFF").isEqualTo(202);
        assertThat(Duration.ofNanos(System.nanoTime() - closing))
                .as("⚠️ AND THE DRAIN FLUSHED IT: the flusher was not due for 5 s")
                .isLessThan(Duration.ofMillis(2500));

        assertThat(node.shutdownJournal())
                .as("⚠️ THE OBSERVED ORDER, written by the switches, the flush, the listener "
                        + "and the graph as they moved -- not by the sequence's own report, "
                        + "which lists its steps in order whatever they did")
                .containsSubsequence(DrainGate.READINESS_FAILED, DrainGate.POLLS_RELEASED,
                        DrainGate.BULK_REFUSED, Assembly.FLUSHED, FrontDoor.LISTENER_STOPPED,
                        Assembly.FLUSHED, Assembly.GRAPH_CLOSED)
                .endsWith(Assembly.GRAPH_CLOSED);
        assertThat(node.shutdownJournal().indexOf(Assembly.GRAPH_CLOSED))
                .as("⚠️ THE LEASE GOES LAST, and exactly once")
                .isEqualTo(node.shutdownJournal().size() - 1);

        ShutdownSequence.Report report = node.lastShutdown();
        assertThat(report.failures()).isEmpty();
        assertThat(report.took()).containsOnlyKeys(Step.values());
        assertThat(report.total()).as("⚠️ §7's budget, measured on a real drain")
                .isLessThan(Duration.ofSeconds(30));

        try (BinStore store = LocalFsBinStore.at(root.toString())) {
            assertThat(store.list("bins/cluster-a/data/", null, 100).objects())
                    .as("⚠️ WHAT WAS ACKED IS IN THE STORE: both writes, flushed apart")
                    .hasSizeGreaterThanOrEqualTo(2);
            var lease = store.list("bins/cluster-a/ctl/lease/", null, 10).objects();
            assertThat(lease).as("the premise: there is a lease object").isNotEmpty();
            String text;
            try (var in = store.get(lease.get(0).key())) {
                text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            var matcher = java.util.regex.Pattern.compile("\"expiresAtMillis\":(\\d+)")
                    .matcher(text);
            assertThat(matcher.find()).isTrue();
            assertThat(Long.parseLong(matcher.group(1)))
                    .as("⚠️ THE TERM WAS RELEASED, so a successor need not wait a TTL")
                    .isLessThanOrEqualTo(System.currentTimeMillis());
        }
    }

    @Test
    void anIDLENodeThrowsEVERYSwitchInORDERAndTheLeaseGOESLast() throws Exception {
        // ⚠️ NOTHING IS INSIDE, SO NOTHING IS FLUSHED UNTIL STEP 4, and the
        // journal is exact. A step left empty -- readiness never failed, the
        // polls never released -- is missing here, where the sequence's own
        // report would still list it.
        Path root = dir.resolve("store");
        Files.createDirectories(root);
        IngesterNode node = Main.run(configFile(root).toString());
        WebClient client = WebClient.builder()
                .baseUri("http://localhost:" + node.port()).build();
        assertThat(client.get(HealthService.READY_PATH).request().status().code())
                .as("a started node is ready").isEqualTo(200);

        node.close();

        assertThat(node.shutdownJournal()).containsExactly(DrainGate.READINESS_FAILED,
                DrainGate.POLLS_RELEASED, DrainGate.BULK_REFUSED, FrontDoor.LISTENER_STOPPED,
                Assembly.FLUSHED, Assembly.GRAPH_CLOSED);
        assertThat(node.gate().ready()).isFalse();
    }
}
