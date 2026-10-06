// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.http.SubscriptionService;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code SIGTERM} drains a real process in research 08 §7's order, inside
 * the 30 s budget, and loses nothing (M8.7, criterion 5).
 *
 * <p>⚠️ **THE NUMBER COMES FROM A REAL DRAIN, NOT FROM A RECORDING SEAM.**
 * The process is signalled the way Kubernetes signals it, and the sequence
 * reports its own timings. Those timings are printed here for an operator to
 * set {@code terminationGracePeriodSeconds} from.
 *
 * <p>⚠️ **A DRAIN THAT SKIPS THE WORK IS FASTER THAN ONE THAT DOES IT**, so
 * the bound alone proves nothing. What carries it is the request that was
 * inside the door when the signal landed: it must be answered 202, and its
 * records must be in the store after the process has exited.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ShutdownDrainIT {

    private static final Pattern STEP =
            Pattern.compile("shutdown step (\\d) ([A-Z_]+) took (\\d+) ms");
    private static final Pattern EVENT = Pattern.compile("shutdown event: ([^\\n]+)");
    private static final Pattern TOTAL = Pattern.compile("graceful shutdown took (\\d+) ms");

    @TempDir
    Path dir;

    @Test
    // ⚠️ POSIX ONLY (M13.54): Kubernetes sends SIGTERM, which `destroy()`
    // is on Linux and macOS; on Windows it is a hard kill, so the case
    // could only fail there, saying nothing about the drain.
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Process.destroy() is TerminateProcess on Windows: no shutdown hook runs, so no graceful stop can be observed (M13.54)")
    void aSIGTERMDrainsInSECTION7sOrderWithinTheBUDGETAndLosesNOTHING() throws Exception {
        int port;
        try (var probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        Path root = dir.resolve("store");
        Files.createDirectories(root);
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.uid=uid-pod1",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=local-fs",
                "store.root=" + root.toString().replace('\\', '/'),
                "endpoint=http://pod1:8080",
                "http.port=" + port,
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                // ⚠️ PINNED AT 5 s, so the request below is inside the door
                // waiting for a flush that is not due when the signal lands.
                ServerProperties.INTERVAL_FLOOR + "=PT5S",
                "").getBytes(StandardCharsets.UTF_8));

        // ⚠️ TO A FILE, NOT A PIPE. MEASURED: a reader draining the pipe
        // concurrently lost the race with the JDK's own drain at exit and saw
        // "Stream closed" instead of the log it was there to read.
        Path logFile = dir.resolve("node.log");
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.Main", file.toString()))
                .redirectErrorStream(true)
                .redirectOutput(logFile.toFile()).start();
        String log;
        try {
            WebClient client = WebClient.builder().baseUri("http://localhost:" + port).build();
            awaitServing(process, client);
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
            // ⚠️ THE NODE ELECTS ON ITS FIRST COMMIT, so it must write before
            // it holds a term to release.
            assertThat(client.post("/logs/_bulk").queryParam("partition", "0")
                    .submit("{\"index\":{\"_id\":\"first\",\"_version\":1}}\n{\"n\":0}\n")
                    .status().code()).isEqualTo(202);

            // ⚠️ A CONSUMER RE-POLLS ON EVERY 200, AND SO DOES THIS ONE. The
            // first write's push is delivered asynchronously and can answer
            // the first poll; what is asserted is the first answer that is
            // NOT a 200, which is the one the drain gave.
            CompletableFuture<Integer> poll = CompletableFuture.supplyAsync(() -> {
                while (true) {
                    try (var response = client.get("/sub/" + uuid + "/0")
                            .queryParam("wait", "30").queryParam("sub", "s1").request()) {
                        if (response.status().code() != 200) {
                            return response.status().code();
                        }
                    }
                }
            });
            CompletableFuture<Integer> inside = CompletableFuture.supplyAsync(() ->
                    client.post("/logs/_bulk").queryParam("partition", "0")
                            .submit("{\"index\":{\"_id\":\"inside\",\"_version\":1}}\n{\"n\":1}\n")
                            .status().code());
            // ⚠️ GIVEN TIME TO ARRIVE, AND THEN CHECKED NOT TO HAVE FINISHED.
            // This process cannot see the node's gate, so the premise is
            // asserted instead: a request that had already been answered
            // would make the 202 below say nothing about the drain.
            Thread.sleep(1000);
            assertThat(inside.isDone())
                    .as("the premise: the request is still inside the door at the signal")
                    .isFalse();

            // `destroy()` is SIGTERM on Linux, which is what Kubernetes sends.
            process.destroy();
            assertThat(process.waitFor(60, TimeUnit.SECONDS))
                    .as("the process did not exit after SIGTERM").isTrue();
            log = Files.readString(logFile);

            assertThat(poll.get(20, TimeUnit.SECONDS))
                    .as("⚠️ THE SUBSCRIBER WAS TOLD TO GO").isEqualTo(503);
            assertThat(inside.get(20, TimeUnit.SECONDS))
                    .as("⚠️ THE REQUEST INSIDE WAS ANSWERED, NOT CUT OFF").isEqualTo(202);
        } finally {
            process.destroyForcibly();
        }

        List<String> order = new ArrayList<>();
        Matcher step = STEP.matcher(log);
        while (step.find()) {
            order.add(step.group(2));
        }
        assertThat(order).as("⚠️ THE OBSERVED ORDER OF EVENTS, in:%n%s", log)
                .containsExactly("FAIL_READINESS", "RELEASE_SUBSCRIBERS", "FINISH_IN_FLIGHT",
                        "FLUSH_AND_COMMIT", "RELEASE_LEASES");
        List<String> events = new ArrayList<>();
        Matcher event = EVENT.matcher(log);
        while (event.find()) {
            events.add(event.group(1).strip());
        }
        assertThat(events)
                .as("⚠️ THE ORDER THE SWITCHES, THE FLUSH, THE LISTENER AND THE GRAPH REALLY "
                        + "MOVED, written at each of them, in:%n%s", log)
                .containsSubsequence(io.github.huyz0.os.biningester.http.DrainGate.READINESS_FAILED,
                        io.github.huyz0.os.biningester.http.DrainGate.POLLS_RELEASED,
                        io.github.huyz0.os.biningester.http.DrainGate.BULK_REFUSED, Assembly.FLUSHED,
                        FrontDoor.LISTENER_STOPPED, Assembly.FLUSHED, Assembly.GRAPH_CLOSED)
                .endsWith(Assembly.GRAPH_CLOSED);
        Matcher total = TOTAL.matcher(log);
        assertThat(total.find()).as("no total in:%n%s", log).isTrue();
        long millis = Long.parseLong(total.group(1));
        // ⚠️ REPORTED, for terminationGracePeriodSeconds.
        System.out.println("M8.7 measured graceful shutdown: " + millis + " ms");
        assertThat(millis).as("⚠️ §7's budget").isLessThan(30_000);

        try (BinStore store = LocalFsBinStore.at(root.toString())) {
            assertThat(store.list("bins/cluster-a/data/", null, 100).objects())
                    .as("⚠️ BOTH WRITES ARE IN THE STORE AFTER THE PROCESS IS GONE")
                    .hasSizeGreaterThanOrEqualTo(2);
            var lease = store.list("bins/cluster-a/ctl/lease/", null, 10).objects();
            assertThat(lease).isNotEmpty();
            String text;
            try (var in = store.get(lease.get(0).key())) {
                text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            Matcher expiry = Pattern.compile("\"expiresAtMillis\":(\\d+)").matcher(text);
            assertThat(expiry.find()).isTrue();
            assertThat(Long.parseLong(expiry.group(1)))
                    .as("⚠️ THE TERM WAS RELEASED").isLessThanOrEqualTo(System.currentTimeMillis());
        }
    }

    /**
     * Waits until the node answers, or the process dies trying — bounded, with
     * a liveness check every turn (testing.md rule 15's external-system
     * carve-out, the shape {@code ConfigExitCodeIT} uses).
     */
    private static void awaitServing(Process process, WebClient client) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            assertThat(process.isAlive()).as("main() died before it started serving").isTrue();
            try {
                client.get("/live").request().status();
                return;
            } catch (RuntimeException notYet) {
                Thread.sleep(200);
            }
        }
        throw new AssertionError("the node never started serving");
    }
}
