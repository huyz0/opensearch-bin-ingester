// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * One ingester, running as its own operating-system process (M8.8).
 *
 * <p>⚠️ **A TEST THAT CALLS {@code close()} IS NOT A KILL.** {@code SIGKILL}
 * runs no shutdown hook, flushes nothing and releases nothing. {@code SIGSTOP}
 * freezes every thread at once, the lease renewer with the rest, and the
 * process never learns it was frozen. Neither can be produced inside the test
 * JVM, and research 08 §9 names the frozen process as one of the rows that
 * finds real bugs. So this starts the shipped entry point, {@code Main}, in a
 * JVM of its own and signals it the way the kernel and Kubernetes do.
 *
 * <p>⚠️ **THE TEST JVM'S OWN CLASSPATH**, so the process under test runs the
 * same classes every other suite exercises, not a jar assembled differently.
 */
public final class NodeProcess implements AutoCloseable {

    private final String podId;
    private final int port;
    private final Path log;
    private final Process process;
    private volatile boolean paused;

    private NodeProcess(String podId, int port, Path log, Process process) {
        this.podId = podId;
        this.port = port;
        this.log = log;
        this.process = process;
    }

    /**
     * Starts a node and waits until it answers.
     *
     * @param dir where its settings file and its log go
     * @param settings the store's settings, from {@link ChaosBucket#nodeSettings()},
     *     plus anything the row overrides
     */
    public static NodeProcess start(Path dir, String podId, Map<String, String> settings)
            throws Exception {
        int port;
        // ⚠️ BOUND AND RELEASED, so another process could take it in the
        // window between -- the shape ConfigExitCodeIT uses and says so. The
        // node reports its port to nobody, so the test has to choose it.
        try (var probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        Map<String, String> all = new LinkedHashMap<>();
        all.put("pod.id", podId);
        all.put("trust.domain", "cluster-a");
        all.put("endpoint", "http://localhost:" + port);
        all.put("http.port", String.valueOf(port));
        all.put("producer.subject", "producer-1");
        all.put("producer.allowed-indices", "logs");
        all.putAll(settings);
        StringBuilder text = new StringBuilder();
        all.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        Path file = dir.resolve(podId + ".properties");
        Files.writeString(file, text.toString(), StandardCharsets.UTF_8);

        // ⚠️ TO A FILE, NOT A PIPE: a pipe nobody drains fills, and the node
        // then blocks on a log line -- which is a stall this harness would
        // report as a finding.
        Path log = dir.resolve(podId + ".log");
        ProcessBuilder builder = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-cp", System.getProperty("java.class.path"),
                "binjava.server.Main", file.toString()))
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        builder.environment().putAll(ChaosBucket.credentials());
        NodeProcess node = new NodeProcess(podId, port, log, builder.start());
        try {
            node.awaitServing();
        } catch (Exception | Error failed) {
            node.close();
            throw failed;
        }
        return node;
    }

    /**
     * Waits until the node answers {@code /live}, or dies trying.
     *
     * <p>⚠️ **BOUNDED, WITH A LIVENESS CHECK EVERY TURN** — testing.md rule
     * 15's external-system carve-out, in the shape that cannot become an
     * unbounded wait.
     */
    private void awaitServing() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        WebClient client = client();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new AssertionError(podId + " died before it started serving:\n" + log());
            }
            try {
                client.get("/live").request().close();
                return;
            } catch (RuntimeException notYet) {
                Thread.sleep(200);
            }
        }
        throw new AssertionError(podId + " never started serving:\n" + log());
    }

    public String podId() {
        return podId;
    }

    public int port() {
        return port;
    }

    public long pid() {
        return process.pid();
    }

    public boolean alive() {
        return process.isAlive();
    }

    /** A client onto this node's front door. */
    public WebClient client() {
        return WebClient.builder().baseUri("http://localhost:" + port).build();
    }

    /**
     * Registers the index {@code logs} with {@code uuid}, as the plugin does
     * over the wire.
     */
    public void registerLogs(java.util.UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        String indexUuid = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(buffer.array());
        int status = client().post(binjava.http.SubscriptionService.REGISTER_PATH)
                .submit(new binjava.format.IndexRegistration(indexUuid, "logs", List.of(),
                        4, 4, 1, 1).encode())
                .status().code();
        if (status != 204) {
            throw new AssertionError(podId + " refused the registration: " + status);
        }
    }

    /**
     * Writes one bulk of {@code records} documents to partition 0 of
     * {@code logs}.
     *
     * @return the status, which is 202 once the records are durable
     */
    public int write(String idPrefix, int records) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"").append(idPrefix).append('-').append(i)
                    .append("\",\"_version\":1}}\n{\"n\":").append(i).append("}\n");
        }
        return client().post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString()).status().code();
    }

    /** Everything the node has printed so far. */
    public String log() throws IOException {
        return Files.readString(log, StandardCharsets.UTF_8);
    }

    /**
     * {@code SIGKILL}: no hook, no flush, no release.
     *
     * @return the exit code, which is the signal's
     */
    public int kill() throws InterruptedException {
        resumeIfPaused();
        process.destroyForcibly();
        return awaitExit();
    }

    /**
     * {@code SIGTERM}, which is what Kubernetes sends and what runs the
     * graceful shutdown.
     */
    public int terminate() throws InterruptedException {
        resumeIfPaused();
        process.destroy();
        return awaitExit();
    }

    /**
     * {@code SIGSTOP}: every thread frozen at once, the lease renewer included,
     * and nothing in the process can notice.
     */
    public void pause() throws Exception {
        signal("STOP");
        paused = true;
    }

    /** {@code SIGCONT}. */
    public void resume() throws Exception {
        signal("CONT");
        paused = false;
    }

    private void resumeIfPaused() {
        if (paused) {
            try {
                resume();
            } catch (Exception ignored) {
                // a process that cannot be continued is killed next anyway
            }
        }
    }

    /**
     * ⚠️ **THROUGH {@code kill(1)}**, because the JDK sends only {@code SIGTERM}
     * and {@code SIGKILL}. Its exit status is checked: a signal that was not
     * delivered would leave a row asserting a freeze that never happened.
     */
    private void signal(String name) throws Exception {
        Process kill = new ProcessBuilder("kill", "-" + name, String.valueOf(process.pid()))
                .redirectErrorStream(true).start();
        if (!kill.waitFor(10, TimeUnit.SECONDS) || kill.exitValue() != 0) {
            throw new AssertionError("kill -" + name + " " + process.pid() + " failed: "
                    + new String(kill.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private int awaitExit() throws InterruptedException {
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
            throw new AssertionError(podId + " did not exit");
        }
        return process.exitValue();
    }

    /** Whatever state the row left it in, the process does not outlive the test. */
    @Override
    public void close() {
        resumeIfPaused();
        process.destroyForcibly();
    }
}
