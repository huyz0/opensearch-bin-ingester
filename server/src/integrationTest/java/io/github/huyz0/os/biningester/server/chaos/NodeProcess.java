// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.bench.BulkBatch;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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

    private static final String PAD = "x".repeat(2048);
    private static final int PRODUCER_SOCKET_SEND_BUFFER_BYTES = 4 * 1024 * 1024;

    private final String podId;
    private final int port;
    private final Path log;
    private final Process process;
    private volatile boolean paused;
    private int resumeAttempts;
    private final String macroPath;
    private final java.time.Duration producerReadTimeout;
    private final ThreadLocal<WebClient> producerClients;

    private final ChaosProxy peers;

    private NodeProcess(String podId, int port, Path log, Process process, ChaosProxy peers,
            String macroPath, java.time.Duration producerReadTimeout) {
        this.podId = podId;
        this.port = port;
        this.log = log;
        this.process = process;
        this.peers = peers;
        this.macroPath = macroPath;
        this.producerReadTimeout = producerReadTimeout;
        this.producerClients = ThreadLocal.withInitial(this::newClient);
    }

    static NodeProcess forTest(Process process, boolean paused) {
        NodeProcess node = new NodeProcess("test", 0, Path.of("test.log"), process, null, null,
                java.time.Duration.ofSeconds(2));
        node.paused = paused;
        return node;
    }

    /**
     * The proxy peers reach this node through, to cut and heal.
     *
     * @throws IllegalStateException if the node was started without one
     */
    public ChaosProxy peers() {
        if (peers == null) {
            throw new IllegalStateException(podId + " was started without a peer proxy");
        }
        return peers;
    }

    /**
     * A jar holding only a manifest that names {@link SkewAgent}.
     *
     * <p>⚠️ **THE AGENT'S CLASS IS ALREADY ON THE CHILD'S CLASSPATH**, which is
     * this JVM's, so the jar needs nothing but the {@code Premain-Class} line.
     */
    static Path agentJar(Path dir) throws IOException {
        Path jar = dir.resolve("skew-agent.jar");
        if (!Files.exists(jar)) {
            java.util.jar.Manifest manifest = new java.util.jar.Manifest();
            manifest.getMainAttributes().put(java.util.jar.Attributes.Name.MANIFEST_VERSION, "1.0");
            manifest.getMainAttributes().putValue("Premain-Class", SkewAgent.class.getName());
            try (var out = new java.util.jar.JarOutputStream(Files.newOutputStream(jar),
                    manifest)) {
                out.flush();
            }
        }
        return jar;
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
        return start(dir, podId, settings, Options.NONE);
    }

    /**
     * What the network and the clock do to one node (M8.23).
     *
     * @param skew how far ahead of true time this process's wall clock runs
     *     ({@link SkewAgent}); negative runs behind, zero leaves it alone
     * @param peerProxy whether peers reach this node through a {@link ChaosProxy}
     *     the test can cut, which isolates it from its peers inbound
     */
    public record Options(java.time.Duration skew, boolean peerProxy) {

        public static final Options NONE = new Options(java.time.Duration.ZERO, false);
    }

    /** The same, with the network and the clock under the test's control. */
    public static NodeProcess start(Path dir, String podId, Map<String, String> settings,
            Options options) throws Exception {
        return start(dir, podId, settings, options, null);
    }

    /** Starts a node with the opt-in macro count snapshot endpoint enabled. */
    public static NodeProcess start(Path dir, String podId, Map<String, String> settings,
            Options options, Path countsFile) throws Exception {
        return start(dir, podId, settings, options, countsFile, java.time.Duration.ofSeconds(2));
    }

    /** Starts a node with an explicit response timeout for high-throughput test producers. */
    public static NodeProcess start(Path dir, String podId, Map<String, String> settings,
            Options options, Path countsFile, java.time.Duration producerReadTimeout)
            throws Exception {
        int port;
        // ⚠️ BOUND AND RELEASED, so another process could take it in the
        // window between -- the shape ConfigExitCodeIT uses and says so. The
        // node reports its port to nobody, so the test has to choose it.
        try (var probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        ChaosProxy peers = options.peerProxy() ? new ChaosProxy("localhost", port) : null;
        Map<String, String> all = new LinkedHashMap<>();
        all.put("pod.id", podId);
        all.put("pod.az", "az-a");
        all.put("trust.domain", "cluster-a");
        // ⚠️ THE ADVERTISED ENDPOINT IS THE PROXY when there is one: it is
        // what the lease names, so it is what every peer dials.
        all.put("endpoint", "http://localhost:" + (peers == null ? port : peers.port()));
        all.put("http.port", String.valueOf(port));
        all.put("producer.subject", "producer-1");
        all.put("producer.allowed-indices", "logs");
        all.putAll(settings);
        all.put("pod.uid", UUID.randomUUID().toString());
        Properties properties = new Properties();
        all.forEach(properties::setProperty);
        Path file = dir.resolve(podId + ".properties");
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            properties.store(writer, null);
        }

        // ⚠️ TO A FILE, NOT A PIPE: a pipe nobody drains fills, and the node
        // then blocks on a log line -- which is a stall this harness would
        // report as a finding.
        Path log = dir.resolve(podId + ".log");
        List<String> command = new java.util.ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m"));
        if (!options.skew().isZero()) {
            command.add("-javaagent:" + agentJar(dir) + "=" + options.skew());
        }
        String macroPath;
        if (countsFile != null) {
            macroPath = "/_macro/store-counts/" + java.util.UUID.randomUUID();
            command.add("-Dbinstore.macro.path=" + macroPath);
        } else {
            macroPath = null;
        }
        command.addAll(List.of("-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.Main", file.toString()));
        ProcessBuilder builder = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        builder.environment().putAll(ChaosBucket.credentials());
        NodeProcess node = new NodeProcess(podId, port, log, builder.start(), peers, macroPath,
                producerReadTimeout);
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
        WebClient client = client();
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> {
                    if (!process.isAlive()) {
                        throw new AssertionError(
                                podId + " died before it started serving:\n" + log());
                    }
                    try (var response = client.get("/live").request()) {
                        assertThat(response.status().code())
                                .as(podId + " /live responds successfully").isEqualTo(200);
                    } catch (RuntimeException notYet) {
                        throw new AssertionError(podId + " is not serving yet", notYet);
                    }
                });
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

    boolean paused() {
        return paused;
    }

    int resumeAttempts() {
        return resumeAttempts;
    }

    /**
     * A client onto this node's front door.
     *
     * <p>⚠️ **WITH A 2 s TIMEOUT, AS A REAL PRODUCER HAS.** MEASURED (M8.12):
     * without one, a request to a SIGSTOPped node waited out the client's 30 s
     * default. Within seconds every round-robin producer was parked on the
     * frozen node, nothing reached a live one, and "visibility resumed" took
     * 30 s for a reason that had nothing to do with the sequencer.
     */
    public WebClient client() {
        return newClient();
    }

    private WebClient newClient() {
        return newClient(producerReadTimeout);
    }

    private WebClient newClient(java.time.Duration readTimeout) {
        return WebClient.builder().baseUri("http://localhost:" + port)
                .proxy(io.helidon.webclient.api.Proxy.noProxy())
                // The benchmark's largest NDJSON body is about 2.5 MiB. Windows can
                // block a synchronous write when the default send buffer fills while
                // the server is waiting for that body to become durable.
                .socketOptions(options -> options.socketSendBufferSize(
                        PRODUCER_SOCKET_SEND_BUFFER_BYTES))
                .connectTimeout(java.time.Duration.ofSeconds(2))
                .readTimeout(readTimeout).build();
    }

    private WebClient producerClient() {
        return producerClients.get();
    }

    /**
     * Registers the index {@code logs} with {@code uuid}, as the plugin does
     * over the wire.
     */
    public void registerLogs(java.util.UUID uuid) {
        registerIndex("logs", uuid);
    }

    /** Registers one index with the same wire shape used by the plugin. */
    public void registerIndex(String index, java.util.UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        String indexUuid = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(buffer.array());
        int status;
        try (var response = client().post(
                io.github.huyz0.os.biningester.http.SubscriptionService.REGISTER_PATH)
                .submit(new io.github.huyz0.os.biningester.format.IndexRegistration(indexUuid, index,
                        List.of(), 4, 4, 1, 1).encode())) {
            status = response.status().code();
        }
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
        try (var response = producerClient().post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString())) {
            return response.status().code();
        }
    }

    /** Writes one bulk with a longer response timeout for interval-ceiling probes. */
    public int write(String idPrefix, int records, java.time.Duration readTimeout) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"").append(idPrefix).append('-').append(i)
                    .append("\",\"_version\":1}}\n{\"n\":").append(i).append("}\n");
        }
        try (var response = newClient(readTimeout).post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString())) {
            return response.status().code();
        }
    }

    /**
     * Writes one bulk with exactly these document ids to partition 0 of
     * {@code logs}.
     *
     * @return the status, which is 202 once the records are durable
     */
    public int write(List<String> ids) {
        StringBuilder body = new StringBuilder();
        for (String id : ids) {
            // ⚠️ 2 KB OF PAYLOAD PER RECORD, so a segment is large enough that
            // its PUT takes measurable time -- which is the window an ack
            // that ran ahead of its PUT would be caught in.
            body.append("{\"index\":{\"_id\":\"").append(id)
                    .append("\",\"_version\":1}}\n{\"pad\":\"").append(PAD).append("\"}\n");
        }
        try (var response = producerClient().post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString())) {
            return response.status().code();
        }
    }

    /** Writes a generated benchmark batch over the producer's real socket. */
    public int write(BulkBatch batch) {
        // The generator carries `_index` so the same body can be replayed by
        // generic bulk tooling. This front door takes the index from the URL,
        // so remove only that redundant metadata field at the process seam.
        String body = new String(batch.body(), StandardCharsets.UTF_8)
                .replaceAll("\\{\\\"index\\\":\\{\\\"_index\\\":\\\"[^\\\"]+\\\",",
                        "{\\\"index\\\":{");
        try (var response = producerClient().post("/" + batch.stream().index() + "/_bulk")
                .queryParam("partition", String.valueOf(batch.stream().partition()))
                .submit(body)) {
            return response.status().code();
        }
    }

    /** Reads a point-in-time count snapshot before the process is terminated. */
    public void snapshotCounts(Path path) throws IOException {
        if (macroPath == null) {
            throw new IllegalStateException(podId + " was not started with macro counts enabled");
        }
        String json;
        try (var response = client().get(macroPath).request()) {
            if (response.status().code() != 200) {
                throw new IOException(podId + " count snapshot returned " + response.status().code());
            }
            json = response.as(String.class);
        }
        Path absolute = path.toAbsolutePath();
        Path parent = absolute.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writeAtomicSnapshot(absolute, json);
    }

    static void writeAtomicSnapshot(Path absolute, String contents) throws IOException {
        Path parent = absolute.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path temporary = absolute.resolveSibling(absolute.getFileName() + ".tmp");
        Files.writeString(temporary, contents, StandardCharsets.UTF_8);
        replaceAtomically(temporary, absolute, (source, target) -> Files.move(source, target,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING));
    }

    @FunctionalInterface
    interface AtomicMover {
        void move(Path source, Path target) throws IOException;
    }

    static void replaceAtomically(Path temporary, Path absolute, AtomicMover mover)
            throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                mover.move(temporary, absolute);
                return;
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.deleteIfExists(temporary);
                throw new IOException("atomic snapshot replacement is not supported", unsupported);
            } catch (java.nio.file.AccessDeniedException denied) {
                if (attempt == 99) {
                    Files.deleteIfExists(temporary);
                    throw denied;
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    Files.deleteIfExists(temporary);
                    throw new IOException("interrupted during atomic snapshot replacement", interrupted);
                }
            }
        }
    }

    String macroPath() {
        return macroPath;
    }

    int status(String path) throws IOException {
        try (var response = client().get(path).request()) {
            return response.status().code();
        }
    }

    /**
     * {@code SIGKILL}, sent and not waited for (M8.9).
     *
     * <p>⚠️ **FOR A KILL SYNCHRONISED TO AN EVENT**, where the caller's next
     * act must follow the signal and not the process's exit.
     */
    public void killNow() {
        process.destroyForcibly();
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
            resumeAttempts++;
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
        if (System.getProperty("os.name").startsWith("Windows")) {
            signalWindows(name);
            return;
        }
        Process kill = new ProcessBuilder("kill", "-" + name, String.valueOf(process.pid()))
                .redirectErrorStream(true).start();
        if (!kill.waitFor(10, TimeUnit.SECONDS) || kill.exitValue() != 0) {
            throw new AssertionError("kill -" + name + " " + process.pid() + " failed: "
                    + new String(kill.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Windows has no POSIX signals; suspend/resume the child through its native process handle. */
    private void signalWindows(String name) throws Exception {
        String nativeCall = switch (name) {
            case "STOP" -> "NtSuspendProcess";
            case "CONT" -> "NtResumeProcess";
            default -> throw new IllegalArgumentException("unsupported process signal: " + name);
        };
        String declarations = "using System; using System.Runtime.InteropServices; "
                + "public static class NodeProcessSignals { "
                + "[DllImport(\"ntdll.dll\")] public static extern int NtSuspendProcess(IntPtr h); "
                + "[DllImport(\"ntdll.dll\")] public static extern int NtResumeProcess(IntPtr h); }";
        String encodedDeclarations = java.util.Base64.getEncoder().encodeToString(
                declarations.getBytes(StandardCharsets.UTF_8));
        String script = "Add-Type -TypeDefinition ([Text.Encoding]::UTF8.GetString("
                + "[Convert]::FromBase64String('" + encodedDeclarations + "'))); "
                + "$p = [System.Diagnostics.Process]::GetProcessById(" + process.pid() + "); "
                + "$status = [NodeProcessSignals]::" + nativeCall + "($p.Handle); "
                + "if ($status -ne 0) { [Console]::Error.WriteLine(('NTSTATUS 0x{0:X8}' -f $status)); exit 1 }";
        Process nativeSignal = new ProcessBuilder("powershell.exe", "-NoLogo", "-NoProfile",
                "-NonInteractive", "-Command", script).redirectErrorStream(true).start();
        if (!nativeSignal.waitFor(10, TimeUnit.SECONDS) || nativeSignal.exitValue() != 0) {
            nativeSignal.destroyForcibly();
            throw new AssertionError("Windows " + name + " for process " + process.pid()
                    + " failed: " + new String(nativeSignal.getInputStream().readAllBytes(),
                            StandardCharsets.UTF_8));
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
        try {
            // Windows keeps the redirected log handle open until the child
            // has actually exited. Returning immediately leaves JUnit's
            // @TempDir cleanup racing that handle and turns a passing test
            // into a teardown failure.
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException(podId + " did not exit after close");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (peers != null) {
            peers.close();
        }
    }
}
