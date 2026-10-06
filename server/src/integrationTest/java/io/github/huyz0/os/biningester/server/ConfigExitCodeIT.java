// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * M8's criterion 21, the half that only a PROCESS has: the exit code (M8.4,
 * M8.26).
 *
 * <p>⚠️ **{@code ServerProperties} IS ALREADY TESTED OVER A MAP AND THAT IS NOT
 * THIS.** A parser test proves the message names the key; it says nothing about
 * what the process does with it. Everything between the refusal and the exit
 * status is invisible to it: swapping the two exit constants, calling
 * {@code System.exit(0)}, dropping the argument guard, catching the refusal and
 * starting degraded, or printing a stack trace at an operator who mistyped a
 * mount. Review MEASURED every one of those surviving the suite.
 *
 * <p>⚠️ **A REAL JVM, NOT {@code Main.run}.** {@code main()} calls
 * {@code System.exit}, which a test JVM cannot survive — so the only way to
 * read an exit status is to be a different process. That is why this is T3 and
 * not T0; it needs no Docker.
 *
 * <p>⚠️ **AND THE NON-ZERO IS THE POINT, NOT THE PARTICULAR NUMBER.** A pod
 * whose config is wrong must die and keep dying: Kubernetes turns a non-zero
 * exit into {@code CrashLoopBackOff}, which is the signal an operator actually
 * sees. A process that exits 0 on a mistyped key is restarted silently for ever
 * and looks, from every dashboard, like a healthy deployment.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ConfigExitCodeIT {

    /**
     * ⚠️ **THE JVM'S OWN MARKER FOR AN EXCEPTION NOBODY CAUGHT.** An earlier
     * draft asserted that no {@code \t at } appeared at all, which is a
     * different and false claim: Helidon logs its own {@code BindException} at
     * SEVERE on the way out, and a library logging a failure it handled is not
     * this process dumping a trace at an operator.
     */
    private static final String UNCAUGHT = "Exception in thread \"main\"";

    @TempDir
    Path dir;

    private record Exit(int code, String output) {
    }

    /**
     * Runs {@code Main} in its own JVM.
     *
     * <p>⚠️ **THE TEST JVM'S OWN CLASSPATH**, so the process under test is
     * built from the same classes every other case here exercises rather than
     * from a jar assembled differently.
     */
    private static Exit runMain(long waitSeconds, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.Main"));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        var output = new java.io.ByteArrayOutputStream();
        Thread drain = Thread.ofVirtual().start(() -> {
            try (var in = process.getInputStream()) {
                in.transferTo(output);
            } catch (java.io.IOException died) {
                // The process ended; whatever was read is what is reported.
            }
        });
        if (!process.waitFor(waitSeconds, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            drain.join();
            throw new AssertionError("main() did not exit in " + waitSeconds + "s: "
                    + output.toString(StandardCharsets.UTF_8));
        }
        drain.join();
        return new Exit(process.exitValue(), output.toString(StandardCharsets.UTF_8));
    }

    private Path write(String name, String body) throws Exception {
        Path file = dir.resolve(name);
        Files.write(file, body.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private String valid(Path root, int port) {
        return String.join("\n",
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
                "");
    }

    @Test
    void theTHREEExitCodesArePAIRWISEDistinctAndNONEOfThemIsZERO() {
        // ⚠️ **THE CONSTANTS THEMSELVES, BECAUSE EVERY OTHER CASE COMPARES
        // AGAINST THEM.** An assertion that moves with the thing it measures
        // measures nothing: review set all three to 0 and the whole file stayed
        // green. This is the one case that can see that.
        //
        // ⚠️ AND PAIRWISE DISTINCT, because the codes are what an operator
        // ACTS on and the actions differ: 2 is "you invoked it wrong", 3 is
        // "the file was found and refused -- edit it", 4 is "the settings were
        // fine and the machine would not have it". Collapsing any two leaves
        // every case above green while destroying the distinction.
        assertThat(List.of(Main.EXIT_USAGE, Main.EXIT_CONFIG, Main.EXIT_START))
                .doesNotContain(0)
                .doesNotHaveDuplicates()
                .allMatch(code -> code > 0 && code < 128,
                        "a positive code below 128, so it cannot be read as a signal");
    }

    @Test
    void NOArgumentsIsAUSAGEExitAndTheMessageLISTSTheSettings() throws Exception {
        Exit exit = runMain(60);

        assertThat(exit.code())
                .as("⚠️ NON-ZERO, ASSERTED AS A LITERAL. Review MEASURED the version that "
                        + "only compared against `Main.EXIT_USAGE`: setting all three "
                        + "constants to 0 left every case in this file GREEN, which is "
                        + "verbatim the behaviour this class exists to prevent%n%s",
                        exit.output())
                .isNotZero()
                .isEqualTo(Main.EXIT_USAGE);
        assertThat(exit.output())
                .as("⚠️ AND THE SETTINGS ARE LISTED, because the commonest next question "
                        + "is what the file should contain")
                .contains("usage:")
                .contains("store.kind");
    }

    @Test
    void TWOArgumentsIsALSOAUsageExitRatherThanTheFirstBeingUsed() throws Exception {
        // ⚠️ THE GUARD IS `!= 1`, NOT `< 1`. Silently ignoring a second
        // argument is how an operator's `ingester a.properties b.properties`
        // starts a node from the wrong file and reports that the flag "did
        // nothing".
        Path file = write("node.properties", valid(dir, 0));
        Exit exit = runMain(60, file.toString(), file.toString());

        assertThat(exit.code()).isNotZero().isEqualTo(Main.EXIT_USAGE);
    }

    @Test
    void anUNKNOWNKeyEXITSNonZeroWithAMessageAndNOStackTrace() throws Exception {
        Path file = write("typo.properties", valid(dir, 0) + "lease.tll=PT30S\n");
        Exit exit = runMain(60, file.toString());

        assertThat(exit.code())
                .as("⚠️ AND IT IS NOT THE USAGE CODE: the file was found and REFUSED, which "
                        + "is a different thing for an operator to act on%n%s", exit.output())
                .isNotZero()
                .isNotEqualTo(Main.EXIT_USAGE)
                .isEqualTo(Main.EXIT_CONFIG);
        assertThat(exit.output()).contains("lease.tll");
        assertThat(exit.output())
                .as("⚠️ NO STACK TRACE. A trace for a one-line typo buries the one sentence "
                        + "that would help, and trains an operator to stop reading the output")
                .doesNotContain(UNCAUGHT);
    }

    @Test
    void aMISSINGConfigFileEXITSWithTheConfigCodeAndNAMESThePath() throws Exception {
        Path absent = dir.resolve("absent.properties");
        Exit exit = runMain(60, absent.toString());

        assertThat(exit.code()).isNotZero().isEqualTo(Main.EXIT_CONFIG);
        assertThat(exit.output()).contains(absent.toString()).doesNotContain(UNCAUGHT);
    }

    @Test
    void aPORTAlreadyHELDEXITSNonZeroWithAMessageRatherThanRunningDeaf() throws Exception {
        // ⚠️ MEASURED: Helidon does NOT throw when the port is in use -- it
        // returns -1 from `port()`. A node that took that for success would
        // hold and RENEW a term while serving nothing, and the fleet would
        // forward every commit to an endpoint that refuses connections.
        // ⚠️ AN ENVIRONMENT MISTAKE, NOT A DEFECT, so it gets a message and an
        // exit code rather than a stack trace.
        try (var socket = new java.net.ServerSocket(0)) {
            Path root = dir.resolve("store");
            Files.createDirectories(root);
            Path file = write("busy.properties", valid(root, socket.getLocalPort()));

            Exit exit = runMain(120, file.toString());

            assertThat(exit.code()).as("%s", exit.output())
                    .isNotZero()
                    .isEqualTo(Main.EXIT_START);
            assertThat(exit.output())
                    .contains("could not start")
                    .contains(String.valueOf(socket.getLocalPort()))
                    // ⚠️ NOT "no stack trace anywhere". MEASURED: Helidon logs
                    // its OWN `BindException` at SEVERE before returning, and
                    // that is the library's logging rather than an exception
                    // nobody caught. What must not appear is the JVM's marker
                    // for the second thing.
                    .doesNotContain(UNCAUGHT);
        }
    }

    @Test
    void aSIGTERMRunsTheSHUTDOWNHookAndTheHookRELEASESTheTerm() throws Exception {
        // ⚠️ **THE RELEASED LEASE IS THE EVIDENCE, AND THE EXIT CODE CANNOT BE.**
        // MEASURED: a JVM ended by `SIGTERM` exits 143 whether or not a hook
        // ran -- the status is the signal's, not the hook's -- so an earlier
        // draft asserting 0 was asserting something no correct implementation
        // produces.
        //
        // ⚠️ **AND THE NODE MUST WRITE BEFORE IT CAN RELEASE.** MEASURED too:
        // `FleetSequencer` elects on the first commit, so a node that has
        // served no write holds no term and has nothing to release. Hence the
        // registration and the `_bulk` below; a draft that sent `SIGTERM` to a
        // freshly started node waited two minutes for a lease that was never
        // going to exist.
        //
        // ⚠️ **THIS IS THE ONLY PLACE THE SHUTDOWN HOOK IS EXERCISED AT ALL.**
        // Deleting `addShutdownHook` leaves every in-process test green: a test
        // that calls `close()` itself is testing `close()`, not the hook. And
        // the hook is what turns a rolling deploy's visibility stall from a
        // TTL-long wait into a sub-second one (research 08 §7 step 5), which is
        // most real-world "AZ resilience" incidents.
        //
        // ⚠️ M8.7 owns the shutdown ORDER and the 30 s budget. What is claimed
        // here is that a `SIGTERM` reaches `close()` and that `close()`
        // releases a term this node actually held.
        int port;
        try (var probe = new java.net.ServerSocket(0)) {
            // ⚠️ BOUND AND RELEASED, so another process can take it in the
            // window between. It is the conventional trick and the probability
            // is low; it is named because a single unrepeatable failure here is
            // the one to distrust rather than to investigate. A kernel-chosen
            // port cannot be used: this process has to dial the node, and the
            // node reports its port to nobody.
            port = probe.getLocalPort();
        }
        Path root = dir.resolve("store");
        Files.createDirectories(root);
        Path file = write("live.properties", valid(root, port));

        List<String> command = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.Main", file.toString());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try {
            var client = io.helidon.webclient.api.WebClient.builder()
                    .baseUri("http://localhost:" + port).build();
            awaitServing(process, client);

            // ⚠️ REGISTERED OVER THE WIRE, because this process shares no heap
            // with the node -- which is also the only way M6's registrar
            // reaches a real server.
            var uuid = java.util.UUID.randomUUID();
            var buffer = java.nio.ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            String indexUuid = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(buffer.array());
            assertThat(client.post(io.github.huyz0.os.biningester.http.SubscriptionService.REGISTER_PATH)
                    .submit(new io.github.huyz0.os.biningester.format.IndexRegistration(
                            indexUuid, "logs", List.of(), 4, 4, 1, 1).encode())
                    .status().code())
                    .isEqualTo(204);

            assertThat(client.post("/logs/_bulk").queryParam("partition", "0")
                    .submit("{\"index\":{\"_id\":\"doc-0\",\"_version\":1}}\n{\"n\":0}\n")
                    .status().code())
                    .as("the node must commit before it holds a term to release")
                    .isEqualTo(202);

            // ⚠️ READ THROUGH THE STORE, NOT OFF THE FILESYSTEM. A backend owns
            // how a key becomes a path, and a test that walked directories
            // would be asserting that layout rather than the lease.
            assertThat(expiryOf(leaseText(root)))
                    .as("sanity: the term is HELD before the signal, or the assertion after "
                            + "it would pass over a lease that was never taken")
                    .isGreaterThan(System.currentTimeMillis());

            // ⚠️ `destroy()` IS `SIGTERM` ON LINUX, which is what Kubernetes
            // sends. `destroyForcibly()` would be `SIGKILL` and no hook runs.
            process.destroy();
            assertThat(process.waitFor(120, TimeUnit.SECONDS))
                    .as("the process did not exit after SIGTERM").isTrue();

            assertThat(expiryOf(leaseText(root)))
                    .as("⚠️ THE HOOK RELEASED THE TERM. Asserted on the lease object's own "
                            + "expiry, which a release moves into the past -- the holder's "
                            + "NAME stays, which is how a successor knows whom it replaced, "
                            + "so its absence cannot carry this")
                    .isLessThanOrEqualTo(System.currentTimeMillis());
        } finally {
            process.destroyForcibly();
        }
    }

    /**
     * Waits until the node answers, or the process dies trying.
     *
     * <p>⚠️ **testing.md RULE 15's EXTERNAL-SYSTEM CARVE-OUT, TAKEN WITH A
     * BOUNDED POLL RATHER THAN WITH Awaitility.** The rule's second sentence
     * allows waiting where a real external system is involved and names
     * Awaitility; this project pins a sha1 and a licence for every jar, and the
     * dependency would be bought to replace eleven lines. What the rule is
     * actually against is an UNBOUNDED or arbitrary wait, so the shape here is
     * the one that cannot become that: a deadline, a liveness assertion on the
     * subject every turn, and a named failure at the end. ⚠️ A case copied from
     * this one must keep all three — a sleep inside a loop with no deadline is
     * the flake rule 15 exists to prevent.
     */
    private static void awaitServing(Process process, io.helidon.webclient.api.WebClient client)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            assertThat(process.isAlive()).as("main() died before it started serving").isTrue();
            try {
                // ⚠️ AN UNREGISTERED INDEX, SO THIS IS A LIVENESS PROBE AND NOT
                // A WRITE. Any answer at all means the listener is up.
                client.post("/not-an-index/_bulk").queryParam("partition", "0")
                        .submit("{}\n").status();
                return;
            } catch (RuntimeException notYet) {
                Thread.sleep(200);
            }
        }
        throw new AssertionError("the node never started serving");
    }

    /** What the lease object under {@code root} currently says. */
    private static String leaseText(Path root) throws Exception {
        try (var store = io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore.at(root.toString())) {
            var page = store.list("bins/cluster-a/ctl/lease/", null, 10);
            assertThat(page.objects())
                    .as("no lease object: the node holds no term, so nothing here is about "
                            + "releasing one")
                    .isNotEmpty();
            try (var in = store.get(page.objects().get(0).key())) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }

    private static long expiryOf(String leaseJson) {
        var matcher = java.util.regex.Pattern.compile("\"expiresAtMillis\":(\\d+)")
                .matcher(leaseJson);
        if (!matcher.find()) {
            throw new AssertionError("no expiresAtMillis in: " + leaseJson);
        }
        return Long.parseLong(matcher.group(1));
    }
}
