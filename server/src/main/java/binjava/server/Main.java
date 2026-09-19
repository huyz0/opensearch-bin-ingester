// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * The process (M8.4, FR-1, FR-13).
 *
 * <p>⚠️ **THIS FILE IS ARGV, A CLOCK, A SHUTDOWN HOOK AND AN EXIT CODE. THAT IS
 * ALL IT IS.** Everything a node does lives in {@link IngesterNode}, which a
 * test starts the same way this does — through {@link #run}, off a real file.
 * A {@code main()} holding the wiring would leave the tested path and the
 * shipped path related only by inspection, which is the failure ADR-0052 §3
 * puts this row before the chaos matrix to avoid: the matrix would then be
 * killing a process nothing else exercises.
 *
 * <p>⚠️ **ONE OF TWO FILES EXEMPT FROM {@code check-io-seam.sh} BY NAME**, the
 * other being {@link ConfigFile}. What it reaches for is the real clock, which
 * is banned by CONSTRUCT ({@code Clock.system}) everywhere else — and that ban
 * is the whole reason eight milestones of tests can move time at all. The
 * exemption is what a composition root IS: the I/O and the clock become real in
 * exactly one place, and that place is small enough to read.
 *
 * <p>⚠️ **A MISCONFIGURATION EXITS NON-ZERO WITH A MESSAGE, NEVER A STACK
 * TRACE.** {@link ConfigurationException} is the one type an operator's mistake
 * arrives as; anything else is a defect in this code and its stack trace is the
 * useful output. Printing a trace for a mistyped key trains an operator to
 * ignore the output entirely.
 */
public final class Main {

    /** ⚠️ The conventional "you invoked it wrong", distinct from a crash. */
    static final int EXIT_USAGE = 2;

    /** ⚠️ Distinct from {@link #EXIT_USAGE}: the file was found and refused. */
    static final int EXIT_CONFIG = 3;

    /**
     * ⚠️ **THE SETTINGS PARSED AND THE NODE STILL COULD NOT START.** A port
     * already in use is the ordinary case and it is an ENVIRONMENT mistake
     * rather than a defect in this code, so it gets a message and a code like
     * every other operator-facing failure — review found the first draft
     * letting it out as an uncaught stack trace, against this class's own
     * javadoc. ⚠️ Distinct from {@link #EXIT_CONFIG} because the two need
     * different actions: one is a file to edit, the other is a machine to look
     * at.
     */
    static final int EXIT_START = 4;

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: ingester <config.properties>");
            System.err.println("settings: " + ServerProperties.knownKeys());
            System.exit(EXIT_USAGE);
            return;
        }
        IngesterNode node;
        try {
            node = run(args[0]);
        } catch (ConfigurationException refused) {
            // ⚠️ THE MESSAGE ALONE. It already names the key or the path --
            // that is what `ServerProperties` and `ConfigFile` exist to
            // guarantee -- and a trace on top buries it.
            System.err.println("refusing to start: " + refused.getMessage());
            System.exit(EXIT_CONFIG);
            return;
        } catch (IllegalStateException refused) {
            // ⚠️ WHAT THE STARTUP REFUSALS THROW: a front door that did not
            // bind, and M5.43's refusal to run `direct` over a backend that
            // cannot sign. Both are things an operator can fix, and neither is
            // worth a stack trace.
            System.err.println("could not start: " + refused.getMessage());
            System.exit(EXIT_START);
            return;
        } catch (IOException failed) {
            // ⚠️ NOT AN OPERATOR MISTAKE AND NOT SILENT. The store answered, or
            // the term could not be taken; the trace is the useful output and
            // the exit code is the JVM's own 1.
            failed.printStackTrace();
            System.exit(1);
            return;
        }

        // ⚠️ THE HOOK CLOSES THE NODE, AND CLOSING RELEASES THE LEASE. Without
        // it a `SIGTERM` -- which is how Kubernetes ends every pod on every
        // deploy -- leaves the term held until it expires, and research 08 §7
        // step 5 measures that as the difference between a sub-second
        // visibility gap and a TTL-long one on a ROLLOUT. Most real-world "AZ
        // resilience" incidents are rollouts.
        // ⚠️ `close()` RUNS research 08 §7's ORDER (`ShutdownSequence`), and
        // what it measured is printed here for an operator to set
        // `terminationGracePeriodSeconds` from.
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.close();
            } catch (IOException failed) {
                // ⚠️ REPORTED, NEVER THROWN. An exception out of a shutdown
                // hook is swallowed by the JVM, so the only way this is ever
                // seen is if it is printed here.
                failed.printStackTrace();
            } finally {
                ShutdownSequence.Report report = node.lastShutdown();
                node.shutdownJournal().forEach(event ->
                        System.err.println("shutdown event: " + event));
                if (report != null) {
                    report.lines().forEach(System.err::println);
                }
                stopped.countDown();
            }
        }, "ingester-shutdown"));

        try {
            // ⚠️ PARKED, NOT SPINNING, AND NOT `node.awaitSomething()`. The
            // server's own threads are what serve; this one exists so the JVM
            // has a non-daemon thread to keep it alive, and the latch is
            // counted down by the hook.
            stopped.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reads {@code configPath}, assembles a node and starts serving.
     *
     * <p>⚠️ **THE REAL CLOCK ENTERS THE PROGRAM HERE**, at one call, and is
     * handed down as a parameter from then on.
     *
     * @throws ConfigurationException if the file is missing, unreadable, or
     *     names a setting that cannot be honoured
     * @throws IOException if the store cannot be opened or the term cannot be
     *     taken
     */
    public static IngesterNode run(String configPath) throws IOException {
        ServerConfig config = ServerProperties.parse(ConfigFile.read(configPath));
        // ⚠️ THE TOKEN IS READ HERE, ON EVERY CONNECTION, because this file is
        // one of the two the I/O seam exempts, and a projected service-account
        // token rotates under a running pod.
        Optional<String> tokenFile = config.membership().flatMap(MembershipConfig::tokenFile);
        return IngesterNode.start(config, Clock.systemUTC(), () -> tokenFile.map(Main::readToken));
    }

    /**
     * ⚠️ **AN UNREADABLE TOKEN IS AN UNCHECKED FAILURE OF THAT CONNECTION**, and
     * the watch retries it: a token file mid-rotation is briefly absent, and
     * that must cost a reconnect, not the node.
     */
    private static String readToken(String path) {
        try {
            return java.nio.file.Files.readString(java.nio.file.Path.of(path)).strip();
        } catch (IOException unreadable) {
            throw new java.io.UncheckedIOException("the Kubernetes service-account token at " + path
                    + " could not be read", unreadable);
        }
    }
}
