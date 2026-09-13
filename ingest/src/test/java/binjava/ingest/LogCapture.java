// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Runs work with the streams AND the logging system captured, and returns
 * everything written — the instrument M5's SPEC criterion 7 asks for when it
 * says a signed URL appears in no log, trace or error message
 * (security.md rule 4).
 *
 * <p>⚠️ ONE PROBE, NOT ONE PER TEST CLASS, AND THAT IS THE POINT. This began
 * as a private helper in {@code GrantNeverLoggedTest}, and when
 * {@code DirectServingTest} needed the same assertion for the hub's `direct`
 * path it re-implemented the probe and dropped two of the three channels
 * below. Review MEASURED the consequence: a {@code System.Logger} leak passing
 * the URL as a format ARGUMENT, and a bare {@code System.out.println} of it,
 * both survived the whole module green, while the identical leak written with
 * {@code +} concatenation was caught. A partial probe does not fail — it
 * reports a clean run, which is the worst outcome an instrument can have.
 *
 * <p>⚠️ CHANNEL 1, THE STREAMS. {@code System.out} and {@code System.err} are
 * swapped, which catches a debug print left behind.
 *
 * <p>⚠️ CHANNEL 2, THE LOGGING SYSTEM, BECAUSE THE STREAM SWAP ALONE IS NOT
 * ENOUGH — and round-1 review of M5.13 measured exactly how it fails.
 * {@code System.Logger} is the only logging facility in this codebase
 * (java-style.md; the sequencer alone has four {@code System.getLogger}
 * sites), and JUL's {@code ConsoleHandler} binds {@code System.err} when the
 * HANDLER is constructed. Once anything in the JVM has logged — any earlier
 * test in the same worker counts — later records go to the ORIGINAL stream and
 * never reach the buffer. MEASURED: a {@code System.Logger} leak of a grant
 * SURVIVED the stream swap at both INFO and ERROR. So a JUL handler is
 * attached to the ROOT logger at {@code Level.ALL} with the level forced,
 * catching records however the console handler is bound.
 *
 * <p>⚠️ CHANNEL 3, THE RECORD'S PARAMETERS, which is the one a second
 * implementation dropped. {@code System.Logger.log(level, "msg {0}", arg)}
 * puts {@code arg} in {@link java.util.logging.LogRecord#getParameters()} and
 * leaves {@code getMessage()} as the unformatted pattern, so a probe reading
 * only the message sees {@code "serving {0} direct via {1}"} and never the
 * secret. Parameterised logging is the NORMAL form, not an exotic one.
 *
 * <p>All three feed one buffer, so a leak through any of them fails the same
 * assertion.
 */
final class LogCapture {

    private LogCapture() {
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static String capturing(ThrowingRunnable work) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream sink = new PrintStream(captured, true, StandardCharsets.UTF_8);
        PrintStream realOut = System.out;
        PrintStream realErr = System.err;

        java.util.logging.Logger root = java.util.logging.LogManager.getLogManager()
                .getLogger("");
        java.util.logging.Level realLevel = root.getLevel();
        StringBuilder logged = new StringBuilder();
        java.util.logging.Handler probe = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) {
                logged.append(record.getLevel()).append(' ')
                        .append(record.getMessage()).append('\n');
                Object[] params = record.getParameters();
                if (params != null) {
                    for (Object p : params) {
                        logged.append(p).append('\n');
                    }
                }
                if (record.getThrown() != null) {
                    logged.append(record.getThrown()).append('\n');
                }
            }

            @Override public void flush() {
            }

            @Override public void close() {
            }
        };
        probe.setLevel(java.util.logging.Level.ALL);
        try {
            System.setOut(sink);
            System.setErr(sink);
            root.addHandler(probe);
            root.setLevel(java.util.logging.Level.ALL);
            work.run();
        } finally {
            System.setOut(realOut);
            System.setErr(realErr);
            root.removeHandler(probe);
            root.setLevel(realLevel);
        }
        return captured.toString(StandardCharsets.UTF_8) + logged;
    }
}
