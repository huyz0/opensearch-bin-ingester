// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.SignedUrl;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A grant reaches no log, trace or error message — asserted by CAPTURING
 * OUTPUT while one is issued (M5.13, security.md rule 4).
 *
 * <p>⚠️ M5's SPEC CRITERION 7 ASKS FOR EXACTLY THIS INSTRUMENT, not for a
 * {@code toString} test. {@code SignedUrlTest} already pins the redacting
 * {@code toString}, and that closes the case where something prints the OBJECT.
 * It closes nothing about a URL reaching output some other way: through a
 * string concatenation, through an exception message written by code that
 * never heard of ADR-0041, or through a stack trace.
 *
 * <p>⚠️ AND THE FAILURE PATH IS THE ONE THAT MATTERS. On the happy path nobody
 * is trying to print anything. It is when signing or fetching fails that a
 * message gets built out of whatever is in scope, and a grant is in scope
 * precisely then.
 */
class GrantNeverLoggedTest {

    private static final String SEGMENT = "seg/2026/09/11/abc";

    /** The distinctive part of a signed URL: what must never appear anywhere. */
    private static final String SIGNATURE = "X-Amz-Signature=DEADBEEFCAFE";

    private static final class SigningStore implements BinStore {
        private final BinStore delegate = new MemoryBinStore();
        private final IOException failWith;

        SigningStore(IOException failWith) {
            this.failWith = failWith;
        }

        @Override public SignedUrl presign(String key, Duration ttl) throws IOException {
            if (failWith != null) {
                throw failWith;
            }
            return new SignedUrl("https://store.example/" + key + "?" + SIGNATURE,
                    Instant.now().plus(ttl));
        }

        @Override public Capabilities capabilities() {
            Capabilities c = delegate.capabilities();
            return new Capabilities(c.conditionalWrites(), c.batchDelete(), true,
                    c.maxKeyBytes(), c.minPartSize(), c.costs());
        }

        @Override public java.io.InputStream get(String k) throws IOException {
            return delegate.get(k);
        }

        @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
            return delegate.getRange(k, a, b);
        }

        @Override public java.util.Optional<binjava.binstore.ObjectStat> stat(String k)
                throws IOException {
            return delegate.stat(k);
        }

        @Override public binjava.binstore.Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
                throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfMatch(
                String k, Body b, binjava.binstore.Version v) throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public binjava.binstore.ListPage list(String p, String a, int m)
                throws IOException {
            return delegate.list(p, a, m);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * Runs {@code work} with the streams AND the logging system captured, and
     * returns everything written.
     *
     * <p>⚠️ THE STREAM SWAP ALONE IS NOT ENOUGH, and round-1 review measured
     * exactly how it fails. {@code System.Logger} is the only logging facility
     * in this codebase (java-style.md; the sequencer alone has four
     * {@code System.getLogger} sites), and JUL's {@code ConsoleHandler} binds
     * {@code System.err} when the HANDLER is constructed. Once anything in the
     * JVM has logged — any earlier test in the same worker counts — later
     * records go to the ORIGINAL stream and never reach the buffer below.
     * MEASURED: a {@code System.Logger} leak of the grant SURVIVED the stream
     * swap at both INFO and ERROR, while the identical {@code println} was
     * caught. Criterion 7's "appears in no LOG" was the clause left unasserted.
     *
     * <p>⚠️ SO A JUL HANDLER IS ATTACHED TO THE ROOT LOGGER TOO, at
     * {@code Level.ALL} with the level forced, catching records however the
     * console handler is bound. Both instruments feed one buffer, so a leak
     * through either fails the same assertion.
     */
    private static String capturing(ThrowingRunnable work) throws Exception {
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
                logged.append(record.getMessage()).append('\n');
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

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Issuing a grant writes nothing containing the signature.
     *
     * <p>⚠️ THE HAPPY PATH IS THE WEAK HALF and is here for completeness: a
     * deliberate {@code System.out.println(grant)} would be caught by
     * {@code SignedUrlTest}'s redacting {@code toString} anyway. What this adds
     * is that nothing ELSE — a logging framework, a debug print left behind —
     * emitted it either.
     */
    @Test
    void issuingAGrantWritesNOTHINGContainingTheSignature() throws Exception {
        try (SigningStore store = new SigningStore(null)) {
            GrantIssuer issuer = new GrantIssuer(store);
            SignedUrl[] granted = new SignedUrl[1];
            String output = capturing(() -> granted[0] = issuer.grantFor(SEGMENT));

            assertThat(granted[0].url()).contains(SIGNATURE);
            assertThat(output)
                    .as("a grant was minted and its signature must be nowhere in the output")
                    .doesNotContain(SIGNATURE)
                    .doesNotContain("X-Amz-Signature")
                    .doesNotContain("https://store.example");
        }
    }

    /**
     * A signing FAILURE names the key and never the grant.
     *
     * <p>⚠️ THIS IS THE INTERESTING PATH. An exception message is built out of
     * whatever is in scope by code that never read ADR-0041, and printing a
     * stack trace is what an operator does first. The wrapper
     * {@code GrantIssuer} adds must carry the object KEY — a name, not a
     * credential — and nothing else.
     *
     * <p>⚠️ AND THE KEY MUST BE THERE, not merely the URL absent. Rule 4 obeyed
     * by making the system undebuggable is a rule someone will quietly break
     * back: "which segment failed" has to stay answerable from the message.
     */
    @Test
    void aSigningFailureNamesTheKEYAndNeverTheGRANT() throws Exception {
        // ⚠️ A BADLY-BEHAVED BACKEND, deliberately. Round-1 review found the
        // "never the GRANT" half of this test UNFAILABLE: the store threw
        // before minting, so no reachable expression could contain a
        // signature, and interpolating the cause -- `+ ": " +
        // signingFailed.getMessage()` -- survived. That interpolation is the
        // amplifier, and security.md rule 4 names CREDENTIALS as well as URLs:
        // a signing failure is the likeliest place in the system for one to
        // surface in text. So the cause carries a signature here, and the
        // assertion is that GrantIssuer does not repeat it.
        IOException backendFailure =
                new IOException("credential vend refused for " + SIGNATURE);
        try (SigningStore store = new SigningStore(backendFailure)) {
            GrantIssuer issuer = new GrantIssuer(store);

            String output = capturing(() ->
                    assertThatThrownBy(() -> issuer.grantFor(SEGMENT))
                            .isInstanceOf(IOException.class)
                            .satisfies(e -> {
                                // ⚠️ THE MESSAGE, NOT THE STACK TRACE. The cause
                                // IS chained -- deliberately, because losing it
                                // makes a signing failure undiagnosable -- so a
                                // trace prints whatever the backend wrote. That
                                // residual is the BACKEND's rule-4 defect and is
                                // stated in `BinStore.presign`'s javadoc, where a
                                // backend author reads it; M5.42 owns checking
                                // it once a capable backend exists.
                                assertThat(e.getMessage()).doesNotContain(SIGNATURE);
                                System.err.println(e.getMessage());
                            }));

            assertThat(output)
                    .as("GrantIssuer's own message must not repeat what the backend said, "
                            + "however careless the backend was")
                    .doesNotContain(SIGNATURE)
                    .doesNotContain("X-Amz-Signature");
            assertThat(output)
                    .as("but the KEY must be there, or rule 4 is obeyed by making failures "
                            + "undiagnosable and someone will break it back")
                    .contains(SEGMENT);
        }
    }

    /**
     * The startup refusal names neither a grant nor a credential.
     *
     * <p>⚠️ IT CANNOT HOLD ONE — no grant has been minted at that point — which
     * is exactly why it is worth asserting: this is the message an operator
     * will paste into a ticket, and it must stay safe to paste as the check
     * grows.
     */
    @Test
    void theSTARTUPRefusalIsSafeToPasteIntoATicket() throws Exception {
        try (MemoryBinStore cannot = new MemoryBinStore()) {
            String output = capturing(() ->
                    assertThatThrownBy(() -> new GrantIssuer(cannot))
                            .isInstanceOf(IllegalStateException.class)
                            .satisfies(e -> System.err.println(e.getMessage())));
            assertThat(output)
                    .doesNotContain(SIGNATURE)
                    .doesNotContain("https://");
            assertThat(output)
                    .as("and it must say what to DO -- the backend cannot presign, so `direct` "
                            + "is the setting to change")
                    .contains("direct");
        }
    }
}
