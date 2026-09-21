// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Which verbs the SDK retries, and which it must not (M8.22, ADR-0002).
 *
 * <p>⚠️ **A RETRIED CONDITIONAL WRITE IS A CORRECTNESS HAZARD.** A
 * {@code putIfAbsent} whose first attempt SUCCEEDED and whose response was lost
 * answers 412 on the retry, which the backend reports as "someone else got
 * there" — a writer that won being told it lost, in the one place the commit
 * log cannot tell the difference. Every other verb here is idempotent and worth
 * retrying, and an earlier draft turned retries off for all of them.
 *
 * <p>⚠️ **NO CONTAINER, AND NO MinIO.** What this needs is an endpoint that
 * does not answer, which is a closed port — so this case runs wherever a socket
 * can be opened. It lives in the T3 source set because it makes a real network
 * call, not because it needs the fixture.
 *
 * <p>⚠️ **THE ATTEMPT COUNT IS IN THE SDK's OWN MESSAGE**, which is what makes
 * this assertable at all. It is PARSED and COMPARED rather than matched as a
 * string: the count a read reaches is the SDK's default and its wording is the
 * SDK's, so a dependency bump that moved either would read as a regression
 * here. Review measured that deleting the per-request plugin left every other
 * case in this module green.
 *
 * <p>⚠️ **THE DEAD PORT IS BOUND AND RELEASED**, so another process can take it
 * in the window between. It is the conventional trick and the probability is
 * low; it is named because a single unrepeatable failure here is the one to
 * distrust rather than to investigate.
 */
class S3RetryScopeIT {

    @Test
    void aCONDITIONALWriteIsTriedONCEAndAGETIsRetried() throws Exception {
        int deadPort;
        try (ServerSocket taken = new ServerSocket(0)) {
            deadPort = taken.getLocalPort();
        }
        var credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("nobody", "nothing"));
        byte[] raw = "x".getBytes(StandardCharsets.UTF_8);
        Body body = new Body(raw.length, () -> new java.io.ByteArrayInputStream(raw));

        try (var store = S3BinStore.open(new S3Settings("http://localhost:" + deadPort,
                "us-east-1", "bucket", true), credentials)) {
            Throwable conditional = org.assertj.core.api.Assertions.catchThrowable(
                    () -> store.putIfAbsent("chain/000001", body));
            assertThat(conditional)
                    .as("⚠️ ONE ATTEMPT. A second one cannot tell its own landed write from "
                            + "a rival's, and answers 412 either way")
                    .isInstanceOf(IOException.class);
            assertThat(attemptsIn(conditional.getMessage())).isEqualTo(1);
            Throwable matched = org.assertj.core.api.Assertions.catchThrowable(
                    () -> store.putIfMatch("lease/term", body, new Version("\"v\"")));
            assertThat(matched).isInstanceOf(IOException.class);
            assertThat(attemptsIn(matched.getMessage())).isEqualTo(1);

            // ⚠️ AN `IOException`, NOT AN `UncheckedIOException`. The SDK
            // opens the body, the connect fails, and it closes the stream
            // unread -- `Body.checkedStream` then throws from inside the SDK's
            // own machinery, which wraps it. The SPI declares `IOException` on
            // every write, so a caller catching what the SPI promises was being
            // unwound by an unchecked throw. MEASURED against this closed port.
            assertThatThrownBy(() -> store.put("seg/one", body))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(java.io.UncheckedIOException.class);

            // ⚠️ COMPARATIVE, NOT A HARD-CODED 4. The SDK's default attempt
            // count and its message format are the SDK's to change, and a
            // dependency bump that moved either would read here as a regression
            // in this project. What this commit decides is that a read is
            // retried and a conditional write is not.
            assertThatThrownBy(() -> store.get("seg/one"))
                    .as("⚠️ AND A READ IS STILL RETRIED. It is idempotent, the endpoint really "
                            + "does drop connections -- this milestone measured it -- and an "
                            + "earlier draft disabled retries for every verb at once")
                    .isInstanceOf(IOException.class)
                    .satisfies(read -> assertThat(attemptsIn(read.getMessage()))
                            .isGreaterThan(attemptsIn(conditional.getMessage())));
        }
    }
    /**
     * How many attempts the SDK says it made.
     *
     * <p>⚠️ **PARSED RATHER THAN MATCHED**, so the comparison above is about
     * the numbers and not about the SDK's wording — and a message that stops
     * carrying the count fails loudly here rather than making a case pass by
     * matching nothing.
     */
    private static int attemptsIn(String message) {
        var matcher = java.util.regex.Pattern.compile("SDK Attempt Count: (\\d+)").matcher(message);
        if (!matcher.find()) {
            throw new AssertionError("no attempt count in: " + message);
        }
        return Integer.parseInt(matcher.group(1));
    }
}
