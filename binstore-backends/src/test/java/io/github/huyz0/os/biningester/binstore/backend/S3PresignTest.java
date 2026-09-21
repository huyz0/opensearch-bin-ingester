// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.huyz0.os.biningester.binstore.SignedUrl;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The S3 backend's presigning, where no network is needed to judge it (M8.18,
 * M5.37, M5.42, ADR-0041).
 *
 * <p>⚠️ **THE HALF NO TEST ABOVE A BACKEND COULD BUY.** {@code BinStore.presign}
 * requires that signing issue no request per signature, and a meter wrapped
 * around a store cannot see the store's own traffic. HERE the store's client is
 * this test's, and it refuses every call.
 */
class S3PresignTest {

    private static final S3Settings SETTINGS =
            new S3Settings("http://localhost:9", "us-east-1", "bucket", true);

    /** Every call is a request, and every request is refused. */
    private static final class RefusingClient implements S3Client {
        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {
        }
    }

    private static S3BinStore store(AwsCredentialsProvider credentials) {
        return new S3BinStore(new RefusingClient(), SETTINGS.bucket(), null,
                SETTINGS.maxKeyBytes(), S3BinStore.presigner(SETTINGS, credentials));
    }

    @Test
    void signingISSUESNoRequestAndNAMESTheKey() throws Exception {
        try (S3BinStore s3 = store(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("AKIDEXAMPLE", "secret")))) {
            assertThat(s3.capabilities().presignedUrls())
                    .as("the premise: a store with a presigner says it can presign").isTrue();

            SignedUrl signed = s3.presign("seg/one", Duration.ofSeconds(30));

            assertThat(signed.url())
                    .as("⚠️ SIGNED WITHOUT A SINGLE CALL ON THE CLIENT, every one of which "
                            + "throws: a `stat` or a round trip inside `presign` would be one "
                            + "request per grant, on the path `direct` exists to make cheap")
                    .contains("seg/one")
                    .contains("X-Amz-Signature=");
        }
    }

    @Test
    void aSIGNINGFailureCarriesNEITHERTheCredentialNORAUrl() {
        // ⚠️ security.md RULE 4, M5.42's obligation: whatever this throws may
        // be chained, printed and pasted into a ticket, and a signing failure
        // is the likeliest place in the system for a credential to surface.
        String secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        AwsCredentialsProvider broken = () -> {
            throw SdkClientException.create("could not refresh: key AKIAIOSFODNN7EXAMPLE secret "
                    + secret + " from https://sts.example/?X-Amz-Signature=abc");
        };
        Throwable thrown;
        try (S3BinStore s3 = store(broken)) {
            thrown = catchThrowable(() -> s3.presign("seg/one", Duration.ofSeconds(30)));
        } catch (Exception closing) {
            throw new AssertionError(closing);
        }

        assertThat(thrown).as("the failure is reported").isInstanceOf(java.io.IOException.class);
        StringWriter printed = new StringWriter();
        thrown.printStackTrace(new PrintWriter(printed));
        assertThat(printed.toString())
                .as("⚠️ THE WHOLE PRINTED TRACE, CAUSES INCLUDED, names no credential and no URL")
                .doesNotContain(secret)
                .doesNotContain("AKIAIOSFODNN7EXAMPLE")
                .doesNotContain("X-Amz-Signature")
                .doesNotContain("https://");
    }

    @Test
    void aSTOREWithNoPresignerSAYSSoAndREFUSES() throws Exception {
        try (S3BinStore s3 = new S3BinStore(new RefusingClient(), "bucket")) {
            assertThat(s3.capabilities().presignedUrls()).isFalse();
            assertThat(catchThrowable(() -> s3.presign("seg/one", Duration.ofSeconds(30))))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
