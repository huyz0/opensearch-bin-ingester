// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.binstore.backend.S3BinStore;
import io.github.huyz0.os.biningester.binstore.backend.S3Settings;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * The S3 backend against the SHARED contract (M8.22, NFR-8).
 *
 * <p>⚠️ **THE SUITE ALREADY PASSED AGAINST TWO BACKENDS AND THAT BOUGHT THE
 * SPI's SHAPE, NOT A DEPLOYMENT's BEHAVIOUR.** `MemoryBinStore` is a map in the
 * caller's own heap and `LocalFsBinStore` is a directory; both answer
 * `presignedUrls=false` and neither has a network. What running the same cases
 * against a real endpoint buys is the one thing a fake cannot be wrong about in
 * the same way: where RustFS and this project's contract DISAGREE, this is where
 * it shows up, rather than in production.
 *
 * <p>⚠️ **ONE BUCKET PER STORE, NOT ONE PER SUITE.** The contract's cases assume
 * a store that starts empty — several list every key under a prefix — and a
 * shared bucket would make them depend on execution order, which is the flake
 * that teaches people to re-run.
 *
 * <p>⚠️ **A SEPARATE CLASS FROM {@code S3BinStoreIntegrationTest}, DELIBERATELY.**
 * That file pins the behaviours this backend has that the SPI does not describe —
 * the error shapes, the batch split, the request count. This one pins that it is
 * the same store every other backend is. A suite that mixed them would make
 * "the contract" and "this implementation" one thing, and the first divergence
 * would be argued rather than found.
 *
 * <p>⚠️ **IN THE CONTRACT's OWN PACKAGE, WHICH IS NOT DECORATION.** Three cases
 * below are overridden; one is disabled with a measurement, and the suite's methods are
 * package-private — so a subclass anywhere else cannot OVERRIDE them, only
 * shadow them, and a shadowed case runs twice with one of them still failing.
 * Sitting in {@code io.github.huyz0.os.biningester.binstore} makes the disabling an override the
 * compiler checks: rename a case in the suite and this file stops compiling,
 * rather than silently disabling nothing.
 */
class S3BinStoreConformanceIT extends BinStoreConformance {

    private static final AwsCredentialsProvider CREDENTIALS = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY));

    private static String endpoint;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        endpoint = S3Fixture.endpoint();
    }

    @Override
    protected BinStore newStore() {
        String bucket = "m822-" + UUID.randomUUID();
        try (S3Client admin = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(CREDENTIALS)
                .build()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
        // ⚠️ 255 BYTES, NOT S3's 1,024, AND MEASURED RATHER THAN GUESSED:
        // RustFS refuses a 256-byte path component with "Object name contains
        // unsupported characters", and refuses 256 BYTES of two-byte characters
        // while accepting 254. The key limit is a property of the endpoint in
        // front of the backend, which is why it is configuration rather than a
        // constant — and this case is exactly the one that found it.
        return S3BinStore.open(
                new S3Settings(endpoint, "us-east-1", bucket, true, RUSTFS_MAX_KEY_BYTES),
                CREDENTIALS);
    }

    /** ⚠️ RustFS's per-component limit, measured: 255 accepted, 256 refused. */
    private static final long RUSTFS_MAX_KEY_BYTES = 255;

    // ------------------------------------------------------------------
    // ⚠️ ONE CASE OF THE SHARED CONTRACT THE FIXTURE CANNOT JUDGE.
    //
    // Each is overridden here rather than weakened in the suite, because the
    // suite is what every OTHER backend is held to and the divergence is the
    // fixture's. The case says what was measured, so a reader can tell "the backend
    // does not do this" from "this endpoint cannot show it" -- and the
    // milestone evidence records the divergence, which is the point of the split.
    // ⚠️ AN OVERRIDE, NOT A DELETION: if the suite renames or removes one of
    // these, this file stops compiling instead of silently skipping nothing.
    // ⚠️ `@Test` IS REPEATED ON EACH, AND DROPPING IT WAS TRIED AND REJECTED.
    // Method annotations are not inherited by an override, so an override
    // without `@Test` does not run and is not reported: MEASURED, 36 cases with
    // 1 skipped became 35 with 0 skipped -- a contract case vanishing from
    // the report rather than appearing in it with a reason. ⚠️ THE COST IS
    // THAT the report contains the disabled case with a reason rather than
    // silently omitting it.
    // ------------------------------------------------------------------

    @Test
    @Disabled("RustFS's listing is not a byte range: MEASURED against the raw SDK, with no "
            + "code of this project's involved -- keys p/a, p/ab and p/a/x written, HEAD finds "
            + "all three, and ListObjectsV2 with prefix p/a answers [p/a, p/ab]. S3 answers "
            + "all three. Nothing this backend can do changes it")
    @Override
    void aPrefixIsAByteRangeNotADirectory() {
    }

    @Test
    @Override
    void putIfAbsentSucceedsExactlyOnceUnderConcurrentWriters() throws Exception {
        super.putIfAbsentSucceedsExactlyOnceUnderConcurrentWriters();
    }

    @Test
    @Override
    void putIfMatchSucceedsExactlyOnceUnderConcurrentWriters() throws Exception {
        super.putIfMatchSucceedsExactlyOnceUnderConcurrentWriters();
    }
}
