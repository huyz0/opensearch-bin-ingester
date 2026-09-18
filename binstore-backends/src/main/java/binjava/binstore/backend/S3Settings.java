// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import java.util.Objects;

/**
 * What an S3-compatible endpoint needs to be talked to (M8.2, ADR-0054).
 *
 * <p>⚠️ **THE CREDENTIAL IS NOT HERE, AND THAT IS THE POINT.** Nothing in this
 * record carries a secret: the SDK's default provider chain reads the
 * environment, the container's role or the profile, so a deployment gives the
 * pod an identity rather than giving this project a password to hold. The one
 * exception is a static pair for a TEST fixture, which
 * {@link S3BinStore#open(S3Settings, software.amazon.awssdk.auth.credentials.AwsCredentialsProvider)}
 * takes explicitly — so a credential in a field is a call site a reviewer can
 * grep for. ⚠️ security.md rule 5 and the "never put object-store credentials in
 * index settings" line in AGENTS.md are the same rule read from the other end.
 *
 * @param endpoint the S3 endpoint, or null for the region's AWS endpoint. ⚠️ A
 *     MinIO or Ceph deployment sets this; AWS does not, and an endpoint written
 *     out by hand for AWS is how a deployment ends up pinned to one region's
 *     hostname.
 * @param region the region. ⚠️ REQUIRED EVEN FOR MinIO, which ignores it: SigV4
 *     signs over it, so a client with none cannot sign at all.
 * @param bucket the one bucket this store reads and writes
 * @param pathStyle whether to address the bucket in the path rather than in the
 *     hostname. ⚠️ **TRUE FOR EVERY NON-AWS ENDPOINT THIS PROJECT HAS MET**:
 *     virtual-host addressing needs a wildcard DNS entry per bucket, which a
 *     MinIO reached at `localhost` does not have.
 * @param maxKeyBytes the longest key this endpoint accepts. ⚠️ **AN ENDPOINT
 *     PROPERTY, NOT A PROTOCOL ONE, AND MEASURED RATHER THAN ASSUMED**: S3
 *     documents 1,024 bytes, and MinIO refuses at 256 bytes per path component
 *     with "Object name contains unsupported characters" — measured at 255
 *     accepted and 256 refused, and at 254 bytes of two-byte characters accepted
 *     against 256 refused, so it is a BYTE limit rather than a character one.
 *     A backend that advertised 1,024 against such an endpoint would pass a
 *     startup check and fail at the first long key.
 */
public record S3Settings(String endpoint, String region, String bucket, boolean pathStyle,
        long maxKeyBytes) {

    /** ⚠️ S3's own documented limit, which is what an unqualified endpoint gets. */
    public static final long S3_MAX_KEY_BYTES = 1024;

    /** The same, at S3's key limit. */
    public S3Settings(String endpoint, String region, String bucket, boolean pathStyle) {
        this(endpoint, region, bucket, pathStyle, S3_MAX_KEY_BYTES);
    }

    public S3Settings {
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(bucket, "bucket");
        if (region.isBlank()) {
            throw new IllegalArgumentException("a region is required: SigV4 signs over it, so a "
                    + "client without one cannot sign a request at all");
        }
        if (bucket.isBlank()) {
            throw new IllegalArgumentException("a bucket is required");
        }
        if (maxKeyBytes <= 0) {
            throw new IllegalArgumentException("a key limit is positive: " + maxKeyBytes);
        }
    }
}
