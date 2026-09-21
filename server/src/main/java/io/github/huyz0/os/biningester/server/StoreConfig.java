// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Which object store this pod writes to, as configuration rather than as code
 * (M8.1, extended for the S3-compatible backend at M8.4).
 *
 * <p>⚠️ A NAME AND NOT A CLASS. The kind is the string an operator writes; the
 * one place it becomes a type is {@link StoreFactory}, which is the only site
 * in {@code src/main} permitted to name a backend (M8's criterion 2, enforced
 * by {@code check-module.sh} once M8.29 extends it).
 *
 * <p>⚠️ **NO CREDENTIAL IS IN THIS RECORD AND NONE MAY BE ADDED.** The SDK's
 * default provider chain reads the environment, the container's role or the
 * profile, so a deployment gives the pod an identity rather than giving this
 * project a password to hold — the same rule read from the other end as
 * security.md rule 5 and AGENTS.md's "never put object-store credentials in
 * index settings". A static pair exists for a TEST fixture only, and it is
 * passed to {@code S3BinStore.open} explicitly so that a credential in a field
 * is a call site a reviewer can grep for.
 *
 * @param kind {@code memory}, {@code local-fs} or {@code s3}
 * @param root where a filesystem-backed store puts its objects, as the TEXT an
 *     operator configured, and ⚠️ **absent for every kind that has no
 *     filesystem** — see {@link StoreFactory} for why an ignored root is
 *     refused rather than dropped. ⚠️ **A STRING AND NOT A `Path`**, so that
 *     this module never names {@code java.nio.file} and
 *     {@code check-io-seam.sh}'s exempt list stays as short as it is
 * @param endpoint the S3 endpoint, ⚠️ **absent for AWS itself**: a hostname
 *     written out by hand for AWS is how a deployment ends up pinned to one
 *     region's endpoint. A MinIO or Ceph deployment sets it
 * @param region ⚠️ **required by `s3` EVEN AGAINST MinIO, which ignores it**:
 *     SigV4 signs over the region, so a client with none cannot sign at all
 * @param bucket the one bucket an {@code s3} store reads and writes
 * @param pathStyle whether to address the bucket in the path rather than in the
 *     hostname. ⚠️ True for every non-AWS endpoint this project has met:
 *     virtual-host addressing needs a wildcard DNS entry per bucket, which a
 *     MinIO reached at {@code localhost} does not have
 */
public record StoreConfig(String kind, Optional<String> root, Optional<String> endpoint,
        Optional<String> region, Optional<String> bucket, boolean pathStyle) {

    /**
     * A store with no S3 settings — every kind that is not {@code s3}.
     *
     * <p>⚠️ **NOT A DEFAULT FOR `s3`.** {@link StoreFactory} refuses an
     * {@code s3} built this way by name, because a bucket defaulted to anything
     * is a node that writes somewhere nobody configured.
     */
    public StoreConfig(String kind, Optional<String> root) {
        this(kind, root, Optional.empty(), Optional.empty(), Optional.empty(), false);
    }

    public StoreConfig {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(bucket, "bucket");
        if (kind.isBlank()) {
            // ⚠️ EARLY, for the reason `LeaseConfig` refuses a blank podId: an
            // unset environment variable otherwise constructs fine and fails
            // later, where the message names a backend nobody configured.
            throw new IllegalArgumentException("store kind is blank");
        }
    }

    /** The kind, trimmed and lower-cased — what {@link StoreFactory} matches on. */
    public String normalizedKind() {
        return kind.trim().toLowerCase(Locale.ROOT);
    }
}
