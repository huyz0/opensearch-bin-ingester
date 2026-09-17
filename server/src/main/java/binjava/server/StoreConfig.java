// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Which object store this pod writes to, as configuration rather than as code
 * (M8.1).
 *
 * <p>⚠️ A NAME AND NOT A CLASS. The kind is the string an operator writes; the
 * one place it becomes a type is {@link StoreFactory}, which is the only site
 * in {@code src/main} permitted to name a backend (M8's criterion 2, enforced
 * by {@code check-module.sh} once M8.29 extends it).
 *
 * @param kind {@code memory} or {@code local-fs} today; {@code s3} at M8.2
 * @param root where a filesystem-backed store puts its objects, as the TEXT an
 *     operator configured, and ⚠️ **absent for every kind that has no
 *     filesystem** — see {@link StoreFactory} for why an ignored root is
 *     refused rather than dropped. ⚠️ **A STRING AND NOT A `Path`**, so that
 *     this module never names {@code java.nio.file} and
 *     {@code check-io-seam.sh}'s exempt list stays one module long
 */
public record StoreConfig(String kind, Optional<String> root) {

    public StoreConfig {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(root, "root");
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
