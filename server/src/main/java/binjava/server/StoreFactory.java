// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.binstore.BinStore;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.binstore.backend.S3BinStore;
import binjava.binstore.backend.S3Settings;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * The one place in {@code src/main} that turns a configured NAME into a
 * backend (M8.1, M8.4, M8's criterion 2).
 *
 * <p>⚠️ EVERY OTHER MODULE TAKES A {@link BinStore} AND CANNOT NAME THIS
 * PACKAGE. That is the seam eight milestones of testability were bought with,
 * and a composition root is where it is finally spent — in exactly one file, so
 * that "who chooses the store" has a single answer a reader can check.
 *
 * <p>⚠️ **A MISCONFIGURATION IS REFUSED, NEVER DEFAULTED.** A root that falls
 * back to an in-memory store when it does not recognise a name starts happily,
 * acks writes, and loses every one of them on exit; a root that defaults a
 * missing filesystem root writes under the process's working directory, where
 * nothing reads it back. Both pass every test that does not look at the store,
 * which is most of them.
 *
 * <p>⚠️ **AN IGNORED SETTING IS REFUSED IN BOTH DIRECTIONS.** A missing one is
 * the obvious half; the other is a {@code store.bucket} configured against
 * {@code local-fs}, where an operator who wrote a bucket name believes their
 * data is in it. Every kind below therefore states what it needs AND refuses
 * what it cannot honour.
 */
public final class StoreFactory {

    private StoreFactory() {
    }

    /**
     * Builds the store {@code config} names.
     *
     * @throws IllegalArgumentException if the kind is unknown, or if the
     *     settings do not fit the kind — including a {@code root} given to a
     *     kind that has no filesystem. ⚠️ **AN IGNORED SETTING IS A LIE AN
     *     OPERATOR CANNOT SEE**: someone who configured a root meant
     *     durability, and silently handing them a map is worse than refusing.
     */
    public static BinStore open(StoreConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        String kind = config.normalizedKind();
        return switch (kind) {
            case "memory" -> {
                requireNoRoot(config, kind);
                requireNoS3Settings(config, kind);
                yield new MemoryBinStore();
            }
            case "local-fs" -> {
                requireNoS3Settings(config, kind);
                yield LocalFsBinStore.at(requireRoot(config, kind));
            }
            // ⚠️ NO CREDENTIAL IS PASSED, so the SDK's default provider chain
            // resolves the pod's identity. The overload that takes one exists
            // for the MinIO fixture and is not reachable from configuration --
            // which is what keeps "where does this process get its secret" a
            // question with one answer (security.md rule 5).
            case "s3" -> {
                requireNoRoot(config, kind);
                yield S3BinStore.open(new S3Settings(config.endpoint().orElse(null),
                        requireS3Setting(config.region(), kind, "store.region"),
                        requireS3Setting(config.bucket(), kind, "store.bucket"),
                        config.pathStyle()));
            }
            default -> throw new IllegalArgumentException(
                    "unknown store kind: " + config.kind() + " (known: memory, local-fs, s3)");
        };
    }

    private static String requireRoot(StoreConfig config, String kind) {
        return config.root().orElseThrow(() -> new IllegalArgumentException(
                "store kind " + kind + " needs a root and none was configured"));
    }

    private static void requireNoRoot(StoreConfig config, String kind) {
        if (config.root().isPresent()) {
            throw new IllegalArgumentException(
                    "store kind " + kind + " has no filesystem, so a root cannot be honoured: "
                            + config.root().get());
        }
    }

    private static String requireS3Setting(Optional<String> value, String kind, String key) {
        return value.orElseThrow(() -> new IllegalArgumentException(
                "store kind " + kind + " needs " + key + " and none was configured"));
    }

    /**
     * ⚠️ **THE OTHER DIRECTION OF THE SAME RULE.** An operator who wrote
     * {@code store.bucket} against {@code local-fs} believes their data is in
     * that bucket; the process that silently wrote it to a directory instead
     * passes every test and loses the argument at the incident review.
     */
    private static void requireNoS3Settings(StoreConfig config, String kind) {
        refuseSetting(config.endpoint(), kind, "store.endpoint");
        refuseSetting(config.region(), kind, "store.region");
        refuseSetting(config.bucket(), kind, "store.bucket");
        if (config.pathStyle()) {
            throw new IllegalArgumentException("store kind " + kind
                    + " talks to no endpoint, so store.path-style cannot be honoured");
        }
    }

    private static void refuseSetting(Optional<String> value, String kind, String key) {
        if (value.isPresent()) {
            throw new IllegalArgumentException("store kind " + kind
                    + " talks to no endpoint, so " + key + " cannot be honoured: " + value.get());
        }
    }
}
