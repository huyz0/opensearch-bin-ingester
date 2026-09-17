// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.binstore.BinStore;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.Objects;

/**
 * The one place in {@code src/main} that turns a configured NAME into a
 * backend (M8.1, M8's criterion 2).
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
                yield new MemoryBinStore();
            }
            case "local-fs" -> LocalFsBinStore.at(requireRoot(config, kind));
            default -> throw new IllegalArgumentException(
                    "unknown store kind: " + config.kind() + " (known: memory, local-fs)");
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
}
