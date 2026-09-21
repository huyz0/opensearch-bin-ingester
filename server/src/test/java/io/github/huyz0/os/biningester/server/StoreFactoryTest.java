// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The one place in {@code src/main} that turns a configured NAME into a
 * backend (M8.1, criterion 2).
 *
 * <p>⚠️ EVERY CASE HERE IS ABOUT A CONFIGURATION AN OPERATOR CAN GET WRONG,
 * not about the backends themselves — those have their own conformance suite.
 * What this class pins is that a wrong name is REFUSED rather than defaulted,
 * because a root that defaults an unrecognised store kind starts happily and
 * writes nowhere anyone expects.
 */
class StoreFactoryTest {

    @Test
    void theMEMORYKindBuildsAnInMemoryStore() throws Exception {
        try (BinStore store = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            assertThat(store.getClass().getName())
                    .as("the configured NAME chooses the backend, and nothing else does")
                    .isEqualTo("io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore");
        }
    }

    @Test
    void theLOCALFSKindBuildsAStoreROOTEDWhereItWasTold(@TempDir Path dir) throws Exception {
        Path root = dir.resolve("bins");
        try (BinStore store = StoreFactory.open(new StoreConfig("local-fs", Optional.of(root.toString())))) {
            store.put("k", io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
        }
        assertThat(java.nio.file.Files.walk(root).anyMatch(java.nio.file.Files::isRegularFile))
                .as("⚠️ THE ROOT IS READ FROM THE CONFIG, NOT DEFAULTED. A factory that "
                        + "ignored it would build a store under the process's working "
                        + "directory and every other assertion here would still pass")
                .isTrue();
    }

    @Test
    void anUNKNOWNKindIsREFUSEDAndTheMessageNamesWhatWasAsked() {
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("s4", Optional.empty())))
                .as("⚠️ NAMING THE VALUE IS THE POINT: an operator who typed `s4` for `s3` "
                        + "needs to see `s4` and not only a list of what was expected")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s4")
                .hasMessageContaining("store");
    }

    @Test
    void aLOCALFSKindWithNOROOTIsREFUSEDRatherThanDefaulted() {
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("local-fs", Optional.empty())))
                .as("⚠️ A DEFAULTED ROOT IS THE SILENT FAILURE THIS WHOLE CLASS EXISTS FOR: "
                        + "the node starts, writes somewhere under its working directory, "
                        + "and nothing reads it back")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("root");
    }

    @Test
    void aMEMORYKindWithAROOTIsREFUSEDRatherThanIGNORED(@TempDir Path dir) {
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("memory", Optional.of(dir.toString()))))
                .as("⚠️ AN IGNORED SETTING IS A LIE AN OPERATOR CANNOT SEE. Someone who "
                        + "configured a root meant durability; silently giving them a "
                        + "HashMap is the failure mode, not the tidy one")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("root");
    }

    @Test
    void theKINDIsCASEInsensitiveAndTRIMMED() throws Exception {
        try (BinStore store = StoreFactory.open(new StoreConfig("  MEMORY  ", Optional.empty()))) {
            assertThat(store.getClass().getName())
                    .as("⚠️ THE SAME BACKEND AS THE CANONICAL SPELLING, not merely "
                            + "non-null: a factory that fell through to a default for an "
                            + "unrecognised `  MEMORY  ` would satisfy an `isNotNull`, which "
                            + "is what an earlier draft of this case asserted")
                    .isEqualTo("io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore");
        }
    }
}
