// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A journal file named within a directory given as text (M13.27i): the server
 * names its {@code emptyDir} from configuration and may not reach the file
 * system itself (non-negotiable 7).
 */
class FileJournalFileNamedTest {

    @TempDir
    Path dir;

    @Test
    void theNAMEDFileInTheDirectoryIsOpened() throws Exception {
        try (JournalFile file = FileJournalFile.in(dir.toString(), "journal")) {
            file.append(new byte[] {7, 8});
            file.force();
        }

        assertThat(Files.readAllBytes(dir.resolve("journal"))).containsExactly(7, 8);
    }

    @Test
    void aMISSINGDirectoryIsCreated() throws Exception {
        try (JournalFile file = FileJournalFile.in(dir.resolve("fast").toString(), "epoch")) {
            file.replace(new byte[] {1});
        }

        assertThat(Files.readAllBytes(dir.resolve("fast").resolve("epoch"))).containsExactly(1);
    }
}
