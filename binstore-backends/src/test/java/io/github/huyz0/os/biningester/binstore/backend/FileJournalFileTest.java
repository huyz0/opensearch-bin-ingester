// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.JournalFileConformance;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The journal on a real file (M13.24), held to the same cases as the fake.
 */
class FileJournalFileTest {

    @TempDir
    Path dir;

    private final JournalFileConformance.Opener opener = new JournalFileConformance.Opener() {
        @Override
        public JournalFile create() {
            return FileJournalFile.open(dir.resolve("journal"));
        }

        @Override
        public JournalFile reopen(JournalFile file) throws IOException {
            file.close();
            return FileJournalFile.open(dir.resolve("journal"));
        }
    };

    @Test
    void aNEWFileIsEmpty() throws Exception {
        JournalFileConformance.aNewFileIsEmpty(opener);
    }

    @Test
    void appendsAreReadBackINOrder() throws Exception {
        JournalFileConformance.appendsAreReadBackInOrder(opener);
    }

    @Test
    void forcedBytesSURVIVEAReopen() throws Exception {
        JournalFileConformance.forcedBytesSurviveAReopen(opener);
    }

    @Test
    void aTRUNCATEIsDurableAndAppendsFollowIt() throws Exception {
        JournalFileConformance.aTruncateIsDurableAndAppendsFollowIt(opener);
    }

    @Test
    void aREPLACESwapsTheWholeContentsDurably() throws Exception {
        JournalFileConformance.aReplaceSwapsTheWholeContentsDurably(opener);
    }

    private JournalFile create() throws IOException {
        return opener.create();
    }

    @Test
    void aREPLACELeavesNoTemporaryFileBehind() throws Exception {
        try (JournalFile file = create()) {
            file.append("old".getBytes(StandardCharsets.UTF_8));
            file.force();
            file.replace("new".getBytes(StandardCharsets.UTF_8));
        }

        try (var listing = Files.list(dir)) {
            assertThat(listing.map(p -> p.getFileName().toString()))
                    .as("the temporary file is renamed over the journal, never left to be "
                            + "mistaken for one -- or to fill the emptyDir")
                    .containsExactly("journal");
        }
    }
}
