// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.JournalFileConformance;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The real file held to the same-handle and truncate-bound checks (M13.24
 * review round 1, T1 and P4).
 *
 * <p>⚠️ THE SAME-HANDLE CASE IS THE ONE THE JOURNAL LIVES ON: after a
 * compaction it appends on the handle it just replaced with. A replace that
 * left the channel closed failed every append after the first compaction, and
 * passed every case that reopened first.
 */
class FileJournalFileHandleTest {

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
    void appendsCONTINUEOnTheSameHandleAfterAReplaceAndATruncate() throws Exception {
        JournalFileConformance.appendsContinueOnTheSameHandleAfterAReplaceAndATruncate(opener);
    }

    @Test
    void aTruncatePASTTheEndIsRefused() throws Exception {
        JournalFileConformance.aTruncatePastTheEndIsRefused(opener);
    }
}
