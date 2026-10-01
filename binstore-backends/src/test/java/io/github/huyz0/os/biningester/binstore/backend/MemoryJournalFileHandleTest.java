// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.JournalFileConformance;
import org.junit.jupiter.api.Test;

/**
 * The fake held to the same-handle and truncate-bound checks (M13.24 review
 * round 1, T1 and P4).
 */
class MemoryJournalFileHandleTest {

    private static final JournalFileConformance.Opener OPENER =
            new JournalFileConformance.Opener() {
                @Override
                public JournalFile create() {
                    return new MemoryJournalFile();
                }

                @Override
                public JournalFile reopen(JournalFile file) {
                    return file;
                }
            };

    @Test
    void appendsCONTINUEOnTheSameHandleAfterAReplaceAndATruncate() throws Exception {
        JournalFileConformance.appendsContinueOnTheSameHandleAfterAReplaceAndATruncate(OPENER);
    }

    @Test
    void aTruncatePASTTheEndIsRefused() throws Exception {
        JournalFileConformance.aTruncatePastTheEndIsRefused(OPENER);
    }
}
