// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.JournalFileConformance;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * The fake journal file, held to the conformance cases and to the crashes it
 * exists to inject (M13.24).
 *
 * <p>⚠️ A FAKE THAT NEVER LOSES ANYTHING MAKES EVERY JOURNAL TEST VACUOUS. The
 * journal's whole contract -- nothing answered before its force, a torn tail
 * truncated before the next append -- is about what a crash takes away, so
 * the fake must take it away exactly as ADR-0082 §4 assumes a disk does.
 */
class MemoryJournalFileTest {

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
    void aNEWFileIsEmpty() throws Exception {
        JournalFileConformance.aNewFileIsEmpty(OPENER);
    }

    @Test
    void appendsAreReadBackINOrder() throws Exception {
        JournalFileConformance.appendsAreReadBackInOrder(OPENER);
    }

    @Test
    void forcedBytesSURVIVEAReopen() throws Exception {
        JournalFileConformance.forcedBytesSurviveAReopen(OPENER);
    }

    @Test
    void aTRUNCATEIsDurableAndAppendsFollowIt() throws Exception {
        JournalFileConformance.aTruncateIsDurableAndAppendsFollowIt(OPENER);
    }

    @Test
    void aREPLACESwapsTheWholeContentsDurably() throws Exception {
        JournalFileConformance.aReplaceSwapsTheWholeContentsDurably(OPENER);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aCRASHLosesEveryUnforcedByte() {
        MemoryJournalFile file = new MemoryJournalFile();
        file.append(bytes("durable"));
        file.force();
        file.append(bytes("-lost"));

        file.crash();

        assertThat(file.readAll()).isEqualTo(bytes("durable"));
        assertThat(file.size()).isEqualTo(7);
    }

    @Test
    void aTEARINGCrashKeepsAPrefixOfTheUnforcedBytes() {
        MemoryJournalFile file = new MemoryJournalFile();
        file.append(bytes("ok|"));
        file.force();
        file.append(bytes("partial"));

        file.crashTearing(3);

        assertThat(file.readAll()).isEqualTo(bytes("ok|par"));
    }

    @Test
    void aTRUNCATEAndAREPLACESurviveACrash() {
        MemoryJournalFile file = new MemoryJournalFile();
        file.append(bytes("abcdef"));
        file.force();
        file.truncate(3);
        file.crash();
        assertThat(file.readAll()).as("a truncate is durable when it returns")
                .isEqualTo(bytes("abc"));

        file.replace(bytes("xy"));
        file.append(bytes("unforced"));
        file.crash();
        assertThat(file.readAll()).as("a replace is durable; what followed it unforced is not")
                .isEqualTo(bytes("xy"));
    }
}
