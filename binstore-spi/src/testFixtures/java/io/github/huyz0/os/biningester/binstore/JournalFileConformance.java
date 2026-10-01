// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * What every {@link JournalFile} must do (M13.24, ADR-0083): the fake and the
 * real file are held to the same checks, so a test that passes against the
 * fake says something about the file.
 *
 * <p>⚠️ CHECKS, NOT {@code @Test} METHODS. Each implementation's test class
 * calls them from its own test methods, so every case is a test of a concrete
 * class -- one that can be run, and observed failing, on its own.
 *
 * <p>⚠️ "SURVIVES A REOPEN" IS THE DURABILITY A FILE CAN SHOW IN A TEST. A real
 * crash cannot be injected into a file system from here; what can be checked
 * is that forced, truncated and replaced contents are what a fresh handle on
 * the same storage reads. The crash cases -- unforced bytes lost, a torn
 * prefix kept -- belong to the fake, which the journal's own tests run against.
 */
public final class JournalFileConformance {

    /** Opens journal files on one storage. */
    public interface Opener {
        /** A new, empty journal file. */
        JournalFile create() throws IOException;

        /** A fresh handle on the same storage as {@code file}, after closing it. */
        JournalFile reopen(JournalFile file) throws IOException;
    }

    private JournalFileConformance() {
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static void aNewFileIsEmpty(Opener opener) throws Exception {
        try (JournalFile file = opener.create()) {
            assertThat(file.readAll()).isEmpty();
            assertThat(file.size()).isZero();
        }
    }

    public static void appendsAreReadBackInOrder(Opener opener) throws Exception {
        try (JournalFile file = opener.create()) {
            file.append(bytes("ab"));
            file.append(bytes("cde"));

            assertThat(file.readAll()).isEqualTo(bytes("abcde"));
            assertThat(file.size()).isEqualTo(5);
        }
    }

    public static void forcedBytesSurviveAReopen(Opener opener) throws Exception {
        JournalFile file = opener.create();
        file.append(bytes("held"));
        file.force();

        try (JournalFile again = opener.reopen(file)) {
            assertThat(again.readAll()).isEqualTo(bytes("held"));
            again.append(bytes("+more"));
            assertThat(again.readAll()).as("and appending continues at the end")
                    .isEqualTo(bytes("held+more"));
        }
    }

    public static void aTruncateIsDurableAndAppendsFollowIt(Opener opener) throws Exception {
        JournalFile file = opener.create();
        file.append(bytes("keep|torn"));
        file.force();
        file.truncate(5);

        try (JournalFile again = opener.reopen(file)) {
            assertThat(again.readAll()).isEqualTo(bytes("keep|"));
            again.append(bytes("next"));
            again.force();
            assertThat(again.readAll()).as("the tear is gone, so the next append is not lost "
                    + "behind it").isEqualTo(bytes("keep|next"));
            assertThat(again.size()).isEqualTo(9);
        }
    }

    /**
     * ⚠️ THE SAME HANDLE, NO REOPEN: the journal appends on the handle it just
     * compacted with. A replace that left its handle closed, or still on the
     * old file -- which on Linux survives the rename as an unlinked inode --
     * passes every case that reopens first (M13.24 review round 1, T1).
     */
    public static void appendsContinueOnTheSameHandleAfterAReplaceAndATruncate(Opener opener)
            throws Exception {
        JournalFile file = opener.create();
        file.append(bytes("old"));
        file.force();
        file.replace(bytes("new"));
        file.append(bytes("+1"));
        file.force();
        assertThat(file.readAll()).isEqualTo(bytes("new+1"));
        file.truncate(3);
        file.append(bytes("+2"));
        file.force();
        assertThat(file.readAll()).isEqualTo(bytes("new+2"));

        try (JournalFile again = opener.reopen(file)) {
            assertThat(again.readAll())
                    .as("and the appends landed in the journal, not in a replaced file")
                    .isEqualTo(bytes("new+2"));
        }
    }

    /** A truncate past the end is refused, never a hole the next recovery would cut at. */
    public static void aTruncatePastTheEndIsRefused(Opener opener) throws Exception {
        try (JournalFile file = opener.create()) {
            file.append(bytes("abc"));
            file.force();

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> file.truncate(10))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(file.readAll()).isEqualTo(bytes("abc"));
        }
    }

    public static void aReplaceSwapsTheWholeContentsDurably(Opener opener) throws Exception {
        JournalFile file = opener.create();
        file.append(bytes("old-old-old"));
        file.force();
        file.replace(bytes("new"));

        try (JournalFile again = opener.reopen(file)) {
            assertThat(again.readAll()).isEqualTo(bytes("new"));
            assertThat(again.size()).isEqualTo(3);
            again.append(bytes("!"));
            assertThat(again.readAll()).isEqualTo(bytes("new!"));
        }
    }
}
