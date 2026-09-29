// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * M12.22 (H25; M10.37 T3/T4; M12.7 review P3): three edges of the JVM gates
 * that the retired scripts' tests left unpinned, or that the JVM gates got
 * wrong themselves.
 */
class GateScannerEdgesTest {

    private static final String BASE = "src/main/java/io/github/huyz0/os/biningester/";
    /** Split, so this file cites no record: the gate reads it too. */
    private static final String PREFIX = "ADR" + "-";

    /**
     * ⚠️ A CHARACTER LITERAL HOLDING A QUOTE (M12.7 review P3): the stripper
     * removed strings with a regex that knew no character literals, so the
     * {@code '"'} paired with the NEXT string's opening quote and the code
     * between was deleted as if it were text -- a socket, or a 429, there was
     * invisible to every gate that strips. Four production files hold such a
     * literal.
     */
    @Test
    void aQuoteCharacterLiteralHidesNoCodeFromTheGates() throws Exception {
        Path root = scratch("char-literal");
        try {
            Path socket = write(root, "ingest/" + BASE + "ingest/Quoting.java",
                    "class Quoting { char q = '\"'; java.net.Socket s; String t = \"x\"; }");
            Path escaped = write(root, "ingest/" + BASE + "ingest/Escaping.java",
                    "class Escaping { char q = '\\''; java.net.ServerSocket s; String t = \"'\"; }");
            // ⚠️ A COMMENT OPENER INSIDE A STRING IS TEXT (review T3).
            Path url = write(root, "ingest/" + BASE + "ingest/Linking.java",
                    "class Linking { String u = \"http://x\"; java.net.Socket s; }");
            // ⚠️ A TEXT BLOCK IS TEXT, escaped quotes and all, and code after
            // it is code (review T1).
            String textBlock = "\"\"\"";
            Path inBlock = write(root, "ingest/" + BASE + "ingest/Prose.java",
                    "class Prose { String b = " + textBlock + "\n  java.net.Socket \"said\"\n"
                            + textBlock + "; int x; }");
            Path afterBlock = write(root, "ingest/" + BASE + "ingest/AfterBlock.java",
                    "class AfterBlock { String b = " + textBlock + "\n  a \\" + textBlock
                            + " b\n" + textBlock + "; java.net.Socket s; }");
            Path refusal = write(root, "server/" + BASE + "server/Door.java",
                    "class Door { char q = '\"'; void f() { res.status(429); } String m = \"y\"; }");
            List<String> seam = new ArrayList<>();
            List<String> refusals = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.ioSeam(root,
                    List.of(socket, escaped, url, inBlock, afterBlock), seam);
            RepositoryGateChecks.INSTANCE.singleTooManyRequests(root, List.of(refusal), refusals);

            assertThat(seam).as("⚠️ THE SOCKET AFTER A QUOTE LITERAL IS CODE, and is seen")
                    .anyMatch(message -> message.contains("Quoting.java"))
                    .anyMatch(message -> message.contains("Escaping.java"))
                    .anyMatch(message -> message.contains("Linking.java"))
                    .anyMatch(message -> message.contains("AfterBlock.java"))
                    .noneMatch(message -> message.contains("Prose.java"));
            assertThat(refusals).as("and so is the 429").singleElement()
                    .satisfies(message -> assertThat(message).contains("server/Door.java"));
        } finally {
            delete(root);
        }
    }

    /**
     * ⚠️ THE SHORT FORM IS REFUSED, EVEN WHERE ITS RECORD EXISTS (M10.37 T3):
     * the retired script padded a one-digit citation to four and let it
     * resolve; the JVM gate refuses anything but the zero-padded four-digit
     * id, so a citation is spelt one way. Pinned in the direction the gate
     * actually enforces, which is the stricter one.
     */
    @Test
    void onlyAZeroPaddedCitationOfAnExistingRecordResolves() throws Exception {
        Path root = scratch("adr");
        try {
            Path record = write(root, "docs/internal/product/decisions/0007-a-decision.md",
                    "# 0007. A decision");
            Path padded = write(root, "docs/a.md", "See " + PREFIX + "0007.");
            Path shortForm = write(root, "docs/b.md", "See " + PREFIX + "7.");
            Path missing = write(root, "docs/c.md", "See " + PREFIX + "0099.");
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.adrReferences(root,
                    List.of(record, padded, shortForm, missing), failures);

            assertThat(failures).hasSize(2)
                    .anyMatch(message -> message.contains("b.md") && message.contains(PREFIX + "7")
                            && message.contains(PREFIX + "0007"))
                    .anyMatch(message -> message.contains("c.md")
                            && message.contains(PREFIX + "0099"))
                    .noneMatch(message -> message.contains("a.md"));
        } finally {
            delete(root);
        }
    }

    /**
     * ⚠️ AN EXEMPTION IS ONE EXACT PATH (M10.37 T4): the io-seam gate exempts
     * six files by their full repository path, so a file that merely shares a
     * name with one -- in another module, a subpackage, or a directory whose
     * name ends with the module's -- is still under the gate.
     */
    @Test
    void anIoSeamExemptionCoversItsExactPathOnly() throws Exception {
        Path root = scratch("exempt");
        try {
            String body = "class Main { java.net.Socket s; }";
            Path exempt = write(root, "server/" + BASE + "server/Main.java", body);
            Path otherModule = write(root, "ingest/" + BASE + "server/Main.java", body);
            Path subpackage = write(root, "server/" + BASE + "server/sub/Main.java", body);
            Path lookalike = write(root, "xserver/" + BASE + "server/Main.java", body);
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.ioSeam(root,
                    List.of(exempt, otherModule, subpackage, lookalike), failures);

            assertThat(failures).hasSize(3)
                    .anyMatch(message -> message.contains("ingest"))
                    .anyMatch(message -> message.contains("sub"))
                    .anyMatch(message -> message.contains("xserver"));
        } finally {
            delete(root);
        }
    }

    private static Path write(Path root, String relative, String body) throws Exception {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, body + "\n");
        return file;
    }

    /** The checkout's root: the harness runs with {@code buildSrc} as its working directory. */
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }

    private static Path scratch(String name) throws Exception {
        Path dir = repository().resolve("buildSrc/build/tmp/gate-edges")
                .resolve(name + "-" + UUID.randomUUID());
        Files.createDirectories(dir);
        return dir;
    }

    private static void delete(Path root) throws Exception {
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
