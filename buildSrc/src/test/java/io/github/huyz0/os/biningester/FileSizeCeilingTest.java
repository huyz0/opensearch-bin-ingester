// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * M11.24: `DefaultIngest`, `Assembly` and `BulkService` are refused at 600
 * lines, one case per file, so the splits M11.24a-c made cannot grow back as
 * M11's did (M11 criterion 1, OBSERVED-NOT at close).
 */
class FileSizeCeilingTest {

    private static final String DEFAULT_INGEST =
            "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/DefaultIngest.java";
    private static final String ASSEMBLY =
            "server/src/main/java/io/github/huyz0/os/biningester/server/Assembly.java";
    private static final String BULK_SERVICE =
            "http/src/main/java/io/github/huyz0/os/biningester/http/BulkService.java";

    @Test
    void theCeilingNamesExactlyTheThreeSplitFilesAt600AndTheTreeMeetsIt() {
        assertThat(RepositoryGateChecks.INSTANCE.getSPLIT_CEILINGS().keySet())
                .isEqualTo(Set.of(DEFAULT_INGEST, ASSEMBLY, BULK_SERVICE));
        assertThat(RepositoryGateChecks.INSTANCE.getSPLIT_CEILINGS().values())
                .containsOnly(600);
        List<String> failures = new ArrayList<>();
        RepositoryGateChecks.INSTANCE.splitCeilings(repository(), failures);
        assertThat(failures).as("this tree, after M11.24a-c").isEmpty();
    }

    @Test
    void defaultIngestIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAt600(DEFAULT_INGEST);
    }

    @Test
    void assemblyIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAt600(ASSEMBLY);
    }

    @Test
    void bulkServiceIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAt600(BULK_SERVICE);
    }

    @Test
    void aRefusalReportsTheFilesOwnLineCountNotTheCeiling() throws Exception {
        Path root = scratch("count");
        try {
            for (String named : List.of(DEFAULT_INGEST, ASSEMBLY, BULK_SERVICE)) {
                write(root.resolve(named), 599);
            }
            write(root.resolve(ASSEMBLY), 650);
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.splitCeilings(root, failures);

            assertThat(failures).singleElement()
                    .satisfies(message -> assertThat(message).contains(ASSEMBLY + " has 650 lines"));
        } finally {
            delete(root);
        }
    }

    @Test
    void aNamedFileThatNoLongerExistsIsRefusedRatherThanSkipped() throws Exception {
        Path root = scratch("missing");
        try {
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.splitCeilings(root, failures);

            assertThat(failures).hasSize(3).anyMatch(message -> message.contains(ASSEMBLY)
                    && message.contains("does not exist"));
        } finally {
            delete(root);
        }
    }

    /**
     * ⚠️ THE PREDICATE IS ONLY A GATE WHILE {@code ./gradlew gates} CALLS IT: a
     * ceiling proved correct here and dropped from {@code verify()} would refuse
     * nothing (M11.24 review T3).
     */
    @Test
    void theRepositoryGatesTaskRunsTheCeiling() throws Exception {
        String task = Files.readString(repository().resolve(
                "buildSrc/src/main/kotlin/io/github/huyz0/os/biningester/RepositoryGatesTask.kt"));
        String verify = task.substring(task.indexOf("fun verify()"),
                task.indexOf("private fun trackedTree"));

        assertThat(verify).contains("RepositoryGateChecks.splitCeilings(root, failures)");
    }

    /** Every named file at 599 lines, then {@code path} at 600. */
    private static void assertRefusedAt600(String path) throws Exception {
        Path root = scratch("ceiling");
        try {
            for (String named : List.of(DEFAULT_INGEST, ASSEMBLY, BULK_SERVICE)) {
                write(root.resolve(named), 599);
            }
            List<String> underCeiling = new ArrayList<>();
            RepositoryGateChecks.INSTANCE.splitCeilings(root, underCeiling);
            assertThat(underCeiling).as("599 lines is under the ceiling").isEmpty();

            write(root.resolve(path), 600);
            List<String> atCeiling = new ArrayList<>();
            RepositoryGateChecks.INSTANCE.splitCeilings(root, atCeiling);
            assertThat(atCeiling).as("600 lines is refused, naming the file").singleElement()
                    .satisfies(message -> assertThat(message).contains(path + " has 600 lines"));
        } finally {
            delete(root);
        }
    }

    /** The checkout's root: the harness runs with {@code buildSrc} as its working directory. */
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }

    /**
     * ⚠️ UNDER {@code buildSrc/build/tmp}, NEVER THE SYSTEM TEMP DIRECTORY
     * (testing.md rule 17): this class runs in every {@code ./gradlew gates},
     * so a system-temp fixture is a leak per pre-commit (M11.24 review T1).
     */
    private static Path scratch(String name) throws Exception {
        Path dir = repository().resolve("buildSrc/build/tmp/file-size-ceiling")
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

    private static void write(Path file, int lines) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, "x\n".repeat(lines));
    }
}
