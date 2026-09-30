// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * M11.24: `DefaultIngest`, `Assembly` and `BulkService` are refused at their
 * split ceilings, one case per file, so the splits M11.24a-c made cannot grow
 * back as M11's did (M11 criterion 1, OBSERVED-NOT at close). M13.1a and
 * M13.1b lower `DefaultIngest`'s and `Assembly`'s to 500, the headroom fast
 * mode needs, and M13.1c and M13.1d name `ConsumerClient` and
 * `LocalSequencer` at 600 (M13 criterion 1).
 */
class FileSizeCeilingTest {

    private static final String DEFAULT_INGEST =
            "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/DefaultIngest.java";
    private static final String ASSEMBLY =
            "server/src/main/java/io/github/huyz0/os/biningester/server/Assembly.java";
    private static final String BULK_SERVICE =
            "http/src/main/java/io/github/huyz0/os/biningester/http/BulkService.java";
    private static final String CONSUMER_CLIENT =
            "client/src/main/java/io/github/huyz0/os/biningester/client/ConsumerClient.java";
    private static final String LOCAL_SEQUENCER =
            "sequencer/src/main/java/io/github/huyz0/os/biningester/sequencer/LocalSequencer.java";

    /**
     * Each named file's ceiling: {@code DefaultIngest} at 500 since M13.1a,
     * {@code Assembly} at 500 since M13.1b, {@code BulkService} at 600,
     * {@code ConsumerClient} at 600 since M13.1c, {@code LocalSequencer} at 600
     * since M13.1d.
     */
    private static final Map<String, Integer> CEILINGS = Map.of(
            DEFAULT_INGEST, 500, ASSEMBLY, 500, BULK_SERVICE, 600, CONSUMER_CLIENT, 600,
            LOCAL_SEQUENCER, 600);

    @Test
    void theCeilingNamesExactlyTheSplitFilesAtTheirCeilingsAndTheTreeMeetsIt() {
        assertThat(RepositoryGateChecks.INSTANCE.getSPLIT_CEILINGS()).isEqualTo(CEILINGS);
        List<String> failures = new ArrayList<>();
        RepositoryGateChecks.INSTANCE.splitCeilings(repository(), failures);
        assertThat(failures).as("this tree, after M13.1d").isEmpty();
    }

    @Test
    void defaultIngestIsRefusedAt500LinesAndAdmittedAt499() throws Exception {
        assertRefusedAtCeiling(DEFAULT_INGEST);
    }

    @Test
    void assemblyIsRefusedAt500LinesAndAdmittedAt499() throws Exception {
        assertRefusedAtCeiling(ASSEMBLY);
    }

    @Test
    void bulkServiceIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAtCeiling(BULK_SERVICE);
    }

    @Test
    void consumerClientIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAtCeiling(CONSUMER_CLIENT);
    }

    @Test
    void localSequencerIsRefusedAt600LinesAndAdmittedAt599() throws Exception {
        assertRefusedAtCeiling(LOCAL_SEQUENCER);
    }

    @Test
    void aRefusalReportsTheFilesOwnLineCountNotTheCeiling() throws Exception {
        Path root = scratch("count");
        try {
            for (Map.Entry<String, Integer> named : CEILINGS.entrySet()) {
                write(root.resolve(named.getKey()), named.getValue() - 1);
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

            assertThat(failures).hasSize(CEILINGS.size()).anyMatch(message -> message.contains(ASSEMBLY)
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

    /** Every named file one line under its ceiling, then {@code path} at its ceiling. */
    private static void assertRefusedAtCeiling(String path) throws Exception {
        Path root = scratch("ceiling");
        try {
            for (Map.Entry<String, Integer> named : CEILINGS.entrySet()) {
                write(root.resolve(named.getKey()), named.getValue() - 1);
            }
            List<String> underCeiling = new ArrayList<>();
            RepositoryGateChecks.INSTANCE.splitCeilings(root, underCeiling);
            assertThat(underCeiling).as("one line under the ceiling").isEmpty();

            int ceiling = CEILINGS.get(path);
            write(root.resolve(path), ceiling);
            List<String> atCeiling = new ArrayList<>();
            RepositoryGateChecks.INSTANCE.splitCeilings(root, atCeiling);
            assertThat(atCeiling).as("the ceiling is refused, naming the file").singleElement()
                    .satisfies(message -> assertThat(message).contains(path + " has " + ceiling + " lines"));
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
