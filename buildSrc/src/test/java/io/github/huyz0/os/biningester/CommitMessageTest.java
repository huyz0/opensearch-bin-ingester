// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.gradle.api.GradleException;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;

/**
 * M13.19 (M12 harvest R17): the commit message states its cost -- a body line
 * starting {@code Cost:} that says something, {@code Cost: none} when it is
 * none -- beside the subject's task ID, which had no case of its own.
 */
class CommitMessageTest {

    private static final String BACKLOG = "| M13.19 | R17 | — | open |\n";

    @Test
    void aBodyThatStatesItsCostPasses() {
        assertThat(CommitMessage.INSTANCE.failures(
                "M13.19 Require the cost line\n\nWhy.\n\nCost: none.\n", BACKLOG)).isEmpty();
        assertThat(CommitMessage.INSTANCE.failures(
                "M13.19 Require it\r\n\r\nCost: one GET per segment\r\n", BACKLOG))
                .as("a CRLF message").isEmpty();
    }

    @Test
    void aBodyWithoutACostLineIsRefused() {
        assertThat(CommitMessage.INSTANCE.failures("M13.19 Require the cost line\n\nWhy.\n",
                BACKLOG)).singleElement().asString().contains("Cost:");
    }

    @Test
    void onlyALineThatStartsWithCostAndSaysSomethingCounts() {
        for (String body : List.of(
                "Cost:\n", // says nothing
                "Cost:   \n", // nor this
                "The Cost: is none.\n", // not at the line's start
                "  Cost: none\n", // indented
                "cost: none\n", // another word
                "Costs: none\n")) {
            assertThat(CommitMessage.INSTANCE.failures("M13.19 Subject\n\n" + body, BACKLOG))
                    .as(body.strip()).singleElement().asString().contains("Cost:");
        }
    }

    /** A subject that is itself a cost line is still no body stating one. */
    @Test
    void aCostLineInTheSubjectIsNotTheBodys() {
        assertThat(CommitMessage.INSTANCE.failures("Cost: none\n\nWhy.\n", BACKLOG))
                .hasSize(2)
                .anyMatch(failure -> failure.contains("task ID present in the backlog"))
                .anyMatch(failure -> failure.contains("commit body must state its cost"));
    }

    @Test
    void theSubjectStillNeedsATaskIdTheBacklogNames() {
        assertThat(CommitMessage.INSTANCE.failures("M99.1 Unknown\n\nCost: none\n", BACKLOG))
                .singleElement().asString().contains("task ID present in the backlog");
        assertThat(CommitMessage.INSTANCE.failures("Tidy things\n\nCost: none\n", BACKLOG))
                .singleElement().asString().contains("task ID present in the backlog");
    }

    @Test
    void mergesRevertsAndFixupsAreExempt() {
        for (String subject : List.of("Merge branch 'x'", "Revert \"M13.19 x\"", "fixup! M13.19")) {
            assertThat(CommitMessage.INSTANCE.failures(subject + "\n\nno cost here\n", BACKLOG))
                    .as(subject).isEmpty();
        }
    }

    /** ⚠️ AS THE GATE RUNS IT: the task, over a scratch repository, refuses the message. */
    @Test
    void theCommitMessageTaskRefusesAMessageWithNoCostLine() throws Exception {
        Path repo = repository().resolve("buildSrc/build/tmp/commit-message")
                .resolve("m-" + java.util.UUID.randomUUID());
        try {
            for (String read : List.of(".pre-commit-config.yaml", "AGENTS.md")) {
                write(repo, read, "");
            }
            write(repo, "settings.gradle.kts", "rootProject.name = \"scratch\"\n");
            write(repo, "docs/internal/product/backlog.md", BACKLOG);
            write(repo, "msg.txt", "M13.19 Require the cost line\n\nWhy.\n");
            git(repo, "init", "-q");
            git(repo, "add", "-A");
            RepositoryGatesTask task = ProjectBuilder.builder().withProjectDir(repo.toFile())
                    .build().getTasks().create("checkCommitMessage", RepositoryGatesTask.class);
            task.getRepository().set(repo.toFile());
            task.getCommitMessageFile().set("msg.txt");

            assertThatThrownBy(task::verify).isInstanceOf(GradleException.class)
                    .hasMessageContaining("commit body must state its cost");

            // ⚠️ AND A GOOD ONE IS NOT REFUSED (review T2): the scratch tree fails
            // other gates, but none about the message -- so the task reads the
            // real backlog, which a gate handed "" would not.
            write(repo, "msg.txt", "M13.19 Require the cost line\n\nWhy.\n\nCost: none.\n");
            assertThatThrownBy(task::verify).isInstanceOf(GradleException.class)
                    .satisfies(failed -> assertThat(failed.getMessage())
                            .doesNotContain("commit subject").doesNotContain("commit body"));
        } finally {
            delete(repo);
        }
    }

    private static void write(Path root, String relative, String text) throws Exception {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, text);
    }

    private static void git(Path repo, String... args) throws Exception {
        List<String> command = new java.util.ArrayList<>(List.of("git"));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).directory(repo.toFile())
                .redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        assertThat(process.waitFor()).as("git " + String.join(" ", args) + ": " + output)
                .isZero();
    }

    private static void delete(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                path.toFile().setWritable(true); // git's object files are read-only on Windows
                Files.delete(path);
            }
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
}
