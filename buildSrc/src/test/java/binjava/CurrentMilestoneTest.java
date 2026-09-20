// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The session input must be limited to open rows in the declared milestone. */
class CurrentMilestoneTest {

  private Path scratch(String backlog) throws Exception {
    Path dir = Files.createTempDirectory(Path.of("build/tmp"), "current-milestone-");
    Files.createDirectories(dir.resolve("scripts"));
    Path root = Path.of("..").toAbsolutePath().normalize();
    for (String file : List.of("current-milestone.sh", "lib.sh")) {
      Path destination = dir.resolve("scripts").resolve(file);
      Files.copy(root.resolve("scripts").resolve(file), destination);
      destination.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"), backlog);
    return dir;
  }

  private Run run(Path dir) throws Exception {
    Process process =
        new ProcessBuilder("bash", "scripts/current-milestone.sh")
            .directory(dir.toFile())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes());
    return new Run(process.waitFor(), output);
  }

  @Test
  void selectsOnlyOpenRowsInTheDeclaredMilestone() throws Exception {
    Run result =
        run(
            scratch(
                "**Current milestone: M9 — current**\n"
                    + "\n"
                    + "| ID | Task | Serves | State |\n"
                    + "|---|---|---|---|\n"
                    + "| M9.1 | open | — | todo |\n"
                    + "| M9.2 | closed | — | done |\n"
                    + "**M8 — historical**\n"
                    + "| M8.1 | old | — | todo |\n"));

    assertThat(result.exit()).isZero();
    assertThat(result.output()).contains("| M9.1 | open | — | todo |");
    assertThat(result.output()).doesNotContain("M9.2", "M8.99", "M8.1");
  }

  @Test
  void rejectsASectionThatContainsAnotherMilestone() throws Exception {
    Run result =
        run(
            scratch(
                "**Current milestone: M9 — current**\n"
                    + "| ID | Task | Serves | State |\n"
                    + "|---|---|---|---|\n"
                    + "| M8.99 | misplaced | — | todo |\n"));

    assertThat(result.exit()).isNotZero();
    assertThat(result.output()).contains("outside current milestone");
  }

  @Test
  void rejectsAMissingCurrentMilestone() throws Exception {
    Run result = run(scratch("| M9.1 | open | — | todo |\n"));

    assertThat(result.exit()).isNotZero();
    assertThat(result.output()).contains("current milestone declaration is missing");
  }

  @Test
  void rejectsACurrentMilestoneDeclarationWithoutAnId() throws Exception {
    Run result = run(scratch("**Current milestone: unknown**\n"));

    assertThat(result.exit()).isNotZero();
    assertThat(result.output()).contains("current milestone declaration has no milestone ID");
  }

  private record Run(int exit, String output) {}
}
