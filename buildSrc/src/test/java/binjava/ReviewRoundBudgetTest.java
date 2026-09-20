// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Regression tests for the review budget being enforced before reviewer work starts. */
class ReviewRoundBudgetTest {

  private static final String TASK = "M0.50";

  private Path scratch(Path dir, int rounds) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String file :
        List.of(
            "review.sh",
            "lib.sh",
            "review_rounds.py",
            "review_delta.py",
            "which-standards.sh",
            "review-lenses.sh",
            "check-reviewed.sh",
            "review-roles.sh")) {
      Path destination = dir.resolve("scripts").resolve(file);
      Files.copy(repo.resolve("scripts").resolve(file), destination);
      destination.toFile().setExecutable(true);
    }
    Path buildIndex = dir.resolve("scripts/build-index.sh");
    Files.writeString(buildIndex, "#!/usr/bin/env bash\nexit 0\n");
    buildIndex.toFile().setExecutable(true);
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(
        dir.resolve("docs/internal/product/backlog.md"),
        "| " + TASK + " | review budget | — | todo |\n");
    Files.writeString(dir.resolve(".pre-commit-config.yaml"), "repos: []\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t");
    run(dir, "git add -A && git commit -qm base");
    Files.writeString(dir.resolve("scripts/subject.sh"), "# staged\n");
    run(dir, "git add -- scripts/subject.sh");

    Path reviews = dir.resolve(".harness/review");
    Files.createDirectories(reviews);
    for (int round = 0; round < rounds; round++) {
      String sha = String.format("%064x", round + 1);
      for (String role : List.of("reviewer", "test-reviewer")) {
        Files.writeString(
            reviews.resolve(sha + "." + role + ".json"),
            "{\"diff_sha256\": \""
                + sha
                + "\", \"task\": \""
                + TASK
                + "\", \"role\": \""
                + role
                + "\", \"verdict\": \"pass\", \"findings\": []}");
      }
      String otherSha = String.format("%064x", rounds + round + 1);
      Files.writeString(
          reviews.resolve(otherSha + ".reviewer.json"),
          "{\"diff_sha256\": \""
              + otherSha
              + "\", \"task\": \"M0.999\", \"role\": \"reviewer\","
              + " \"verdict\": \"pass\", \"findings\": []}");
    }
    return dir;
  }

  private void run(Path dir, String command) throws Exception {
    commandOutput(dir, command);
  }

  private String commandOutput(Path dir, String command) throws Exception {
    Process process =
        stripped(
                new ProcessBuilder("bash", "-c", "set -o pipefail; " + command)
                    .directory(dir.toFile()))
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes());
    assertThat(process.waitFor()).as(command + "\n" + output).isZero();
    return output;
  }

  private String packet(Path dir, int[] exitCode, String... environment) throws Exception {
    ProcessBuilder builder =
        stripped(
                new ProcessBuilder("bash", "scripts/review.sh", "context", "--task", TASK)
                    .directory(dir.toFile()))
            .redirectErrorStream(true);
    for (int i = 0; i < environment.length; i += 2) {
      builder.environment().put(environment[i], environment[i + 1]);
    }
    Process process = builder.start();
    String output = new String(process.getInputStream().readAllBytes());
    exitCode[0] = process.waitFor();
    return output;
  }

  private ProcessBuilder stripped(ProcessBuilder builder) {
    builder.environment().keySet().removeIf(key -> key.startsWith("GIT_"));
    builder.environment().remove("GATE_SCOPE");
    builder.environment().remove("CHECK_RANGE");
    return builder;
  }

  @Test
  void thePacketCountsRoundsByTaskBeforeTheNewHashHasAVerdict(@TempDir Path dir)
      throws Exception {
    scratch(dir, 2);
    int[] exitCode = new int[1];

    String output = packet(dir, exitCode);

    assertThat(exitCode[0]).isZero();
    assertThat(output).contains("This is round 3 of 3 for " + TASK);
  }

  @Test
  void thePacketRefusesToOpenAnotherRoundPastTheBudget(@TempDir Path dir) throws Exception {
    scratch(dir, 3);
    int[] exitCode = new int[1];

    String output = packet(dir, exitCode);

    assertThat(exitCode[0]).isNotZero();
    assertThat(output).contains("exceeds the budget").contains("SPLIT");
    assertThat(output).doesNotContain("=== TASK");
    assertThat(output).doesNotContain("=== STANDARDS");
    assertThat(output).doesNotContain("=== WHAT TO LOOK AT");
  }

  @Test
  void aRoundCounterFailureRefusesToOpenAPacket(@TempDir Path dir) throws Exception {
    scratch(dir, 0);
    Files.writeString(dir.resolve("scripts/review_rounds.py"), "raise SystemExit(1)\n");
    int[] exitCode = new int[1];

    String output = packet(dir, exitCode);

    assertThat(exitCode[0]).isNotZero();
    assertThat(output).contains("Could not determine completed review rounds");
    assertThat(output).doesNotContain("=== TASK");
  }

  @Test
  void aMalformedSuccessfulRoundCounterRefusesToOpenAPacket(@TempDir Path dir) throws Exception {
    scratch(dir, 0);
    Files.writeString(dir.resolve("scripts/review_rounds.py"), "print('not-a-number')\n");
    int[] exitCode = new int[1];

    String output = packet(dir, exitCode);

    assertThat(exitCode[0]).isNotZero();
    assertThat(output).contains("non-negative integer");
    assertThat(output).doesNotContain("=== TASK");
  }

  @Test
  void theBudgetCanBeOverriddenForAnUnsplittableChange(@TempDir Path dir) throws Exception {
    scratch(dir, 3);
    int[] exitCode = new int[1];

    String output = packet(dir, exitCode, "REVIEW_ROUND_BUDGET", "4");

    assertThat(exitCode[0]).isZero();
    assertThat(output).contains("This is round 4 of 4 for " + TASK);
    assertThat(output).doesNotContain("exceeds the budget");

    String sha = commandOutput(dir, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    Path reviews = dir.resolve(".harness/review");
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(
          reviews.resolve(sha + "." + role + ".json"),
          "{\"diff_sha256\": \""
              + sha
              + "\", \"task\": \""
              + TASK
              + "\", \"role\": \""
              + role
              + "\", \"verdict\": \"pass\", \"findings\": []}");
    }
    ProcessBuilder defaultGateBuilder =
        stripped(
            new ProcessBuilder("bash", "scripts/check-reviewed.sh")
                .directory(dir.toFile())
                .redirectErrorStream(true));
    Process defaultGate = defaultGateBuilder.start();
    String defaultGateOutput = new String(defaultGate.getInputStream().readAllBytes());
    assertThat(defaultGate.waitFor()).as(defaultGateOutput).isNotZero();

    ProcessBuilder gateBuilder =
        stripped(
            new ProcessBuilder("bash", "scripts/check-reviewed.sh")
                .directory(dir.toFile())
                .redirectErrorStream(true));
    gateBuilder.environment().put("REVIEW_ROUND_BUDGET", "4");
    Process gate = gateBuilder.start();
    String gateOutput = new String(gate.getInputStream().readAllBytes());
    assertThat(gate.waitFor()).as(gateOutput).isZero();
  }
}
