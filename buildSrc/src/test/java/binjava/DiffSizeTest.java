// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A commit that is too large to review in a couple of rounds must SAY SO
 * (M0.81).
 *
 * <p>The evidence is this repository's own history: the three largest commits
 * to module SOURCE trees are also the ones that consumed the most review rounds --
 * M4.7 at 2,039 added lines, M4.10c at 1,423 over six rounds, M4.8b2 at 1,385.
 *
 * <p>⚠️ THE CORRELATION IS NOT CLEAN, and the gate says so rather than
 * pretending: M0.76 exceeded its round budget at 243 added lines, so size is
 * one driver and not the driver. That is why the cap is set where only the
 * clearly-large land above it, and why the escape is an argument rather than a
 * denial.
 *
 * <p>⚠️ IT GATES THE COMMIT MESSAGE STAGE, because the argument is keyed by
 * task id and the task id lives in the subject -- the same place
 * {@code check-commit-msg} and {@code check-test-integrity} read it from.
 */
class DiffSizeTest {

  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-diff-size.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("baselines"));
    Files.writeString(dir.resolve("baselines").resolve("review.txt"), "# arguments\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    return dir;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
        .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** Stages {@code lines} added lines of production source. */
  private void stageLines(Path repo, int lines) throws Exception {
    Path f = repo.resolve("format/src/main/java/binjava/format/Big.java");
    Files.createDirectories(f.getParent());
    StringBuilder b = new StringBuilder("class Big {\n");
    for (int i = 0; i < lines; i++) {
      b.append("  int f").append(i).append("() { return ").append(i).append("; }\n");
    }
    b.append("}\n");
    Files.writeString(f, b.toString());
    run(repo, "git add -A");
  }

  private String gate(Path repo, String subject) throws Exception {
    Files.writeString(repo.resolve("MSG"), subject + "\n");
    ProcessBuilder pb = new ProcessBuilder("bash", "scripts/check-diff-size.sh", "MSG")
        .directory(repo.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  /** An ordinary commit passes without ceremony. */
  @Test
  void aSmallDiffPASSES(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 50);

    assertThat(gate(repo, "M9.1: something small")).startsWith("0");
  }

  /** ⚠️ Over the cap, with no argument staged, is a REFUSAL. */
  @Test
  void aLargeDiffWithNoArgumentFAILS(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 1200);

    String out = gate(repo, "M9.1: something enormous");

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).containsIgnoringCase("split");
  }

  /** An argued cap passes -- the point is that the size was DECIDED. */
  @Test
  void aLargeDiffWithAStagedArgumentPASSES(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 1200);
    Files.writeString(repo.resolve("baselines/review.txt"),
        "# arguments\nsize:M9.1  One wire format, and non-negotiable 8 forbids splitting it.\n");
    run(repo, "git add -A");

    assertThat(gate(repo, "M9.1: something enormous")).startsWith("0");
  }

  /**
   * ⚠️ THE ARGUMENT MUST BE STAGED. An unstaged one suppresses the gate while
   * leaving no trace in the diff anyone reviews -- the hole M4.26 records for
   * the sibling mechanism in this same file.
   */
  @Test
  void anUNSTAGEDArgumentDoesNotCount(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 1200);
    Files.writeString(repo.resolve("baselines/review.txt"),
        "# arguments\nsize:M9.1  Written but never staged.\n");

    assertThat(gate(repo, "M9.1: something enormous")).startsWith("1");
  }

  /** ⚠️ And it must argue THIS task, not some other one. */
  @Test
  void anArgumentForADIFFERENTTaskDoesNotCount(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 1200);
    Files.writeString(repo.resolve("baselines/review.txt"),
        "# arguments\nsize:M9.2  A reason belonging to another row.\n");
    run(repo, "git add -A");

    assertThat(gate(repo, "M9.1: something enormous")).startsWith("1");
  }

  /**
   * ⚠️ AND IT MUST CARRY A REASON. M4.26 measured the sibling key accepting
   * `rounds:M9.1<TAB>` -- a trailing tab splits into a second, EMPTY field, so
   * `NF > 1` argued the cap while justifying nothing.
   */
  @Test
  void anArgumentWithNoREASONDoesNotCount(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    stageLines(repo, 1200);
    Files.writeString(repo.resolve("baselines/review.txt"), "# arguments\nsize:M9.1\t\n");
    run(repo, "git add -A");

    assertThat(gate(repo, "M9.1: something enormous")).startsWith("1");
  }

  /**
   * ⚠️ DOCS DO NOT COUNT. A backlog row or an ADR is read, not reviewed for
   * test weakness, and counting prose would make the gate fire on exactly the
   * commits that are cheapest to review.
   */
  @Test
  void aHugeDOCSONLYDiffPASSES(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      b.append("a line of prose ").append(i).append('\n');
    }
    Files.writeString(repo.resolve("docs.md"), b.toString());
    run(repo, "git add -A");

    assertThat(gate(repo, "M9.1: a great deal of documentation")).startsWith("0");
  }
}
