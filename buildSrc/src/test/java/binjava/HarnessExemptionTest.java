// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * review.md rule 14: a change confined to the harness machinery needs no
 * reviewer verdict, and everything else still does.
 *
 * <p>⚠️ This exemption LOOSENS a gate, which is the direction that normally
 * needs the strongest evidence — so what it may and may not swallow is pinned
 * from both sides here. It exists because reviewing the harness with the harness
 * costs two agent runs per correction: a verdict is bound to the exact staged
 * bytes, so any edit voids it, and two harness tasks in one session took ten and
 * five rounds, four of which found nothing but false sentences in the prose
 * describing the review itself.
 *
 * <p>⚠️ What replaces the reviewer is not nothing: {@code check-tdd} still
 * demands a red record for every new test and {@code check-test-integrity} still
 * refuses a weakened assertion. For gate code those are the stronger signal
 * anyway, since a gate is verified by mutating it and watching a test fail.
 */
class HarnessExemptionTest {

  /** Stage {@code paths} in a scratch repository and return the gate's output. */
  private String gate(Path dir, int[] exitCode, String... paths) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("check-reviewed.sh", "lib.sh", "review-roles.sh", "review_rounds.py")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.writeString(dir.resolve("base.txt"), "base\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    for (String p : paths) {
      Path f = dir.resolve(p);
      Files.createDirectories(f.getParent());
      Files.writeString(f, "x\n");
    }
    run(dir, "git add -A");

    Process p = strip(new ProcessBuilder("bash", "scripts/check-reviewed.sh")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    exitCode[0] = p.waitFor();
    return out;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = strip(new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  private static ProcessBuilder strip(ProcessBuilder b) {
    return GitEnv.stripped(b);
  }

  @Test
  void aChangeConfinedToTheHarnessMachineryNeedsNoVerdict(@TempDir Path dir) throws Exception {
    int[] rc = new int[1];
    String out = gate(dir, rc, "scripts/foo.sh",
        "docs/internal/product/backlog.md", "baselines/review.txt");

    assertThat(rc[0]).as("a harness-only diff must land without verdicts:\n" + out).isZero();
    assertThat(out)
        .as("and must SAY so, because a silent exemption is indistinguishable "
            + "from a gate that stopped working:\n" + out)
        .contains("HARNESS-ONLY");
  }

  @Test
  void oneProductFileTakesTheWHOLEDiffBackThroughReview(@TempDir Path dir) throws Exception {
    // ⚠️ FAIL-CLOSED, and this is the assertion that keeps the exemption
    // honest: the diff still touches `scripts/`, so a rule that asked only
    // "does it touch the harness?" would wave the production file through with
    // it. Every path must be inside the allowlist, not merely one of them.
    int[] rc = new int[1];
    String out = gate(dir, rc,
        "scripts/foo.sh", "sequencer/src/main/java/binjava/sequencer/Foo.java");

    assertThat(rc[0]).as("production code alongside harness code must still be "
        + "reviewed:\n" + out).isNotZero();
    assertThat(out).as("and the reason must name the missing verdict:\n" + out)
        .contains("no 'reviewer' verdict");
  }

  @Test
  void aBuildFileIsPRODUCTIONNotHarness(@TempDir Path dir) throws Exception {
    // ⚠️ `build.gradle.kts` and `settings.gradle.kts` decide what the product
    // compiles to and which modules exist. They sit at the repository root
    // beside AGENTS.md, so a prefix list written by eye collects them.
    int[] rc = new int[1];
    String out = gate(dir, rc, "scripts/foo.sh", "settings.gradle.kts");

    assertThat(rc[0]).as("a build file must not ride in on the exemption:\n" + out).isNotZero();
  }

  @Test
  void aDocsONLYChangeTakesTheORDINARYPathNotTheExemption(@TempDir Path dir) throws Exception {
    // ⚠️ The exemption is for the MACHINERY, not for everything the machinery's
    // allowlist mentions. A docs-only commit is already reduced to one role by
    // `review-roles.sh`; letting it skip review entirely would exempt every
    // backlog and ADR edit in the project, which is most of them.
    int[] rc = new int[1];
    String out = gate(dir, rc, "docs/internal/product/backlog.md");

    assertThat(rc[0]).as("docs alone must not be exempt:\n" + out).isNotZero();
    assertThat(out).as("it takes the ordinary path:\n" + out).contains("verdict");
  }
}
