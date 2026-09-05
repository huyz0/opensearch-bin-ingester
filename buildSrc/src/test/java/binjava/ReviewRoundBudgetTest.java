// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The packet must tell a reviewer which round it really is, and must refuse to
 * open another one past the budget.
 *
 * <p>⚠️ THE PACKET SAID "round 1 of 2" ON EVERY ROUND, for the life of the gate.
 * {@code review_rounds.py} resolves the task from a verdict recorded FOR THE
 * HASH IT IS GIVEN, and {@code review.sh context} runs before any verdict for
 * the new hash exists — generating the packet is how one gets made. So it
 * resolved nothing, printed 0, and every reviewer was told it was round one.
 * That number is the one thing deciding whether rule 12 is about to bite, so no
 * reviewer ever applied its split remedy: two tasks in one session reached ten
 * and five rounds, and one of them spent its last four finding nothing but false
 * sentences in the prose describing its own review.
 *
 * <p>⚠️ The commit gate was never affected and is not changed: at commit time
 * the staged hash does carry verdicts, so the task resolves and the count is
 * right. The defect lived only in the document the reviewer reads.
 */
class ReviewRoundBudgetTest {

  private static final String TASK = "M9.7";

  /** A scratch repository with the scripts the packet needs, and {@code n} rounds recorded. */
  private Path scratch(Path dir, int rounds) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("review.sh", "lib.sh", "review_rounds.py", "review_delta.py",
        "which-standards.sh", "review-lenses.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"),
        "| " + TASK + " | a task | — | todo |\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    // Something staged, so the packet has a diff to hash.
    Files.writeString(dir.resolve("scripts/subject.sh"), "# staged\n");
    run(dir, "git add -- scripts/subject.sh");

    Path reviews = dir.resolve(".harness/review");
    Files.createDirectories(reviews);
    for (int i = 0; i < rounds; i++) {
      String sha = String.format("%064x", i + 1);
      for (String role : List.of("reviewer", "test-reviewer")) {
        Files.writeString(reviews.resolve(sha + "." + role + ".json"),
            "{\"diff_sha256\": \"" + sha + "\", \"task\": \"" + TASK + "\","
                + " \"role\": \"" + role + "\", \"verdict\": \"pass\", \"findings\": []}");
      }
    }
    return dir;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = GitEnv.stripped(new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  private String packet(Path dir, int[] rc, String... env) throws Exception {
    ProcessBuilder b = GitEnv.stripped(
        new ProcessBuilder("bash", "scripts/review.sh", "context", "--task", TASK)
            .directory(dir.toFile())).redirectErrorStream(true);
    for (int i = 0; i < env.length; i += 2) {
      b.environment().put(env[i], env[i + 1]);
    }
    Process p = b.start();
    String out = new String(p.getInputStream().readAllBytes());
    rc[0] = p.waitFor();
    return out;
  }

  @Test
  void theRoundCountIsResolvedByTASKNotByAHashThatHasNoVerdictYet(@TempDir Path dir)
      throws Exception {
    scratch(dir, 3);
    // The hash the packet is about to be built for. It is deliberately one that
    // no verdict names -- which is the situation on EVERY round.
    String unreviewed = String.format("%064x", 999);

    Process byTask = GitEnv.stripped(new ProcessBuilder(
        "python3", "scripts/review_rounds.py", "--for-task", TASK)
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String counted = new String(byTask.getInputStream().readAllBytes()).trim();

    Process byHash = GitEnv.stripped(new ProcessBuilder(
        "python3", "scripts/review_rounds.py", unreviewed)
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String viaHash = new String(byHash.getInputStream().readAllBytes()).trim();

    assertThat(counted).as("three distinct hashes were reviewed for this task").isEqualTo("3");
    // ⚠️ THE DEFECT, pinned so it cannot come back by someone "simplifying" the
    // packet to ask by hash again: the hash route answers 0 for the very state
    // in which the packet is always generated.
    assertThat(viaHash)
        .as("asking by an unreviewed hash cannot see the task, which is why the "
            + "packet must not ask that way")
        .isEqualTo("0");
  }

  @Test
  void thePacketNAMESTheRoundItReallyIs(@TempDir Path dir) throws Exception {
    scratch(dir, 2);
    int[] rc = new int[1];
    String out = packet(dir, rc);

    assertThat(out)
        .as("two rounds are on disk, so this is the third:\n" + out)
        .contains("This is round 3 of 2 for " + TASK);
  }

  @Test
  void thePacketREFUSESToOpenAnotherRoundPastTheBudget(@TempDir Path dir) throws Exception {
    // ⚠️ THE CAP REFUSED THE COMMIT AND NOTHING REFUSED THE ROUND, so an author
    // could keep spending pairs of agents indefinitely -- and each round felt
    // justified, because a reviewer verifying by mutation always finds
    // something. What decays is SEVERITY, not yield. This makes continuing a
    // decision instead of the default.
    scratch(dir, 5);
    int[] rc = new int[1];
    String out = packet(dir, rc);

    assertThat(rc[0]).as("round 6 must not be handed out for free:\n" + out).isNotZero();
    assertThat(out).as("and the refusal must name the remedy, not just refuse:\n" + out)
        .contains("exceeds the budget")
        .contains("SPLIT");
  }

  @Test
  void theBudgetIsOVERRIDABLESoItIsAPromptRatherThanAWall(@TempDir Path dir) throws Exception {
    // ⚠️ DELIBERATELY NOT A HARD STOP. Some commits genuinely cannot be split,
    // and a gate that made another round impossible would push authors to
    // `SKIP=check-reviewed`, which disables the substantive checks too -- the
    // exact trade rule 12's argued escape exists to avoid.
    scratch(dir, 5);
    int[] rc = new int[1];
    String out = packet(dir, rc, "REVIEW_ROUND_BUDGET", "99");

    assertThat(out).as("an explicit override proceeds:\n" + out)
        .contains("This is round 6 of 2 for " + TASK)
        .doesNotContain("exceeds the budget");
  }
}
