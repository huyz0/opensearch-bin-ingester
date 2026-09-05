// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What the review packet costs to read, which is what a round costs.
 *
 * <p>⚠️ MEASURED, not asserted: on the staged M4.7 diff the packet was ~140 KB —
 * a 96 KB diff, 21 KB of standards, and 11.6 KB of backlog rows for a task whose
 * own row is ~1 KB. Two roles times 2.6 rounds average (477 verdicts over 92
 * tasks) is most of what a task spends.
 *
 * <p>Three separable defects, one test each:
 *
 * <ul>
 *   <li>{@code grep -F "$TASK"} SUBSTRING-matched the backlog, so a packet for
 *       {@code M4.7} carried {@code M4.7a} and {@code M4.7b} as well.
 *   <li>{@code review_delta.py}'s own docstring promises "the delta since the
 *       last reviewed hash", and review.sh then printed {@code git diff --cached}
 *       in full anyway. The heading said FILES TOUCHED SINCE THE LAST REVIEWED
 *       ROUND above a command that lists every staged file.
 *   <li>The gate suite was re-run on every packet. Both roles build a packet for
 *       the SAME hash, so every round paid for two identical Gradle-bearing gate
 *       runs.
 * </ul>
 *
 * <p>⚠️ The delta is a REDUCTION in what a reviewer is shown, so
 * {@link #aVerifyRoundStillNamesTheCommandForTheWholeDiff} is not decoration: it
 * is the guard that keeps the reduction recoverable. Less review is never the
 * failure-safe default — review_delta.py says so and this pins it.
 */
class ReviewPacketBoundsTest {

  private static final String ROW_ONE = "ROWBODYFORNINEONE";
  private static final String ROW_ONE_A = "ROWBODYFORNINEONEA";

  private void run(Path dir, String script) throws Exception {
    Process p =
        new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** The packet, exactly as a reviewer receives it. */
  private String packet(Path dir, String task) throws Exception {
    Process p =
        new ProcessBuilder("bash", "scripts/review.sh", "context", "--task", task)
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return out;
  }

  /**
   * A scratch repository with review.sh, the two selectors it calls, and a gate
   * suite of exactly one script that COUNTS ITS OWN INVOCATIONS.
   *
   * <p>⚠️ The counter is the only way to observe caching: a cached run and a
   * re-run print the same PASSED line, so asserting on the packet text cannot
   * distinguish them.
   */
  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f :
        List.of("review.sh", "lib.sh", "review_rounds.py", "review_delta.py",
            "which-standards.sh", "review-lenses.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    // The gate loop greps this file for scripts/check-*.sh, and always appends
    // build-index.sh --check. Both are stubs here; check-counter.sh records a
    // line per invocation so the test can count them.
    Files.writeString(dir.resolve(".pre-commit-config.yaml"),
        "  - entry: scripts/check-counter.sh\n");
    Files.writeString(dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\necho ran >> .harness/gate-invocations\n");
    Files.writeString(dir.resolve("scripts/build-index.sh"), "#!/usr/bin/env bash\nexit 0\n");
    for (String f : List.of("check-counter.sh", "build-index.sh")) {
      dir.resolve("scripts").resolve(f).toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve(".harness"));

    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.createDirectories(dir.resolve("docs/internal/standards"));
    for (String s : List.of("git.md", "sdd.md")) {
      Files.writeString(dir.resolve("docs/internal/standards").resolve(s), "x\n");
    }
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"),
        "| ID | Task | Serves | State |\n"
            + "|---|---|---|---|\n"
            + "| M9.1 | " + ROW_ONE + " | — | todo |\n"
            + "| M9.1a | " + ROW_ONE_A + " | — | todo |\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    return dir;
  }

  /** Stage two files whose bodies are distinguishable in a diff. */
  private void stage(Path dir, String keptBody, String changedBody) throws Exception {
    Files.writeString(dir.resolve("kept.txt"), keptBody + "\n");
    Files.writeString(dir.resolve("changed.txt"), changedBody + "\n");
    run(dir, "git add -A -- kept.txt changed.txt");
  }

  /** Record a pass verdict for whatever is staged now, as a completed round. */
  private void recordRound(Path dir, String task) throws Exception {
    Files.createDirectories(dir.resolve(".harness/review"));
    Files.writeString(dir.resolve("v.json"),
        "{\"diff_sha256\": \"__SHA__\", \"verdict\": \"pass\", \"findings\": []}");
    run(dir, "sha=$(git diff --cached | sha256sum | cut -d' ' -f1);"
        + " sed -i \"s/__SHA__/$sha/\" v.json;"
        + " bash scripts/review.sh record --file v.json --task " + task + " --role reviewer");
  }

  private int gateRuns(Path dir) throws Exception {
    Path f = dir.resolve(".harness/gate-invocations");
    return Files.exists(f) ? Files.readAllLines(f).size() : 0;
  }

  /**
   * The task's row is selected by its ID COLUMN, not by substring.
   *
   * <p>⚠️ Measured: {@code grep -F M4.7} returned 11,620 bytes because it also
   * matched M4.7a and M4.7b. The reviewer then reads three tasks' acceptance
   * criteria and cannot tell which one binds the diff in front of it.
   */
  @Test
  void theBacklogRowIsSelectedByItsIdColumnNotBySubstring(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);
    stage(d, "kept", "changed");
    String p = packet(d, "M9.1");
    assertThat(p).as("the task's own row must be in the packet\n%s", p).contains(ROW_ONE);
    assertThat(p)
        .as("M9.1a is a DIFFERENT task; carrying its row makes the packet ambiguous\n%s", p)
        .doesNotContain(ROW_ONE_A);
  }

  /**
   * Round one carries the whole diff; a verify round carries only what changed
   * since the last reviewed hash.
   *
   * <p>⚠️ ONE test, because the round-one arm is the NEGATIVE CONTROL for the
   * other: a delta that also applied to round one would hide a change nobody had
   * ever reviewed. Split into two tests the control could never be observed red,
   * and a test never seen red is not known to test anything.
   */
  @Test
  void roundOneCarriesTheWholeDiffAndAVerifyRoundCarriesOnlyTheDelta(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir);
    stage(d, "KEPTBODY", "CHANGEDBODY");
    String one = packet(d, "M9.1");
    assertThat(one)
        .as("round one has nothing already reviewed, so withholding any of it hides "
            + "bytes no verdict covers\n%s", one)
        .contains("KEPTBODY").contains("CHANGEDBODY");

    recordRound(d, "M9.1");
    stage(d, "KEPTBODY", "FIXEDBODY");
    String two = packet(d, "M9.1");
    assertThat(two).as("the fix is the whole point of the round\n%s", two).contains("FIXEDBODY");
    assertThat(two)
        .as("kept.txt was in the reviewed diff and carries a verdict; re-reading it is "
            + "what made review cost more than the work\n%s", two)
        .doesNotContain("KEPTBODY");
  }

  /**
   * ⚠️ The reduction must stay RECOVERABLE, and legibly so: the packet says in
   * as many words that the whole diff is still available, and names the command.
   *
   * <p>An incidental mention of {@code git diff --cached} somewhere in the
   * preamble would satisfy a looser assertion while telling a reviewer nothing,
   * so the heading is what is asserted.
   */
  @Test
  void aVerifyRoundNamesTheEscapeToTheWholeDiff(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);
    stage(d, "KEPTBODY", "CHANGEDBODY");
    recordRound(d, "M9.1");
    stage(d, "KEPTBODY", "FIXEDBODY");
    String p = packet(d, "M9.1");
    assertThat(p).as("%s", p)
        .contains("THE WHOLE DIFF IS STILL AVAILABLE")
        .contains("git diff --cached");
  }

  /**
   * The gate suite runs once per staged hash, and again when the bytes change.
   *
   * <p>⚠️ Both roles build a packet for the SAME hash, so every round paid for
   * two identical gate runs — one of them carrying Gradle through
   * {@code check-module.sh}. Keyed on the staged hash, so this is a cache and not
   * a skip: the gates are a function of the bytes they judged.
   *
   * <p>⚠️ The third arm is the NEGATIVE CONTROL and is in the same test for the
   * same reason as above — a cache that survives the bytes changing is not a
   * cache, it is a gate that stopped running, and that arm is green before the
   * cache exists so it can only be observed red beside the arm that demands one.
   */
  @Test
  void theGateSuiteRunsOncePerStagedHashAndAgainWhenTheBytesChange(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir);
    stage(d, "KEPTBODY", "CHANGEDBODY");
    packet(d, "M9.1");
    assertThat(gateRuns(d)).as("the first packet must actually run the gates").isEqualTo(1);
    packet(d, "M9.1");
    assertThat(gateRuns(d))
        .as("the second role's packet judges identical bytes; re-running is pure waste")
        .isEqualTo(1);
    stage(d, "KEPTBODY", "FIXEDBODY");
    packet(d, "M9.1");
    assertThat(gateRuns(d))
        .as("different bytes are a different question, and the cache must not answer it")
        .isEqualTo(2);
  }
}
