// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The round cap, and the escape its own message promises.
 *
 * <p>⚠️ THE GATE PRINTED A REMEDY IT DID NOT IMPLEMENT. On exceeding rule 12's
 * cap it says "SPLIT it, or argue the finding with a staged
 * {@code baselines/review.txt} entry" and then returns before the loop that
 * reads that file, so the second half was never true for the round cap — the
 * baseline could only ever silence an individual blocking or major finding.
 *
 * <p>⚠️ WHY THAT MATTERS MORE THAN A WRONG SENTENCE: an author whose commit
 * genuinely cannot be split — a four-line javadoc, or a checker needing one
 * fixture per arm — has no route left but {@code SKIP=check-reviewed}, and a
 * SKIP disables the gate's SUBSTANTIVE checks too: both roles present, and no
 * unresolved blocking or major finding. So the missing escape does not make the
 * cap stricter, it makes the whole gate absent exactly when the cap bites.
 * Found with {@code git log --grep=SKIP=check-reviewed}: AT LEAST NINE commits in
 * M4 landed that way, seven of them before the session that wrote this class — at
 * least, because that grep finds commits that DISCLOSED a skip rather than commits
 * that took one, and M4.12 bypassed a 12-round cap naming neither. A
 * first draft said "three", counted from the commit bodies that numbered
 * themselves rather than from the log. (Only two of the nine carry an ordinal at
 * all — a draft of THIS sentence said three did, and said so in the same commit
 * that corrected the count in review.md.)
 *
 * <p>⚠️ SO AN ARGUED EXCEPTION IS STRICTER THAN A SKIP, which is the reasoning
 * behind making the promise real rather than deleting it: the round cap can be
 * argued, in the tree, greppable, and reviewed like any other staged change —
 * while both-roles-present and no-unresolved-majors STAY ENFORCED.
 */
class ReviewRoundCapTest {

  private void run(Path dir, String script) throws Exception {
    Process p =
        new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** Runs the gate and returns its exit status paired with everything it printed. */
  private Gate gate(Path dir) throws Exception {
    Process p =
        new ProcessBuilder("bash", "scripts/check-reviewed.sh")
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Gate(p.waitFor(), out);
  }

  private record Gate(int status, String output) {
  }

  /**
   * A scratch repository with the gate, one staged file, and {@code rounds}
   * distinct verdicts already recorded for the same task.
   *
   * <p>⚠️ The verdicts are written for OTHER hashes as well as this one, because
   * that is what a round IS — a distinct staged hash reviewed for one task.
   */
  private Path scratch(Path dir, int rounds, String baseline) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-reviewed.sh", "review_rounds.py", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"), "| M9.1 | x | — | todo |\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");

    // ⚠️ ROLE DERIVATION NEVER RUNS in this scratch: `review-roles.sh` is not
    // copied, so the gate takes its both-roles FALLBACK and requires BOTH verdicts
    // regardless of what the diff contains. Two earlier comments here claimed the
    // opposite -- that a docs-only diff summons one role -- and neither was
    // operative. Verdicts are written for both roles below for that reason.
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"),
        "| M9.1 | x | — | **done** |\n");
    run(dir, "git add -A -- docs");

    // ⚠️ THE BASELINE IS STAGED FIRST, then the hash is taken. Computing it
    // before staging gave a hash the gate never sees -- the fixture's own version
    // of the defect this milestone keeps finding, and it failed loudly rather
    // than passing vacuously, which is why it was caught in one run.
    if (baseline != null) {
      Files.createDirectories(dir.resolve("baselines"));
      Files.writeString(dir.resolve("baselines/review.txt"), baseline);
      run(dir, "git add -A -- baselines");
    }

    Path reviews = dir.resolve(".harness/review");
    Files.createDirectories(reviews);
    String sha = out(dir, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    // ⚠️ BOTH ROLES, for every round, because the fallback above demands both.
    // This test is about the ROUND COUNT, so a missing role would fail it for the
    // wrong reason -- which is exactly what happened before.
    for (int i = 0; i < rounds - 1; i++) {
      String other = "deadbeef%02d".formatted(i);
      for (String role : List.of("reviewer", "test-reviewer")) {
        Files.writeString(reviews.resolve(other + "." + role + ".json"), verdict("M9.1", other));
      }
    }
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"), verdict("M9.1", sha));
    }
    return dir;
  }

  private static String verdict(String task, String sha) {
    return """
        {"task": "%s", "diff_sha256": "%s", "verdict": "pass", "findings": []}
        """.formatted(task, sha);
  }

  private String out(Path dir, String script) throws Exception {
    Process p =
        new ProcessBuilder("bash", "-c", script).directory(dir.toFile()).start();
    String s = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return s;
  }

  @Test
  void withinTheCapTheGatePasses(@TempDir Path dir) throws Exception {
    Gate g = gate(scratch(dir, 2, null));
    assertThat(g.status()).as("two rounds is the cap, not one over it\n%s", g.output()).isZero();
  }

  @Test
  void overTheCapWithNoArgumentTheGateREFUSES(@TempDir Path dir) throws Exception {
    Gate g = gate(scratch(dir, 3, null));
    assertThat(g.status())
        .as("three rounds must still refuse -- an escape that fires without being asked for "
            + "is not an escape, it is the cap removed\n%s", g.output())
        .isNotZero();
    assertThat(g.output()).contains("exceeds");
  }

  @Test
  void overTheCapWithASTAGEDArgumentTheGatePasses(@TempDir Path dir) throws Exception {
    // ⚠️ THE KEY NAMES THE TASK AND THE COUNT, so the exception is greppable and
    // cannot be a bare word that silently covers a future task as well.
    Gate g = gate(scratch(dir, 3, "rounds:M9.1  argued because the commit cannot be split\n"));
    assertThat(g.status())
        .as("an argued round count lands, and -- the point of doing it this way rather than "
            + "skipping -- the both-roles and no-unresolved-majors checks stay live\n%s",
            g.output())
        .isZero();
  }

  @Test
  void anArgumentForADIFFERENTTaskDoesNOTLiftTheCap(@TempDir Path dir) throws Exception {
    Gate g = gate(scratch(dir, 3, "rounds:M9.2  a different task entirely\n"));
    assertThat(g.status())
        .as("an argument staged for another task must not silence this one -- the same defect "
            + "the file's own header records for bare finding ids\n%s", g.output())
        .isNotZero();
  }

  /**
   * The key must be the FIRST FIELD of the line, not merely present in it.
   *
   * <p>⚠️ MEASURED BY REVIEW: widening {@code f[1] == k} to a substring test
   * ({@code index(line, k) > 0}) left all fourteen other tests green, because
   * every fixture that stages the key stages it in first position and every
   * fixture that does not stages no {@code rounds:} text on the line at all. So
   * nothing distinguished "the key IS the first field" from "the key APPEARS
   * somewhere in the line" — and an entirely ordinary, legitimate
   * finding-argument line that happens to mention another task's key in prose,
   * such as {@code reviewer:R7  already fixed elsewhere, see rounds:M9.1 in the
   * parent commit}, would silently argue the round cap for M9.1 under that
   * regression, on a line that was never staged to argue anything.
   */
  @Test
  void aROUNDSKeyThatAppearsMIDLINERatherThanAsTheFIRSTFieldDoesNOTLiftTheCap(
      @TempDir Path dir) throws Exception {
    Gate g = gate(scratch(dir, 3,
        "reviewer:R7  already fixed elsewhere, see rounds:M9.1 in the parent commit\n"));
    assertThat(g.status())
        .as("the key must be the line's first field, not merely present in the line\n%s",
            g.output())
        .isNotZero();
  }

  @Test
  void anUNSTAGEDArgumentDoesNOTLiftTheCap(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, 3, null);
    Files.createDirectories(d.resolve("baselines"));
    // ⚠️ WRITTEN BUT NOT STAGED. review.md rule 9: an unstaged entry suppresses a
    // finding while leaving no trace of it, which is the whole reason the gate
    // reads the INDEX rather than the working tree.
    Files.writeString(d.resolve("baselines/review.txt"),
        "rounds:M9.1  argued but never staged\n");
    Gate g = gate(d);
    assertThat(g.status())
        .as("an unstaged argument is not an argument\n%s", g.output())
        .isNotZero();
  }

  /**
   * The distinguishing case for the INDEX read, which the unstaged test does not reach.
   *
   * <p>⚠️ {@code anUNSTAGEDArgumentDoesNOTLiftTheCap} has the file absent from the
   * index entirely, so the diff of the index is empty and any read at all refuses.
   * Here the file IS staged, carrying a legitimate finding-argument line, while the
   * {@code rounds:} line exists only in the WORKTREE. A gate reading the worktree
   * would lift the cap; one reading the staged diff refuses — review.md rule 9.
   */
  @Test
  void aROUNDSLineOnlyInTheWORKTREEOfASTAGEDFileDoesNOTLiftTheCap(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir, 3, "reviewer:R1  an unrelated finding argued legitimately\n");
    Files.writeString(d.resolve("baselines/review.txt"),
        "reviewer:R1  an unrelated finding argued legitimately\n"
            + "rounds:M9.1  added to the worktree only, never staged\n");
    Gate g = gate(d);
    assertThat(g.status())
        .as("the gate reads the INDEX, so a rounds: line present only on disk argues "
            + "nothing -- review.md rule 9, and the reason the staged-name guard alone is "
            + "not enough\n%s", g.output())
        .isNotZero();
  }

  /**
   * The property the whole design rests on: an ARGUED cap still blocks a major.
   *
   * <p>⚠️ THIS WAS PROSE ONLY. The other tests stage {@code findings: []}, so they
   * cannot tell the unresolved-majors loop from its absence — review measured that
   * inserting a {@code finish} after the {@code warn} left all five green, making
   * the argued path byte-for-byte the {@code SKIP} it is documented as being
   * stricter than. If that is the justification for arguing rather than skipping,
   * it has to be the thing a test holds.
   */
  @Test
  void anARGUEDCapStillBLOCKSAnUnresolvedMajor(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, 3, "rounds:M9.1  argued because the commit cannot be split\n");
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"),
          """
          {"task": "M9.1", "diff_sha256": "%s", "verdict": "changes-requested",
           "findings": [{"id": "R1", "severity": "major", "finding": "x"}]}
          """.formatted(sha));
    }
    Gate g = gate(d);
    assertThat(g.status())
        .as("the cap is waived and the MAJOR still refuses -- this is the entire reason an "
            + "argued exception is stricter than a skip, which disables both checks\n%s",
            g.output())
        .isNotZero();
    assertThat(g.output()).contains("blocking/major");
  }

  @Test
  void aROUNDSKeyWithNOREASONDoesNOTLiftTheCap(@TempDir Path dir) throws Exception {
    // ⚠️ THE KEY IS NOT THE ARGUMENT. An earlier match required only whitespace
    // after the key, so `rounds:M9.1<TAB>` argued the cap while saying why it
    // could not be split precisely nowhere -- and rule 12's escape is worth having
    // only if the reason lands in the tree with it.
    Gate g = gate(scratch(dir, 3, "rounds:M9.1\t\n"));
    assertThat(g.status())
        .as("a bare key is not a justification\n%s", g.output())
        .isNotZero();
  }

  @Test
  void aTASKNameIsMatchedEXACTLYAndNotAsAPattern(@TempDir Path dir) throws Exception {
    // ⚠️ `$TASK` USED TO GO INTO A BRE UNESCAPED, and every task id in this
    // repository contains a `.`. Measured against task M9.1: both `rounds:M9x1`
    // and `rounds:M9-1` lifted the cap, because `.` matched any character. And
    // `review.sh record` does not validate `--task`, so `--task '.*'` would have
    // let any `rounds:` line argue anything at all.
    Gate g = gate(scratch(dir, 3, "rounds:M9x1  a near miss that must not count\n"));
    assertThat(g.status())
        .as("the dot in a task id is a dot, not a wildcard\n%s", g.output())
        .isNotZero();
  }

}
