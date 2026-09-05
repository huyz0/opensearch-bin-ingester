// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Where the {@code rounds:<task>} argument is found and validated -- as
 * opposed to whether the cap fires at all, which is {@link ReviewRoundCapTest}.
 *
 * <p>Split out of that class once it crossed code-structure.md's 500-line
 * limit (M4.21 round 14). These six tests share {@link ReviewRoundCapTest}'s
 * fixture shape and helpers, DUPLICATED here rather than extracted to a
 * shared utility: {@code tdd_scan.py} binds a test's red record to
 * "the file with every test declaration cut out" (M0.56), so touching
 * {@link ReviewRoundCapTest}'s helpers to share them would have re-keyed
 * every one of its ten surviving tests for a file-organization change that
 * altered no test's behaviour. Duplication costs a few dozen lines; refactoring
 * costs re-observing ten tests failing again.
 */
class ReviewRoundCapArgumentValidityTest {

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

  /**
   * WHICH FILE the argument must live in, which nothing pinned.
   *
   * <p>⚠️ MEASURED BY REVIEW, with a working exploit: deleting the pathspec
   * {@code -- baselines/review.txt} from the gate's {@code git diff --cached}
   * left all thirteen other tests GREEN, because every fixture that stages an
   * argument stages it in that file and every fixture that does not stage one
   * has no {@code rounds:} text anywhere. So no test could tell "the added line
   * is in {@code baselines/review.txt}" from "the added line is ANYWHERE in the
   * staged diff" — and the mutated gate printed
   * {@code ARGUED as 'rounds:M9.1' in baselines/review.txt} for an argument
   * staged in {@code backlog.md}: a gate naming a file it never read.
   *
   * <p>⚠️ It matters because the escape is supposed to be greppable in one known
   * place. An argument that counts from any staged file means a source comment,
   * a test fixture or a doc paragraph containing the key waives the cap, and
   * review.md rule two-round-cap states the criterion as a staged {@code baselines/review.txt}.
   */
  @Test
  void anArgumentInADIFFERENTSTAGEDFileDoesNOTLiftTheCap(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, 3, null);
    // ⚠️ NO `baselines/` AT ALL, and the key on its own line in a file that IS
    // staged and IS in the diff -- so only the pathspec can refuse it.
    Files.writeString(d.resolve("docs/internal/product/backlog.md"),
        "| M9.1 | x | — | **done** |\n"
            + "rounds:M9.1  argued in a file that is not baselines/review.txt\n");
    run(d, "git add -A -- docs");
    // ⚠️ RE-RECORD FOR THE NEW HASH. Staging moved it, and without this the gate
    // refuses for a MISSING VERDICT rather than for the cap -- the wrong-reason
    // pass this file's siblings were caught on twice.
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"), verdict("M9.1", sha));
    }

    Gate g = gate(d);
    assertThat(g.status())
        .as("only a staged baselines/review.txt argues the cap, not any staged file\n%s",
            g.output())
        .isNotZero();
    assertThat(g.output())
        .as("and it must not claim it read baselines/review.txt\n%s", g.output())
        .doesNotContain("ARGUED");
  }

  /**
   * Verdicts that name NO TASK refuse, rather than counting as round zero.
   *
   * <p>⚠️ MEASURED BY REVIEW: replacing the guard with {@code if false} left all
   * thirteen other tests green, because no fixture ever wrote an empty task. The
   * exploit is a COMPLETE cap bypass — five recorded rounds whose verdicts carry
   * {@code "task": ""} gave {@code ok ... (round 0 of 2)} and exit 0.
   *
   * <p>⚠️ AND IT IS REACHABLE BY ACCIDENT: {@code review.sh record} does not
   * validate {@code --task}, so omitting the flag writes {@code "task": ""}. A
   * counter whose own definition is "a round IS a reviewed hash" then reports
   * round 0 over two present verdicts, which is impossible on its own terms —
   * the shape that says the records are broken, not that the work is new.
   * M4.29 owns requiring the flag; this pins the refusal visible from here.
   */
  @Test
  void verdictsThatNameNOTASKRefuseRatherThanCountingAsROUNDZERO(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir, 5, null);
    Path reviews = d.resolve(".harness/review");
    // ⚠️ EVERY verdict, not just this hash's: an empty task on the staged hash
    // alone would leave the earlier rounds countable and refuse on the cap, which
    // is the right answer for the wrong reason.
    try (var files = Files.list(reviews)) {
      for (Path f : files.toList()) {
        Files.writeString(f, verdict("", f.getFileName().toString().split("\\.")[0]));
      }
    }

    Gate g = gate(d);
    assertThat(g.status())
        .as("verdicts naming no task are a broken record, not a fresh first round\n%s",
            g.output())
        .isNotZero();
    assertThat(g.output())
        .as("and the refusal must say WHY, not report round 0\n%s", g.output())
        .contains("name no task");
  }

  @Test
  void aROUNDSLineALREADYCOMMITTEDDoesNOTWaiveALATERCap(@TempDir Path dir) throws Exception {
    // ⚠️ REQUIRING THE ARGUMENT ON AN ADDED LINE IS WHAT STOPS THE ESCAPE
    // BECOMING A WORKFLOW: a `rounds:` line committed
    // by an EARLIER argued commit then waives the cap for every later one, while
    // appearing nowhere in the diff under review. That is exactly the "growing list
    // is a signal, not a workflow" failure rule 12 warns about, arriving invisibly.
    //
    // ⚠️ AND IT DOES NOT PIN THE CONTEXT RENDERING, which a draft of this comment
    // claimed. Measured by review across 18 mutations: the all-diff-lines
    // loosening is killed by the SIBLING test
    // `aROUNDSLineAppearingOnlyAsDIFFCONTEXT` and NOT by this one, because here
    // `baselines/review.txt` is unchanged between HEAD and the index, so it does
    // not appear in `git diff --cached -- baselines/review.txt` at all -- as
    // context or otherwise. This test dies on a strict SUBSET of what the context
    // test dies on -- so it pins nothing the context test does not. A draft of
    // this comment said it "uniquely pins HISTORY", which a strict subset cannot
    // do; review could construct no mutation this test kills and that one does
    // not. It is kept because the HISTORY fixture states the rule readably, not
    // because it is load-bearing on its own.
    // ⚠️ The added-line primitive is
    // wrong in the other direction too: git renders a MOVED committed line as
    // delete+add, which re-arms the escape, and an argument that has landed can
    // never carry a later round. M4.31 replaces the primitive; both cases are
    // measured there.
    Path d = scratch(dir, 3, null);
    Files.createDirectories(d.resolve("baselines"));
    Files.writeString(d.resolve("baselines/review.txt"),
        "rounds:M9.1  argued for an EARLIER commit and committed with it\n");
    // ⚠️ COMMITTING TAKES THE STAGED DOCS CHANGE WITH IT, so the diff under review
    // has to be re-created afterwards -- otherwise the gate says "nothing staged"
    // and the test passes for a reason that has nothing to do with the cap.
    run(d, "git add -A && git commit -qm 'an earlier argued commit'");
    Files.writeString(d.resolve("docs/internal/product/backlog.md"),
        "| M9.1 | x | — | **done again** |\n");
    run(d, "git add -A -- docs");

    // ⚠️ VERDICTS FOR THE NEW HASH, or this test passes for the wrong reason.
    // Committing moved the staged bytes, so without these the gate refuses on a
    // MISSING VERDICT in both arms -- and the first version of this test did
    // exactly that: deleting the guard it exists to pin left it green.
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"), verdict("M9.1", sha));
    }

    Gate g = gate(d);
    assertThat(g.status())
        .as("an argument in HISTORY is not an argument for THIS diff -- the line must be "
            + "staged here, so each exception is reviewed with the commit it excuses\n%s",
            g.output())
        .isNotZero();
  }

  @Test
  void verdictsDISAGREEINGAboutTheTaskAreREFUSEDRatherThanResolved(@TempDir Path dir)
      throws Exception {
    // ⚠️ THE COUNT USED TO RESOLVE BY DIRECTORY ORDER. `rounds_for` broke on the
    // first glob hit, so if two roles recorded different `task` values for one
    // hash the gate read whichever the filesystem listed first -- measured: a
    // reviewer under a 3-round task and a test-reviewer under a 1-round task
    // silently produced "round 1 of 2" and the cap never fired. `review.sh record`
    // does not validate `--task`, so a typo reaches this as easily as intent.
    // ⚠️ REFUSED, NOT RESOLVED: choosing a winner would be choosing which reviewer
    // to believe about what they reviewed.
    Path d = scratch(dir, 3, null);
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    Files.writeString(reviews.resolve(sha + ".test-reviewer.json"), verdict("M9.9", sha));

    Gate g = gate(d);
    assertThat(g.status())
        .as("contradictory artifacts stop the commit instead of one of them winning\n%s",
            g.output())
        .isNotZero();
    assertThat(g.output()).contains("disagree");
  }

  @Test
  void aFILENAMEThatMerelyMATCHESTheGuardAsAPatternDoesNOTLiftTheCap(@TempDir Path dir)
      throws Exception {
    // ⚠️ `grep -qx 'baselines/review.txt'` IS A BRE, so the `.` was a wildcard --
    // the exact defect fixed for the task id a few lines below and left sitting in
    // the FILENAME. Measured by review: staging a file named
    // `baselines/review_txt`, with the real argument committed in HEAD, waived the
    // cap.
    Path d = scratch(dir, 3, null);
    Files.createDirectories(d.resolve("baselines"));
    Files.writeString(d.resolve("baselines/review.txt"),
        "rounds:M9.1  committed earlier, not added here\n");
    run(d, "git add -A && git commit -qm 'earlier'");
    // ⚠️ THE DECOY CARRIES A REAL ARGUMENT, not the word "decoy". With inert
    // content this fixture pinned only the BASENAME, and review measured two
    // surviving loosenings: a `baselines/` pathspec and a `baselines/review*`
    // glob both waived the cap with 15/15 green. Staging `baselines/review.txt.bak`
    // carrying the key was then enough to argue while `review.txt` stayed
    // untouched -- the same wildcard defect this test exists for, one level out,
    // in the DIRECTORY rather than the basename.
    Files.writeString(d.resolve("baselines/review_txt"),
        "rounds:M9.1  argued in a decoy filename\n");
    Files.writeString(d.resolve("docs/internal/product/backlog.md"),
        "| M9.1 | x | — | **again** |\n");
    run(d, "git add -A -- baselines/review_txt docs");
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"), verdict("M9.1", sha));
    }

    Gate g = gate(d);
    assertThat(g.status())
        .as("a decoy filename argues nothing\n%s", g.output())
        .isNotZero();
  }

  @Test
  void aROUNDSLineAppearingOnlyAsDIFFCONTEXTDoesNOTLiftTheCap(@TempDir Path dir)
      throws Exception {
    // ⚠️ "THE FILE WAS TOUCHED" IS NOT "THE LINE WAS ADDED". With the argument
    // already in HEAD, any staged edit to the same file waived the cap -- the
    // rounds line shows up in the diff as CONTEXT. The escape is meant to be
    // reviewed with the commit it excuses, and a context line is reviewed with a
    // different one, so the key must be on an ADDED line.
    Path d = scratch(dir, 3, null);
    Files.createDirectories(d.resolve("baselines"));
    Files.writeString(d.resolve("baselines/review.txt"),
        "rounds:M9.1  committed earlier, appears below only as context\n");
    run(d, "git add -A && git commit -qm 'earlier'");
    Files.writeString(d.resolve("baselines/review.txt"),
        "rounds:M9.1  committed earlier, appears below only as context\n"
            + "reviewer:R7  an unrelated finding argued legitimately\n");
    run(d, "git add -A -- baselines");
    Path reviews = d.resolve(".harness/review");
    String sha = out(d, "git diff --cached | sha256sum | cut -d' ' -f1").trim();
    for (String role : List.of("reviewer", "test-reviewer")) {
      Files.writeString(reviews.resolve(sha + "." + role + ".json"), verdict("M9.1", sha));
    }

    Gate g = gate(d);
    assertThat(g.status())
        .as("editing the file for another reason does not re-argue an old exception\n%s",
            g.output())
        .isNotZero();
  }
}
