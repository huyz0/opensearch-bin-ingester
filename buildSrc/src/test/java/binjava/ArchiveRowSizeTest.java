// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A row ADDED to the archive is part of the review surface of the commit that
 * adds it, and nothing was collecting that.
 *
 * <p>⚠️ MEASURED over the archive as it stands in the BASE — what the gate itself
 * reads, via {@code git show <base>:<archive>} — with its own recogniser: 121
 * rows, median 1,306 characters, maximum 18,845 (the M0.56 row), and 25 rows
 * already over the 4,000 cap. A ref that moves as this commit lands would say 122. An earlier draft of this javadoc said
 * the maximum was 11,481, which is the SECOND largest — and gave the count as
 * 120, the archive minus exactly the row it had missed. {@code
 * check-session-load.sh} already recorded 18,845 in its own header, so the wrong
 * figure contradicted a sibling gate standing in the same tree.
 *
 * <p>⚠️ WHAT PROMPTED THE CAP, stated correctly: the seven tasks M4.8 through
 * M4.9 took 40 review rounds between them, and wrote four of the archive's seven
 * largest rows. M4.9 alone ran 9. An earlier draft attributed all 40 to M4.9.
 *
 * <p>⚠️ {@link BacklogBoundsTest} ARGUES THE ARCHIVE MUST NOT BE JUDGED — "it
 * would mean rewriting rows that already shipped ... and the archive is not
 * loaded by anything, so its size costs nothing". Both halves hold of EXISTING
 * rows and this gate leaves them alone: exemption is by ID, so an over-cap row
 * that already shipped stays editable. The cost it collects is a different one
 * from session load.
 *
 * <p>Every test but the last carries a negative control, because a bound with no
 * counter-case is indistinguishable from a gate that refuses everything. ⚠️ THE
 * FAIL-CLOSED TEST DOES NOT: both of its cases assert refusal, so its labelled
 * "negative control" is a second refusal case. Its counter-case is the other four
 * tests, which all run the gate in a working repository and expect exit 0.
 */
class ArchiveRowSizeTest {

  private static final String ARCHIVE = "docs/internal/product/backlog-done.md";
  private static final String HDR = "| ID | Task | Serves | State |\n|---|---|---|---|\n";
  private static final int CAP = 4000;

  private record Gate(int status, String output) {
  }

  private Gate run(Path dir, Map<String, String> env, String... cmd) throws Exception {
    ProcessBuilder b = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true);
    b.environment().putAll(env);
    Process p = b.start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Gate(p.waitFor(), out);
  }

  private Gate git(Path dir, String... args) throws Exception {
    String[] cmd = new String[args.length + 1];
    cmd[0] = "git";
    System.arraycopy(args, 0, cmd, 1, args.length);
    return run(dir, Map.of(), cmd);
  }

  /** A scratch git repository holding the gate; the archive starts committed and empty. */
  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-archive-row-size.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    git(dir, "init", "-q", "-b", "main");
    git(dir, "config", "user.email", "t@example.com");
    git(dir, "config", "user.name", "t");
    write(dir, HDR);
    git(dir, "add", "-A");
    git(dir, "commit", "-qm", "base");
    return dir;
  }

  private void write(Path dir, String body) throws Exception {
    Path p = dir.resolve(ARCHIVE);
    Files.createDirectories(p.getParent());
    Files.writeString(p, body);
  }

  /** A row whose full text is exactly {@code n} characters. */
  private String row(String id, int n) {
    String head = "| " + id + " | ";
    String tail = " | — | done |";
    return head + "x".repeat(n - head.length() - tail.length()) + tail;
  }

  private Gate stageAndRun(Path d, String archive) throws Exception {
    write(d, archive);
    git(d, "add", "-A");
    return run(d, Map.of(), "bash", "scripts/check-archive-row-size.sh");
  }

  /** The cap binds, and a row under it is not refused. Kills a widened CAP. */
  @Test
  void anAddedRowOverTheCapIsRefusedAndOneUnderItIsNot(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);

    Gate over = stageAndRun(d, HDR + row("M9.1", CAP + 1) + "\n");
    assertThat(over.status()).as(over.output()).isNotZero();
    assertThat(over.output()).contains("M9.1", String.valueOf(CAP + 1));

    Gate under = stageAndRun(d, HDR + row("M9.1", CAP) + "\n");
    assertThat(under.status()).as("a row of exactly the cap is within it: " + under.output())
        .isZero();
  }

  /**
   * Shapes a literal '| M' prefix let through — while printing a success line
   * over an empty set, which reads as nothing to judge rather than I could not
   * parse it.
   *
   * <p>⚠️ A BOLDED ID IS NOT DISMISSIBLE AS UNCITABLE: check-commit-msg.sh
   * accepts the subject {@code M9.2} against a row headed {@code | **M9.2** |}.
   */
  @Test
  void aTightlySpacedBoldedOrPipelessRowIsStillJudged(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);
    List<String> heads =
        List.of("|  M9.2 | ", "|M9.2 | ", " | M9.2 | ", "| **M9.2** | ", "| `M9.2` | ",
            "M9.2 | ", "| M9.2.1 | ");
    for (String head : heads) {
      String r = head + "x".repeat(CAP) + " | — | done |";
      Gate g = stageAndRun(d, HDR + r + "\n");
      assertThat(g.status()).as("shape '" + head + "' must not evade the cap: " + g.output())
          .isNotZero();
    }
    // Negative control: every one of those shapes, under the cap, is accepted.
    for (String head : heads) {
      Gate ok = stageAndRun(d, HDR + head + "short | — | done |\n");
      assertThat(ok.status()).as("shape '" + head + "' under the cap: " + ok.output()).isZero();
    }
  }

  /**
   * Exemption is by ID, so a row that already shipped stays EDITABLE.
   *
   * <p>⚠️ 25 of the 121 shipped rows are already over the cap. Under a
   * byte-equality exemption any later edit to one of them — a glossary rename
   * reaches every *.md — would fail with "summarise it", which is the action
   * {@link BacklogBoundsTest} calls turning a size gate into an instruction to
   * destroy the record.
   *
   * <p>⚠️ AN EARLIER VERSION OF THIS TEST PROVED NOTHING: it inserted a row above
   * the legacy one, so git reported only the inserted line, the legacy row never
   * appeared as `+` at all, and the test passed with the exemption deleted.
   * Editing the shipped row is what puts it into the added set.
   */
  @Test
  void anOverCapRowThatAlreadyShippedStaysEditableButANewOneDoesNot(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir);
    String legacy = row("M9.3", CAP + 500);
    write(d, HDR + legacy + "\n");
    git(d, "add", "-A");
    git(d, "commit", "-qm", "legacy row predates the gate");

    Gate edited = stageAndRun(d, HDR + legacy.replace("| — | done |", "| FR-1 | done |") + "\n");
    assertThat(edited.output()).as("the edit must reach the gate as an added row")
        .contains("1 already in the base and exempt");
    assertThat(edited.status()).as("editing a shipped over-cap row must not be refused: "
        + edited.output()).isZero();

    Gate fresh = stageAndRun(d, HDR + legacy + "\n" + row("M9.4", CAP + 1) + "\n");
    assertThat(fresh.status()).as("a genuinely new oversized row still fails: " + fresh.output())
        .isNotZero();
    assertThat(fresh.output()).contains("M9.4").doesNotContain("M9.3");
  }

  /**
   * The gate runs in CI, where nothing is ever staged.
   *
   * <p>⚠️ THIS IS THE VACUOUS-PASS CASE. The hook is `always_run` and CI does not
   * skip it; a staged-only reading reports success over an empty index and exits
   * 0 in a fresh checkout no matter what the range added. CHECK_RANGE is what CI
   * supplies.
   */
  @Test
  void withACheckRangeTheGateJudgesTheRangeRatherThanTheEmptyIndex(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir);
    Gate baseRef = git(d, "rev-parse", "HEAD");
    String base = baseRef.output().trim();

    write(d, HDR + row("M9.5", CAP + 1) + "\n");
    git(d, "add", "-A");
    git(d, "commit", "-qm", "an oversized row, committed and no longer staged");

    Gate staged = run(d, Map.of(), "bash", "scripts/check-archive-row-size.sh");
    assertThat(staged.status()).as("nothing is staged, so the staged reading is silent")
        .isZero();

    Gate ranged = run(d, Map.of("CHECK_RANGE", base), "bash", "scripts/check-archive-row-size.sh");
    assertThat(ranged.status()).as("the range must catch what the empty index cannot: "
        + ranged.output()).isNotZero();
    assertThat(ranged.output()).contains("M9.5");

    // Negative control: the same range over a commit that added a row under the cap.
    Path d2 = scratch(dir.resolve("second"));
    String base2 = git(d2, "rev-parse", "HEAD").output().trim();
    write(d2, HDR + row("M9.6", CAP) + "\n");
    git(d2, "add", "-A");
    git(d2, "commit", "-qm", "a row within the cap");
    Gate ok = run(d2, Map.of("CHECK_RANGE", base2), "bash", "scripts/check-archive-row-size.sh");
    assertThat(ok.status()).as("a range adding only a compliant row passes: " + ok.output())
        .isZero();
    assertThat(ok.output()).contains("CI range");
  }

  /** An unreadable diff must fail CLOSED, and say so, rather than report zero rows. */
  @Test
  void anUnreadableDiffIsRefusedByNameRatherThanReportedAsClean(@TempDir Path dir)
      throws Exception {
    Path d = dir.resolve("norepo");
    Files.createDirectories(d.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("check-archive-row-size.sh", "lib.sh")) {
      Files.copy(repo.resolve("scripts").resolve(f), d.resolve("scripts").resolve(f));
    }
    Gate g = run(d, Map.of(), "bash", "scripts/check-archive-row-size.sh");
    assertThat(g.status()).as("outside a repository the gate must refuse: " + g.output())
        .isNotZero();
    assertThat(g.output()).as("and must name the refusal, not die of an incidental error")
        .contains("refusing rather than reporting a clean tree");

    // Negative control: an unresolvable CHECK_RANGE inside a good repository.
    Path good = scratch(dir.resolve("good"));
    Gate bad = run(good, Map.of("CHECK_RANGE", "0000000000000000000000000000000000000000"),
        "bash", "scripts/check-archive-row-size.sh");
    assertThat(bad.status()).as("an unresolvable range must refuse too: " + bad.output())
        .isNotZero();
  }

  /**
   * ⚠️ A FILE THAT DID NOT EXIST AT THE BASE IS NOT AN UNREADABLE BASE. The
   * gate reads the archive at the base to decide which rows are NEW, and it
   * fails closed when that read fails -- correctly, for a base it cannot see.
   * But an archive ADDED by the range under test has no version at the base at
   * all, and every row in it is new by definition.
   *
   * <p>MEASURED IN CI: the first push after `dependencyLicenses` was fixed
   * carried the commit that CREATED backlog-done.md, so this gate refused with
   * "could not read ... at 7d9c9f24" and took the whole run down. The licence
   * failure had masked it on every earlier run.
   */
  @Test
  void anArchiveABSENTAtTheBaseIsEveryRowNEWNotAFailure(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);
    // scratch() commits an archive, so start from a commit that has none.
    git(d, "rm", "-q", ARCHIVE);
    git(d, "commit", "-qm", "a base with no archive at all");
    String base = git(d, "rev-parse", "HEAD").output().trim();

    write(d, HDR + row("M9.7", CAP) + "\n");
    git(d, "add", "-A");
    git(d, "commit", "-qm", "add the archive");

    Gate g = run(d, Map.of("CHECK_RANGE", base), "bash", "scripts/check-archive-row-size.sh");

    assertThat(g.status()).as("a NEW archive is not an unreadable base: " + g.output()).isZero();
    assertThat(g.output()).doesNotContain("could not read");
  }

  /** ⚠️ And an oversized row in that newly added archive is still REFUSED. */
  @Test
  void anOversizedRowInANEWArchiveIsStillRefused(@TempDir Path dir) throws Exception {
    Path d = scratch(dir);
    git(d, "rm", "-q", ARCHIVE);
    git(d, "commit", "-qm", "a base with no archive at all");
    String base = git(d, "rev-parse", "HEAD").output().trim();

    write(d, HDR + row("M9.8", CAP + 1) + "\n");
    git(d, "add", "-A");
    git(d, "commit", "-qm", "add the archive with an oversized row");

    Gate g = run(d, Map.of("CHECK_RANGE", base), "bash", "scripts/check-archive-row-size.sh");

    assertThat(g.status()).as("the cap still binds in a new file: " + g.output()).isNotZero();
    assertThat(g.output()).contains("M9.8");
  }
}
