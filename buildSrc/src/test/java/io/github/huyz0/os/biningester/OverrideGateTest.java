// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An override entry is diffed against the verdicts recorded for its task
 * (M8.41).
 *
 * <p>⚠️ THE FIXTURES ARE SHAPED LIKE THE REAL FILE: entries run over several
 * lines, a task can have more than one, and a paragraph line can start with an
 * id without being an entry. One-line fixtures described a contract the file
 * does not have, and a parser built to them read only first lines.
 */
class OverrideGateTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  private record Run(int exit, String out) {
  }

  private static Path scratch() throws Exception {
    Path dir = ROOT.resolve("buildSrc/build/tmp/override-gate")
        .resolve(UUID.randomUUID().toString());
    Files.createDirectories(dir.resolve("verdicts"));
    return dir;
  }

  private static void verdict(Path store, String task, String role, String hash,
      String... ids) throws Exception {
    StringBuilder findings = new StringBuilder();
    for (String id : ids) {
      findings.append(findings.length() == 0 ? "" : ",").append("{\"id\":\"").append(id)
          .append("\"}");
    }
    Files.createDirectories(store);
    Files.writeString(store.resolve(hash + "." + role + ".json"),
        "{\"task\":\"" + task + "\",\"role\":\"" + role + "\",\"diff_sha256\":\"" + hash
            + "\",\"verdict\":\"pass\",\"findings\":[" + findings + "]}");
  }

  private static Run run(Path dir, String... command) throws Exception {
    ProcessBuilder pb = ProcessSupport.builder(command).directory(dir.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Run(p.waitFor(), out);
  }

  private static Run check(Path dir, String overrides, String... lines) throws Exception {
    Files.writeString(dir.resolve("overrides.md"), overrides);
    String[] base = {"python3", ROOT.resolve("scripts/override_check.py").toString(), "check",
        dir.resolve("overrides.md").toString(), dir.resolve("verdicts").toString()};
    if (lines.length == 0) {
      return run(dir, base);
    }
    String[] withLines = java.util.Arrays.copyOf(base, base.length + 1);
    withLines[base.length] = String.join(",", lines);
    return run(dir, withLines);
  }

  /** M9.1: two rounds, P1 and T1 then T2. M9.2: one round, T4. */
  private static Path store() throws Exception {
    Path dir = scratch();
    Path v = dir.resolve("verdicts");
    verdict(v, "M9.1", "reviewer", "aaa", "M9.1-P1");
    verdict(v, "M9.1", "test-reviewer", "aaa", "T1");
    verdict(v, "M9.1", "reviewer", "bbb");
    verdict(v, "M9.1", "test-reviewer", "bbb", "T2");
    verdict(v, "M9.2", "test-reviewer", "ccc", "T4");
    return dir;
  }

  @Test
  void aMULTILINEEntryThatMatchesItsVerdictsPASSES() throws Exception {
    Run run = check(store(), "# overrides\n\nM9.1 - TWO ROUNDS: P1 and T1,\n"
        + "then T2 in round two - approved-by: x\n");
    assertThat(run.exit()).as(run.out()).isZero();
  }

  @Test
  void aTaskPREFIXEDFindingIdIsTheSameFinding() throws Exception {
    // ⚠️ `M9.1-P1` in the store is P1: a reviewer sometimes prefixes the task
    Run run = check(store(), "M9.1 - TWO ROUNDS: P1 - approved-by: x\n");
    assertThat(run.exit()).as(run.out()).isZero();
  }

  @Test
  void MORERoundsThanRECORDEDAreRefused() throws Exception {
    Run run = check(store(), "M9.1 - FOUR ROUNDS, each a real defect - approved-by: x\n");
    assertThat(run.exit()).as("⚠️ \"FOUR ROUNDS\" against two recorded").isEqualTo(1);
    assertThat(run.out()).contains("states 4 rounds; 2 recorded");
  }

  @Test
  void aFINDINGOnALATERLineThatNoVerdictRecordsIsRefused() throws Exception {
    Run run = check(store(), "M9.1 - TWO ROUNDS: P1,\nand then T9 - approved-by: x\n");
    assertThat(run.exit()).as("⚠️ T9 is on the entry's SECOND line").isEqualTo(1);
    assertThat(run.out()).contains("T9");
  }

  @Test
  void anotherTASKsFindingDoesNotVouchForThisOne() throws Exception {
    Run run = check(store(), "M9.1 - TWO ROUNDS: T4 - approved-by: x\n");
    assertThat(run.exit()).as("⚠️ T4 is M9.2's, not M9.1's").isEqualTo(1);
  }

  @Test
  void aSECONDEntryForTheSameTaskIsJudgedOnItsOwn() throws Exception {
    Run run = check(store(), "M9.1 - TWO ROUNDS - approved-by: x\n\n"
        + "M9.1 - SIX ROUNDS, later - approved-by: x\n");
    assertThat(run.exit()).as("⚠️ the second entry's SIX is not hidden by the first's TWO")
        .isEqualTo(1);
    assertThat(run.out()).contains("states 6 rounds");
  }

  @Test
  void aPARAGRAPHLineStartingWithAnIdIsNotAnEntry() throws Exception {
    Run run = check(store(), "Some prose that runs on\nM9.9 exists to prevent that.\n");
    assertThat(run.exit()).as(run.out()).isZero();
    assertThat(run.out()).as("no entry was invented").doesNotContain("M9.9");
  }

  @Test
  void ONLYTheEntriesTheGivenLinesTouchAreJudged() throws Exception {
    String file = "M9.1 - FOUR ROUNDS - approved-by: x\n\nM9.2 - ONE ROUND: T4 - x\n";
    assertThat(check(store(), file, "3").exit()).as("line 3 is M9.2's, which is right")
        .isZero();
    assertThat(check(store(), file, "1").exit()).as("line 1 is M9.1's, which is not")
        .isEqualTo(1);
  }

  @Test
  void aTASKWithNoVerdictHereIsUNJUDGEDNotFailedAndNotSilent() throws Exception {
    Run run = check(scratch(), "M9.3 - NINE ROUNDS: T7 - approved-by: x\n");
    assertThat(run.exit()).as("the store is local; a fresh checkout holds none").isZero();
    assertThat(run.out()).as("⚠️ AND IT SAYS SO rather than passing in silence")
        .contains("UNJUDGED M9.3");
  }

  @Test
  void FEWERRoundsThanRecordedPASS_ItIsAnUpperBound() throws Exception {
    // ⚠️ A RE-STAGED DIFF IS A SECOND HASH IN ONE ROUND, so equality would
    // refuse a true entry: two hashes, one round stated, passes.
    Run run = check(store(), "M9.1 - ONE ROUNDS in substance - approved-by: x\n");
    assertThat(run.exit()).as(run.out()).isZero();
  }

  @Test
  void theCOMMITTEDStoreCountsToo() throws Exception {
    // ⚠️ review/verdicts/<task>/ holds rounds the local store does not; read
    // alone, the local one refuses the true "THREE ROUNDS" below.
    Path dir = store();
    verdict(dir.resolve("committed/M9.1"), "M9.1", "reviewer", "ddd", "P5");
    Files.writeString(dir.resolve("overrides.md"),
        "M9.1 - THREE ROUNDS: P1, then P5 - approved-by: x\n");
    String script = ROOT.resolve("scripts/override_check.py").toString();
    String file = dir.resolve("overrides.md").toString();

    Run localOnly = run(dir, "python3", script, "check", file,
        dir.resolve("verdicts").toString());
    Run both = run(dir, "python3", script, "check", file,
        dir.resolve("verdicts") + ":" + dir.resolve("committed"));

    assertThat(localOnly.exit()).as("the premise: the local store alone refuses it")
        .isEqualTo(1);
    assertThat(both.exit()).as("⚠️ WITH THE COMMITTED STORE IT IS TRUE: %s", both.out())
        .isZero();
  }

  @Test
  void aTIERNamedLikeAFindingIsRefusedWithAMessageSayingSo() throws Exception {
    Run run = check(store(), "M9.1 - TWO ROUNDS, a T0 case added - approved-by: x\n");
    assertThat(run.exit()).isEqualTo(1);
    assertThat(run.out()).as("⚠️ LOUD, AND SAYS WHY").contains("T0").contains("tier");
  }

  /** A git repository holding the gate, a committed overrides file, and the store. */
  private static Path repo(String committed) throws Exception {
    Path repo = scratch();
    Files.createDirectories(repo.resolve("scripts"));
    Files.createDirectories(repo.resolve("review"));
    for (String script : new String[] {"lib.sh", "check-override.sh", "override_check.py"}) {
      Files.copy(ROOT.resolve("scripts/" + script), repo.resolve("scripts/" + script));
      repo.resolve("scripts/" + script).toFile().setExecutable(true);
    }
    verdict(repo.resolve(".harness/review"), "M9.1", "reviewer", "aaa", "P1");
    Files.writeString(repo.resolve("review/overrides.md"), committed);
    run(repo, "git", "init", "-q");
    run(repo, "git", "-c", "user.email=t@t", "-c", "user.name=t", "add", "scripts", "review");
    run(repo, "git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base");
    return repo;
  }

  @Test
  void theGATEJudgesAnEditToAnEntrysLATERLine() throws Exception {
    Path repo = repo("# overrides\n\nM9.1 - ONE ROUNDS: P1,\nfirst draft - approved-by: x\n");
    Files.writeString(repo.resolve("review/overrides.md"),
        "# overrides\n\nM9.1 - ONE ROUNDS: P1,\nand also T8 - approved-by: x\n");
    run(repo, "git", "add", "review/overrides.md");

    Run gate = run(repo, "bash", "scripts/check-override.sh");

    assertThat(gate.exit()).as("⚠️ the edit is on the entry's SECOND line: %s", gate.out())
        .isNotZero();
    assertThat(gate.out()).contains("T8");
  }

  @Test
  void theGATEJudgesAnEntryAPUREDeletionTouched() throws Exception {
    // ⚠️ A hunk that only removes lines has no new-side line of its own, so
    // the gate marks where it happened; without that, this reads as "nothing
    // changed" and the entry below, which cites an unrecorded T8, is not seen.
    Path repo = repo("# overrides\n\nM9.1 - ONE ROUNDS: P1,\na line to delete,\n"
        + "and T8 - approved-by: x\n");
    Files.writeString(repo.resolve("review/overrides.md"),
        "# overrides\n\nM9.1 - ONE ROUNDS: P1,\nand T8 - approved-by: x\n");
    run(repo, "git", "add", "review/overrides.md");

    Run gate = run(repo, "bash", "scripts/check-override.sh");

    assertThat(gate.exit()).as(gate.out()).isNotZero();
    assertThat(gate.out()).contains("T8");
  }

  @Test
  void theGATEPassesAConsistentEditAndSaysNothingChangedWhenNothingDid() throws Exception {
    Path repo = repo("# overrides\n\nM9.1 - ONE ROUNDS: P1 - approved-by: x\n");
    assertThat(run(repo, "bash", "scripts/check-override.sh").out())
        .contains("no override entry added or changed");

    Files.writeString(repo.resolve("review/overrides.md"),
        "# overrides\n\nM9.1 - ONE ROUNDS: P1, reworded - approved-by: x\n");
    run(repo, "git", "add", "review/overrides.md");
    Run gate = run(repo, "bash", "scripts/check-override.sh");

    assertThat(gate.exit()).as(gate.out()).isZero();
    assertThat(gate.out()).contains("consistent");
  }
}
