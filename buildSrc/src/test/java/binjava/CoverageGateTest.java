// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * check-coverage.sh / coverage.py, through the production door.
 *
 * <p>Every other gate backend here is exercised by a test that runs the real
 * script against a fixture -- tdd_scan.py by JavaTestParserTest,
 * check-module.sh by ModuleGateTest, check-gate-scope.sh by FreshCheckoutTest.
 * This one shipped without, and review named five mutations that survived:
 * deleting the missing-report failure, turning NOT-MEASURED into ok, flipping
 * the floor test's or to and, dropping the exit-status capture, and hardcoding
 * pct() to 100.
 *
 * <p>The characteristic failure of a threshold gate is not a wrong threshold --
 * it is reporting success while measuring nothing.
 */
class CoverageGateTest {

  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-coverage.sh", "coverage.py", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    // ⚠️ The stub RECORDS its arguments and, when asked for a report, WRITES
    // one. A stub that merely exits 0 made the gate's regeneration step
    // invisible: deleting it left every test green, so the fix for the
    // stale-report defect had no regression protection at all.
    stubGradlew(dir, "exit 0");
    return dir;
  }

  /**
   * Writes the stub. {@code body} runs after the invocation is recorded, so a
   * test can make regeneration fail, or make it produce a different report than
   * the one already on disk.
   */
  private void stubGradlew(Path dir, String body) throws Exception {
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "echo \"$@\" >> \"$(dirname \"$0\")/gradlew-invocations\"\n"
            + body + "\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
  }

  private void module(Path repo, String name, String reportXml, boolean withClasses)
      throws Exception {
    Files.createDirectories(repo.resolve(name));
    Files.writeString(repo.resolve(name).resolve("build.gradle.kts"), "// stub\n");
    if (withClasses) {
      Path classes = repo.resolve(name).resolve("build/classes/java/main/binjava");
      Files.createDirectories(classes);
      Files.writeString(classes.resolve("X.class"), "stand-in for bytecode");
    }
    if (reportXml != null) {
      Path rep = repo.resolve(name).resolve("build/reports/jacoco/test");
      Files.createDirectories(rep);
      Files.writeString(rep.resolve("jacocoTestReport.xml"), reportXml);
    }
  }

  private static String report(int lineCov, int lineMiss, int brCov, int brMiss) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<report name=\"x\">"
        + "<counter type=\"LINE\" missed=\"" + lineMiss + "\" covered=\"" + lineCov + "\"/>"
        + "<counter type=\"BRANCH\" missed=\"" + brMiss + "\" covered=\"" + brCov + "\"/>"
        + "</report>\n";
  }

  private void commit(Path repo) throws Exception {
    Process p = new ProcessBuilder("bash", "-c",
            "set -o pipefail; git init -q . && git config user.email t@e"
                + " && git config user.name t && git add -A && git commit -qm base")
        .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
  }

  private String gate(Path repo) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder("bash", "scripts/check-coverage.sh").directory(repo.toFile());
    pb.environment().put("GATE_SCOPE", "full");
    for (String v : List.of("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "CHECK_RANGE")) {
      pb.environment().remove(v);
    }
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  /** The case the whole gate rests on: nothing to measure is NOT success. */
  @Test
  void reportsNotMeasuredRatherThanOkWhenNoModuleHasClasses(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", null, false);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).contains("NOT-MEASURED");
    assertThat(out).as(out).doesNotContain("module(s) meet");
  }

  @Test
  void failsBelowTheFloors(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(3, 1, 3, 3), true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("format").contains("75.0").contains("50.0");
  }

  /**
   * Below EITHER floor fails. The existing failing fixture is below both, so
   * it cannot distinguish `or` from `and` -- flipping the operator survived it.
   * These two are below exactly one floor each.
   */
  @Test
  void failsBelowEitherFloorIndependently(@TempDir Path dir) throws Exception {
    Path lineOnly = scratch(dir.resolve("a"));
    module(lineOnly, "format", report(90, 10, 100, 0), true);
    commit(lineOnly);
    String outLine = gate(lineOnly);
    assertThat(outLine).as("line 90 < 95, branch 100 >= 90\n%s", outLine).startsWith("1");

    Path branchOnly = scratch(dir.resolve("b"));
    module(branchOnly, "format", report(100, 0, 80, 20), true);
    commit(branchOnly);
    String outBranch = gate(branchOnly);
    assertThat(outBranch).as("line 100 >= 95, branch 80 < 90\n%s", outBranch).startsWith("1");
  }

  /**
   * ⚠️ EXACTLY on the floor. No other fixture lands on 95.0 or 90.0 — they are
   * all strictly above or strictly below — so flipping either {@code <} to
   * {@code <=} survived every test. The gate is named after these two numbers
   * and said nothing about landing on them.
   *
   * <p>{@code covered=19, missed=1} is 95.0% and {@code covered=9, missed=1} is
   * 90.0%: ordinary small-module JaCoCo output, not a contrived edge. The floor
   * is a minimum, so exactly-at-the-floor PASSES.
   */
  @Test
  void exactlyAtTheFloorPasses(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(19, 1, 9, 1), true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("95.0 line / 90.0 branch is AT the floor, not below\n%s", out)
        .startsWith("0");
  }

  /**
   * ⚠️ testing.md rule 7's exclusion mechanism, which no test exercised: with no
   * {@code baselines/coverage.txt} in any fixture, {@code skips} was always
   * empty and the whole branch was dead. Deleting it, inverting it, or making
   * {@code excluded()} excuse everything all passed.
   */
  @Test
  void honoursAnExclusionListedInTheBaseline(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(3, 1, 3, 3), true);   // 75% / 50% -- would fail
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/coverage.txt"),
        "# module  why\nformat  no product code yet; M1 lands the SPI\n");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("an excluded module must not fail the gate\n%s", out).startsWith("0");
  }

  /**
   * ⚠️ A report with NO BRANCH counter at all. JaCoCo omits it for a class with
   * no branches — a record, an enum, a constant holder — which is precisely what
   * `binstore-spi` ships today. Every other fixture supplies both counters, so
   * {@code pct()}'s {@code total == 0} guard was never reached and deleting it
   * survived: the gate would then raise ZeroDivisionError on an ordinary POJO,
   * and the inverse mutation (return 0.0) would fail every branch-free module.
   *
   * <p>No branches means nothing to miss, so branch coverage is 100%.
   */
  @Test
  void aReportWithNoBranchCounterScoresFullBranchCoverage(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<report name=\"x\">"
            + "<counter type=\"LINE\" missed=\"0\" covered=\"10\"/></report>\n", true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("a branch-free class must not crash or fail the gate\n%s", out)
        .startsWith("0");
    assertThat(out).as(out).doesNotContain("Traceback");
    // ⚠️ The VALUE, not just the exit status. Asserting only "it passed" left
    // `c.get('BRANCH', (0, 0))` -> `(91, 100)` alive: 91% clears the 90 floor,
    // so the gate passed and a test named ...ScoresFullBranchCoverage agreed.
    assertThat(out).as("no branches means none missed, so 100%%\n%s", out)
        .contains("branch 100.0%");
    assertThat(out).as("the module must be COUNTED, not skipped\n%s", out)
        .contains("1 module(s) meet");
  }

  /**
   * ⚠️ buildSrc is measured like any other module, and this test exists because
   * the opposite was written first on a rationale that was false. The claim was
   * that dropping a {@code buildSrc/} filter makes the gate fail every commit.
   * It does not: {@code has_classes} globs {@code build/classes/java/main},
   * buildSrc's main sources are Kotlin, {@code :compileJava} is NO-SOURCE, and
   * the loop already skips it. The filter was dead code.
   *
   * <p>⚠️ Named for the LIST, not for measurement. buildSrc is not regenerated
   * like other modules — it is an implicit included build the root
   * {@code ./gradlew test jacocoTestReport} never reaches — and this test does
   * not claim otherwise. That gap is M0.44.
   *
   * <p>Dead was the smaller problem. {@code buildSrc/build.gradle.kts} says
   * buildSrc "gets tests like anything else", so the filter would have
   * un-measured the licence gate and the TDD parser precisely when they gained
   * Java sources — silently exempting the code this repository's own gates
   * depend on. So the fixture builds the state that filter anticipated, and
   * asserts the gate REFUSES it rather than waving it through.
   */
  @Test
  void buildSrcIsNotExemptFromTheModuleList(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(100, 0, 100, 0), true);
    module(repo, "buildSrc", null, true);   // Java classes, no JaCoCo report
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("buildSrc with unmeasured Java classes must FAIL\n%s", out)
        .startsWith("1");
    assertThat(out).as(out).contains("buildSrc has compiled classes but no JaCoCo report");
  }

  /**
   * ⚠️ The summary line must not claim a success that did not happen.
   * `measured` was incremented BEFORE the exclusion check, so a tree whose only
   * module was baselined printed "ok 1 module(s) meet 95% line / 90% branch"
   * while zero modules met either floor. Exit status was right and the sentence
   * was false — the failure mode this gate exists to refuse, in the gate itself.
   */
  @Test
  void anExcludedModuleIsNotCountedAsMeetingTheFloors(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(1, 9, 1, 9), true);   // 10% / 10%
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/coverage.txt"),
        "# module  why\nformat  no product code yet; M1 lands the SPI\n");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("an excluded module must not fail the gate\n%s", out).startsWith("0");
    assertThat(out).as("zero modules met the floors, so none may be claimed\n%s", out)
        .doesNotContain("module(s) meet");
    assertThat(out).as("and the exclusion must still be visible\n%s", out)
        .contains("EXCLUDED");
    // ⚠️ Not the "nothing built yet" wording. Both states reach measured == 0
    // and only one of them is an empty repository.
    assertThat(out).as("an all-excluded tree is not an unbuilt one\n%s", out)
        .contains("every module with classes is excluded");
    assertThat(out).as(out).doesNotContain("no module has compiled classes yet");
  }

  /**
   * ⚠️ Blank lines and comments, which became load-bearing this round. The
   * refusal guard dereferences {@code parts[0]}, so dropping {@code not line}
   * from the skip turns a blank line into
   * {@code IndexError: list index out of range} — a traceback where the gate
   * promises a diagnostic, one layer below where
   * {@code aMalformedReportFailsWithAReasonRatherThanATraceback} refuses the
   * same shape. Dropping {@code startswith('#')} instead makes a single-token
   * comment parse as a reasonless exclusion and refuses a valid baseline.
   * Both survived the suite; this one fixture kills both.
   */
  @Test
  void blankLinesAndCommentsAreSkippedNotParsed(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(3, 1, 3, 3), true);   // 75%/50% -- needs the exclusion
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/coverage.txt"),
        "# module  why\n#TODO\n\nformat  no product code yet; M1 lands the SPI\n");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("a comment must not parse as a reasonless exclusion\n%s", out)
        .startsWith("0");
    assertThat(out).as("a blank line must not crash the parser\n%s", out)
        .doesNotContain("Traceback");
    assertThat(out).as(out).doesNotContain("is excluded with no reason");
    assertThat(out).as("and the real entry must still be honoured\n%s", out)
        .contains("EXCLUDED:");
  }

  /**
   * ⚠️ The mirror of the branch-free case, and NOT symmetric with it. An absent
   * BRANCH counter is a fact about the code (no branches to miss, so 100% is
   * true); an absent LINE counter is a fact about the MEASUREMENT, because every
   * class has lines. Defaulting both to (0, 0) printed
   * {@code ok  line 100.0% (floor 95)} for a report whose instruction counter
   * read 10% — the gate's headline number certifying an unmeasured module.
   * {@code -g:none} and {@code options.debug = false} both produce it, and the
   * {@code c == {}} guard never sees it because the report is not empty.
   */
  @Test
  void aReportWithNoLineCounterIsUnmeasuredNotFullyCovered(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<report name=\"x\">"
            + "<counter type=\"INSTRUCTION\" missed=\"900\" covered=\"100\"/>"
            + "<counter type=\"BRANCH\" missed=\"0\" covered=\"10\"/></report>\n", true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("an unmeasured module must not pass\n%s", out).startsWith("1");
    assertThat(out).as(out).contains("report has no LINE counter");
    assertThat(out).as("and must never be scored as fully covered\n%s", out)
        .doesNotContain("line 100.0%");
  }

  /**
   * ⚠️ An exclusion with no reason is REFUSED. The gate printed "each entry
   * names what unblocks it" while a bare module name parsed fine and rendered
   * as "EXCLUDED:" with nothing after the colon — a property asserted in the
   * output and never checked. testing.md rule 7 makes the baseline a ratchet;
   * a nameless entry is a permanent exemption wearing a ratchet's clothes.
   */
  @Test
  void anExclusionWithoutAReasonIsRefused(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(100, 0, 100, 0), true);
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/coverage.txt"), "# module  why\nformat\n");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("a bare module name must not be accepted\n%s", out).startsWith("1");
    assertThat(out).as(out).contains("is excluded with no reason");
    // ⚠️ The LOCATOR, not just the refusal. Hardcoding the line number and the
    // module name both survived: the gate refused correctly while pointing at a
    // line and a module that do not exist, which in a thirty-row baseline is
    // the entire actionable content of the message.
    assertThat(out).as("must name the offending LINE\n%s", out).contains("coverage.txt:2");
    assertThat(out).as("must name the offending MODULE\n%s", out).contains("\"format\"");
    // ⚠️ and it must not ALSO be honoured as an exclusion on the way out
    assertThat(out).as(out).doesNotContain("EXCLUDED:");
  }

  /** Compiled classes with NO report is a FAILURE, not a quiet skip. */
  @Test
  void failsWhenAModuleHasClassesButNoReport(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", null, true);
    commit(repo);
    assertThat(gate(repo)).startsWith("1");
  }

  @Test
  void passesAboveTheFloors(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(99, 1, 95, 1), true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).doesNotContain("NOT-MEASURED");
  }

  /**
   * ⚠️ THE test for the stale-report defect, and the one whose absence was
   * blocking. A PASSING report is planted on disk; the stub, when the gate
   * regenerates, replaces it with a FAILING one. The gate must report the
   * fresh numbers.
   *
   * <p>Delete the regeneration call and the planted passing report wins —
   * which is precisely the bug: a module compiled after its last report was
   * written stays green forever.
   */
  @Test
  void usesTheRegeneratedReportRatherThanOneAlreadyOnDisk(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(100, 0, 100, 0), true);
    Path fresh = repo.resolve("format/build/reports/jacoco/test/jacocoTestReport.xml");
    // ⚠️ The stub refreshes the report ONLY when `test` is among the tasks,
    // because that is how Gradle behaves: the report is built from exec data
    // that only `test` produces. Running `jacocoTestReport` alone leaves the
    // exec data — and therefore the report — untouched, which is precisely the
    // defect review reproduced (`:jacocoTestReport UP-TO-DATE`, coverage still
    // 100% after a test was weakened). Without this condition the stub hid it.
    stubGradlew(repo,
        "case \" $@ \" in *\" test \"*) cat > '" + fresh + "' <<'XML'\n"
            + report(3, 1, 3, 3) + "XML\n  ;; esac");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as("the stale passing report must not win\n%s", out).startsWith("1");
    assertThat(out).as(out).contains("75.0").contains("50.0");
    assertThat(repo.resolve("gradlew-invocations"))
        .as("the gate must actually invoke Gradle")
        .exists();
  }

  /** If the report cannot be regenerated, coverage was NOT measured. */
  @Test
  void failsWhenTheReportCannotBeRegenerated(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", report(100, 0, 100, 0), true);
    stubGradlew(repo, "echo 'jacoco exploded' >&2; exit 1");
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("could not generate");
  }

  /**
   * ⚠️ Well-formed XML with NO counters at all — distinct from the malformed
   * case below, which raises ParseError and takes a different branch. This one
   * parses cleanly and used to score 100%: {@code counters()} returned an empty
   * map and {@code pct()}'s (0,0) default reads as full coverage. A module with
   * no instrumented classes produces exactly this shape.
   *
   * <p>Reporting success while measuring nothing is the one thing this gate
   * exists to refuse, so the empty-report case gets its own fixture rather than
   * sharing the malformed one.
   */
  @Test
  void aReportThatParsesButCarriesNoCountersIsNotFullCoverage(@TempDir Path dir)
      throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", "<?xml version=\"1.0\"?><report name=\"x\"></report>", true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("no counters");
  }

  /**
   * A malformed report must fail with a REASON, not a Python traceback. JaCoCo
   * 0.8.12 emitted a zero-byte XML on Java 25 bytecode and coverage.py died
   * with ElementTree.ParseError. No current version does -- but "the tool
   * stopped producing bad input" is not "the gate handles bad input".
   */
  @Test
  void aMalformedReportFailsWithAReasonRatherThanATraceback(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format", "", true);
    commit(repo);
    String out = gate(repo);
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).doesNotContain("Traceback");
  }
}
