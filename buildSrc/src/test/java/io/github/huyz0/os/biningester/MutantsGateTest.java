// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code scripts/check-mutants.sh} / {@code scripts/mutants.py}, through the
 * production door: the real script, over real jzap report fixtures, in a real
 * scratch repository (M0.14, M9 criterion 1).
 *
 * <p>The fixtures are the shape jzap 0.1.1 actually emits, copied from a
 * measured {@code ./gradlew :format:mutationTestDiff} run rather than guessed:
 * {@code <module>/build/reports/jzap-diff/jzap-result.json}, with
 * {@code mutationScore} a PERCENTAGE, {@code scoredMutants} excluding
 * {@code NON_VIABLE} and {@code RUN_ERROR}, and each mutant keyed
 * {@code class::method::line::MUTATOR#ordinal}.
 *
 * <p>⚠️ The characteristic failure of a threshold gate is not a wrong threshold
 * -- it is reporting success while measuring nothing. So a module whose
 * production sources changed and that produced no report is a FAILURE, a diff
 * that produced no scored mutants is reported NOT-MEASURED rather than ok, and
 * a baseline entry is refused without a reason, refused once the mutant it
 * names is dead, and refused once the key it names has moved.
 *
 * <p>⚠️ AND THE SCRIPT'S NON-SCORING HALF IS PINNED TOO, because four
 * mutations of it survived the first draft of this suite: scoring only the
 * FIRST module, invoking a task that does not exist, ignoring Gradle's exit
 * status, and dropping the {@code CHECK_RANGE} to {@code JZAP_FROM} export.
 * Every one left ten passing tests, which is what a suite that asserts only
 * the happy number looks like.
 */
class MutantsGateTest {

  /** A scratch repository with the two scripts, lib.sh and a stub gradlew. */
  private Path scratch(Path dir) throws Exception {
    return scratch(dir, "exit 0");
  }

  /**
   * @param tail what the stub gradlew does AFTER recording its invocation and
   *     publishing any staged fixtures -- {@code "exit 1"} to make the
   *     mutation run fail.
   */
  private Path scratch(Path dir, String tail) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-mutants.sh", "mutants.py", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    // ⚠️ The stub RECORDS its invocation -- arguments AND the diff base it was
    // given -- and PRODUCES the report, exactly as the real
    // `mutationTestDiff` does. A stub that merely exits 0 would make the
    // gate's Gradle step invisible; a stub whose recording nothing asserts
    // would make the TASK SELECTION invisible, and `TASKS=":bogus:notATask"`
    // measurably survived that.
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "d=\"$(dirname \"$0\")\"\n"
            + "echo \"ARGS: $@\" >> \"$d/gradlew-invocations\"\n"
            + "echo \"JZAP_FROM: ${JZAP_FROM-<unset>}\" >> \"$d/gradlew-invocations\"\n"
            + "cd \"$d\"\n"
            + "for f in fixtures/*.json; do\n"
            + "  [ -e \"$f\" ] || continue\n"
            + "  m=$(basename \"$f\" .json)\n"
            + "  mkdir -p \"$m/build/reports/jzap-diff\"\n"
            + "  cp \"$f\" \"$m/build/reports/jzap-diff/jzap-result.json\"\n"
            + "done\n"
            + tail + "\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
    return dir;
  }

  /** A module with a committed build file and a production source. */
  private void module(Path repo, String name) throws Exception {
    Files.createDirectories(repo.resolve(name + "/src/main/java/io/github/huyz0/os/biningester"));
    Files.writeString(repo.resolve(name).resolve("build.gradle.kts"), "// stub\n");
    Files.writeString(repo.resolve(name + "/src/main/java/io/github/huyz0/os/biningester/X.java"), "class X {}\n");
  }

  /** What the stub gradlew will publish as {@code name}'s diff report. */
  private void fixture(Path repo, String name, String json) throws Exception {
    Files.createDirectories(repo.resolve("fixtures"));
    Files.writeString(repo.resolve("fixtures").resolve(name + ".json"), json);
  }

  /** A report already on disk when the gate starts -- a PREVIOUS diff's. */
  private void staleReport(Path repo, String name, String json) throws Exception {
    Path dir = repo.resolve(name + "/build/reports/jzap-diff");
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("jzap-result.json"), json);
  }

  /** One mutant object, in jzap 0.1.1's own field names. */
  private static String mutant(String key, String status) {
    String cls = key.substring(0, key.indexOf("::"));
    return "{\"key\":\"" + key + "\",\"class\":\"" + cls + "\",\"method\":\"f()J\","
        + "\"line\":33,\"mutator\":\"MATH\",\"ordinal\":0,"
        + "\"sourceFile\":\"io.github.huyz0.os.biningester/X.java\","
        + "\"description\":\"replaced long addition with subtraction\","
        + "\"status\":\"" + status + "\",\"killingTest\":null,"
        + "\"coveringTests\":1,\"testsRun\":1}";
  }

  /**
   * A whole report. ⚠️ {@code mutationScore} and the count fields are written
   * as jzap writes them, but the gate must recompute from {@code mutants} --
   * it excludes baselined mutants, which jzap knows nothing about, and it
   * pools several modules' reports, which no single report can express.
   */
  private static String report(String... mutants) {
    return "{\"engine\":\"schemata\",\"scope\":\"changed lines between HEAD and -Local-\","
        + "\"testsDiscovered\":390,\"mutationScore\":0.0,\"testStrength\":0.0,"
        + "\"scoredMutants\":0,\"unscoredMutants\":0,\"coveredMutants\":0,"
        + "\"detectedMutants\":0,\"failingBaselineTests\":[],"
        + "\"mutants\":[" + String.join(",", mutants) + "],\"timings\":{}}\n";
  }

  private void commit(Path repo) throws Exception {
    run(repo, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
  }

  /** Stages a change to a module's production source, which is what jzap diffs. */
  private void stageProductionChange(Path repo, String name) throws Exception {
    Files.writeString(repo.resolve(name + "/src/main/java/io/github/huyz0/os/biningester/X.java"),
        "class X { long f() { return 1; } }\n");
    run(repo, "git add -A");
  }

  private void run(Path repo, String cmd) throws Exception {
    Process p = ProcessSupport.builder("bash", "-c", "set -o pipefail; " + cmd)
        .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
  }

  private String gate(Path repo) throws Exception {
    return gate(repo, null);
  }

  /** Exit status on the first line, then the gate's output. */
  private String gate(Path repo, String checkRange) throws Exception {
    ProcessBuilder pb =
        ProcessSupport.builder("bash", "scripts/check-mutants.sh").directory(repo.toFile());
    for (String v : List.of("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "CHECK_RANGE",
        "GATE_SCOPE", "JZAP_FROM")) {
      pb.environment().remove(v);
    }
    if (checkRange != null) {
      pb.environment().put("CHECK_RANGE", checkRange);
    }
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  private String invocations(Path repo) throws Exception {
    Path f = repo.resolve("gradlew-invocations");
    return Files.exists(f) ? Files.readString(f) : "";
  }

  private Path prepared(Path dir, String reportJson) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    fixture(repo, "format", reportJson);
    commit(repo);
    stageProductionChange(repo, "format");
    return repo;
  }

  /** Four of five killed is 80.0%; three of five is 60.0% and is refused. */
  @Test
  void failsWhenSurvivorsPutTheScoreBelowTheEightyPercentFloor(@TempDir Path dir)
      throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "SURVIVED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::35::MATH#0", "NO_COVERAGE")));

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("60.0").contains("80");
    assertThat(out).as("a survivor must be NAMED, or it cannot be killed%n%s", out)
        .contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1")
        .contains("io.github.huyz0.os.biningester.X::f()J::35::MATH#0");
  }

  /**
   * ⚠️ EXACTLY on the floor. 80% is a minimum, so four of five killed PASSES;
   * no other fixture lands on 80.0, so flipping {@code <} to {@code <=} would
   * otherwise survive every test.
   */
  @Test
  void exactlyEightyPercentKilledPasses(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "TIMED_OUT"),
        mutant("io.github.huyz0.os.biningester.X::f()J::35::MATH#0", "SURVIVED")));

    String out = gate(repo);

    assertThat(out).as("80.0 is AT the floor, not below -- and TIMED_OUT is detected%n%s", out)
        .startsWith("0");
    assertThat(out).as(out).contains("80.0");
  }

  /**
   * ⚠️ A PASSING score still names its survivors. 80% with one survivor, or
   * 85% with two, is a change no test noticed sitting under a number that
   * clears a threshold -- and printing the survivors only on failure made
   * exactly that case silent. The floor decides the exit status; the list is
   * the finding either way.
   */
  @Test
  void survivorsAreNamedEvenWhenTheScoreClearsTheFloor(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "TIMED_OUT"),
        mutant("io.github.huyz0.os.biningester.X::f()J::35::MATH#0", "SURVIVED")));

    String out = gate(repo);

    assertThat(out).as(out).startsWith("0");
    assertThat(out).as("the survivor is the finding, not the percentage%n%s", out)
        .contains("io.github.huyz0.os.biningester.X::f()J::35::MATH#0").contains("WARN");
  }

  /**
   * ⚠️ MULTI-MODULE POOLING, which is the whole reason this gate reads the
   * reports rather than jzap's own {@code mutationScore}. Scoring only the
   * FIRST module -- {@code for m in modules[:1]} -- measurably survived a
   * suite whose every fixture had one module, and it would pass a change whose
   * survivors all live in the second: here :format is spotless and every
   * survivor is in :sequencer.
   */
  @Test
  void survivorsInASecondModulePullThePooledScoreBelowTheFloor(@TempDir Path dir)
      throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    module(repo, "sequencer");
    fixture(repo, "format", report(
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::34::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::34::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::35::MATH#0", "KILLED")));
    fixture(repo, "sequencer", report(
        mutant("io.github.huyz0.os.biningester.B::g()J::10::MATH#0", "SURVIVED"),
        mutant("io.github.huyz0.os.biningester.B::g()J::10::MATH#1", "SURVIVED"),
        mutant("io.github.huyz0.os.biningester.B::g()J::11::MATH#0", "SURVIVED"),
        mutant("io.github.huyz0.os.biningester.B::g()J::11::MATH#1", "SURVIVED"),
        mutant("io.github.huyz0.os.biningester.B::g()J::12::MATH#0", "SURVIVED")));
    commit(repo);
    stageProductionChange(repo, "format");
    stageProductionChange(repo, "sequencer");

    String out = gate(repo);

    assertThat(out).as("5 killed of 10 pooled is 50%%, not :format's 100%%%n%s", out)
        .startsWith("1");
    assertThat(out).as(out).contains("5 of 10").contains("50.0");
    assertThat(out).as("the survivors are all in the SECOND module%n%s", out)
        .contains("io.github.huyz0.os.biningester.B::g()J::12::MATH#0");
  }

  /**
   * ⚠️ WHICH TASKS ARE INVOKED, asserted rather than assumed. Replacing the
   * computed task list with {@code ":bogus:notATask"} survived a suite that
   * recorded the invocation and never read it. The fixture pins three separate
   * decisions at once: an UNCHANGED module is not built, a {@code src/main}
   * tree under no Gradle module is not turned into a task that does not exist,
   * and buildSrc is excluded (it is an included build whose own conventions
   * plugin is what APPLIES jzap, so it has no such task).
   */
  @Test
  void onlyTheChangedGradleModulesAreBuilt(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    module(repo, "sequencer");          // a module that does NOT change
    fixture(repo, "format", report(mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#0", "KILLED")));
    Files.createDirectories(repo.resolve("buildSrc/src/main/java/io/github/huyz0/os/biningester"));
    Files.writeString(repo.resolve("buildSrc/build.gradle.kts"), "// stub\n");
    Files.writeString(repo.resolve("buildSrc/src/main/java/io/github/huyz0/os/biningester/Y.java"), "class Y {}\n");
    Files.createDirectories(repo.resolve("notamodule/src/main/java/io/github/huyz0/os/biningester"));
    Files.writeString(repo.resolve("notamodule/src/main/java/io/github/huyz0/os/biningester/Z.java"), "class Z {}\n");
    commit(repo);
    stageProductionChange(repo, "format");
    Files.writeString(repo.resolve("buildSrc/src/main/java/io/github/huyz0/os/biningester/Y.java"),
        "class Y { long f() { return 2; } }\n");
    Files.writeString(repo.resolve("notamodule/src/main/java/io/github/huyz0/os/biningester/Z.java"),
        "class Z { long f() { return 3; } }\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("0");
    assertThat(invocations(repo))
        .as("exactly the changed Gradle modules, and nothing else%n%s", out)
        .contains("ARGS: :format:mutationTestDiff --console=plain -q")
        .doesNotContain(":sequencer:")
        .doesNotContain(":buildSrc:")
        .doesNotContain(":notamodule:");
  }

  /**
   * ⚠️ CHECK_RANGE reaches jzap as JZAP_FROM. Deleting the export survived the
   * whole suite, and the consequence is not a red build: in CI jzap would fall
   * back to its default {@code HEAD..-Local-} base, which in a fresh checkout
   * is EMPTY -- so every push would print NOT-MEASURED and look green while
   * measuring nothing. That is the failure mode this gate exists to refuse,
   * one layer up.
   */
  @Test
  void checkRangeIsHandedToJzapAsTheDiffBase(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    fixture(repo, "format", report(mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#0", "KILLED")));
    commit(repo);
    stageProductionChange(repo, "format");
    run(repo, "git commit -qm change");

    String out = gate(repo, "HEAD~1");

    assertThat(out).as("CHECK_RANGE selects the changed file too%n%s", out).startsWith("0");
    assertThat(invocations(repo)).as("without this, CI diffs nothing%n%s", out)
        .contains("JZAP_FROM: HEAD~1");
  }

  /**
   * ⚠️ Gradle's exit status is load-bearing. Ignoring it survived the whole
   * suite -- and it must fail even though a perfectly readable report from a
   * PREVIOUS run is sitting on disk scoring 100%, which is the state that
   * makes ignoring it dangerous rather than merely sloppy.
   */
  @Test
  void aMutationRunThatDoesNotCompleteIsAFailureAndScoresNoOldReport(@TempDir Path dir)
      throws Exception {
    Path repo = scratch(dir, "exit 1");
    module(repo, "format");
    staleReport(repo, "format", report(
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#1", "KILLED")));
    commit(repo);
    stageProductionChange(repo, "format");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("did not complete");
    assertThat(out).as("the previous diff's report must not be scored%n%s", out)
        .doesNotContain("100.0").doesNotContain("scorable mutant(s) killed");
  }

  /**
   * ⚠️ A PREVIOUS DIFF'S REPORT IS NOT THIS DIFF'S MEASUREMENT.
   * {@code mutationTestDiff}'s scope depends on git state that is not a
   * declared task input, so Gradle can report UP-TO-DATE and leave the old
   * report in place -- and a gate that merely RAN a task and then read
   * whatever file was there would score one change against another change's
   * mutants. Here the run succeeds and publishes nothing; the stale 100%
   * report must not be what the gate reports.
   */
  @Test
  void aStaleReportFromAPreviousDiffIsNotScored(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");                       // no fixture: the stub publishes nothing
    staleReport(repo, "format", report(
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.A::f()J::33::MATH#1", "KILLED")));
    commit(repo);
    stageProductionChange(repo, "format");

    String out = gate(repo);

    assertThat(out).as("a report this run did not produce is not a measurement%n%s", out)
        .startsWith("1");
    assertThat(out).as(out).contains("no jzap report").doesNotContain("100.0");
  }

  /**
   * ⚠️ The vacuous-pass case, and the reason this gate states its blind spot.
   * A production change whose changed lines carry no mutable code -- a javadoc
   * edit, a field rename, an added import -- produces a report with an empty
   * mutant list. That is NOT 0% and must not fail (it would refuse correct
   * work until the gate was switched off), and it is NOT ok either: the gate
   * measured nothing and says so, in the word the other threshold gate uses.
   */
  @Test
  void reportsNotMeasuredRatherThanOkWhenTheChangedLinesProduceNoMutants(@TempDir Path dir)
      throws Exception {
    Path repo = prepared(dir, report());

    String out = gate(repo);

    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).contains("NOT-MEASURED");
    assertThat(out).as("nothing was scored, so no score may be reported%n%s", out)
        .doesNotContain("scorable mutant(s) killed");
  }

  /**
   * ⚠️ A baseline that can swallow a survivor silently defeats the gate, so an
   * entry carries a REASON naming what unblocks it. A bare mutant key is a
   * permanent exemption wearing a ratchet's clothes (testing.md rule 7, the
   * same arity rule {@code baselines/coverage.txt} carries).
   */
  @Test
  void aBaselineEntryWithNoReasonIsRefused(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "SURVIVED")));
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/mutants.txt"),
        "# key  why\nio.github.huyz0.os.biningester.X::f()J::34::MATH#1\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("no reason");
  }

  /**
   * A recorded survivor with a reason does not fail the gate, is excluded from
   * both halves of the score, and is NAMED in the output -- an exemption
   * nobody sees is an exemption nobody removes.
   *
   * <p>⚠️ The second entry names a class this run did not mutate at all, which
   * is the ordinary case on a diff-scoped run and must stay silent. Without
   * it, the unused-entry check below could be written to fail every entry
   * outside the current diff and no test would notice.
   */
  @Test
  void aBaselinedSurvivorIsExcludedFromTheScoreAndNamed(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "SURVIVED")));
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/mutants.txt"),
        "io.github.huyz0.os.biningester.X::f()J::34::MATH#1  equivalent mutant on a defensive branch; M8.62\n"
        + "io.github.huyz0.os.biningester.Elsewhere::h()V::9::MATH#0  a module this diff never touched; M8.62\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as("50%% unbaselined would fail; baselined it is 1/1%n%s", out)
        .startsWith("0");
    assertThat(out).as(out).contains("BASELINED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1")
        .contains("M8.62");
    assertThat(out).as("the excluded mutant must not be counted either way%n%s", out)
        .contains("100.0");
    assertThat(out).as("an entry outside this diff is not an unused one%n%s", out)
        .doesNotContain("UNUSED");
  }

  /**
   * ⚠️ The ratchet's teeth. Once a test kills a recorded survivor, the entry
   * stops describing anything and must go -- otherwise the file grows into a
   * place where a FUTURE survivor at the same key is swallowed without anyone
   * deciding to swallow it.
   */
  @Test
  void aBaselineEntryForAMutantThatIsNowKilledIsRefusedAsStale(@TempDir Path dir)
      throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "KILLED")));
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/mutants.txt"),
        "io.github.huyz0.os.biningester.X::f()J::34::MATH#1  equivalent mutant on a defensive branch; M8.62\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as("the score is 100%% and the gate STILL refuses%n%s", out).startsWith("1");
    assertThat(out).as(out).contains("STALE").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /**
   * ⚠️ The other way an entry stops describing anything, and the one the
   * staleness check alone cannot see: the METHOD was edited, so every line
   * number and ordinal in it moved and the recorded key matches no mutant at
   * all. Kept, it is a standing excuse for whatever lands on that key next.
   * The method IS mutated this run, which is what distinguishes it from an
   * entry that is simply outside the diff.
   */
  @Test
  void aBaselineEntryWhoseKeyHasMovedIsRefusedAsUnused(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#1", "KILLED")));
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/mutants.txt"),
        "io.github.huyz0.os.biningester.X::f()J::34::MATH#1  equivalent mutant on a defensive branch; M8.62\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as("the score is 100%% and the gate STILL refuses%n%s", out).startsWith("1");
    assertThat(out).as(out).contains("UNUSED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /**
   * A module whose production sources changed and that produced no report is a
   * missing MEASUREMENT, not a module with nothing to measure. Reported ok, a
   * single broken Gradle task would make this gate green forever.
   */
  @Test
  void aChangedModuleWithNoReportIsAFailureNotASkip(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    commit(repo);
    stageProductionChange(repo, "format");   // no fixture, so the stub writes no report

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("format").contains("no jzap report");
  }

  /**
   * ⚠️ A change with no production Java pays NO Gradle startup, and says what
   * it did rather than reporting a score it did not compute. Without this the
   * gate would run a mutation pass on every docs commit -- and a gate people
   * wait for is a gate people disable (build.md § Gate scope).
   */
  @Test
  void aChangeWithNoProductionJavaRunsNoGradleAndScoresNothing(@TempDir Path dir)
      throws Exception {
    Path repo = scratch(dir);
    module(repo, "format");
    fixture(repo, "format", report(mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "SURVIVED")));
    commit(repo);
    Files.writeString(repo.resolve("README.md"), "docs only\n");
    run(repo, "git add -A");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).contains("nothing to mutate");
    assertThat(out).as("a score it did not compute must not be reported%n%s", out)
        .doesNotContain("scorable mutant(s) killed");
    assertThat(Files.exists(repo.resolve("gradlew-invocations")))
        .as("gradlew must not have been invoked at all").isFalse();
  }

  /**
   * ⚠️ {@code NON_VIABLE} and {@code RUN_ERROR} are mutants the engine could
   * not score, not mutants the tests failed to kill, and jzap's own
   * {@code MutantStatus.isScored()} excludes them. Counted into the
   * denominator, three killed out of three viable would read 60.0% and refuse
   * a perfectly-tested change -- a false refusal, which is how a gate gets
   * switched off.
   */
  @Test
  void unscorableMutantsAreOutsideBothHalvesOfTheScore(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, report(
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#1", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#0", "KILLED"),
        mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#1", "NON_VIABLE"),
        mutant("io.github.huyz0.os.biningester.X::f()J::35::MATH#0", "RUN_ERROR")));

    String out = gate(repo);

    assertThat(out).as("3 of 3 scorable is 100%%, not 3 of 5%n%s", out).startsWith("0");
    assertThat(out).as(out).contains("100.0").doesNotContain("60.0");
  }

  /**
   * A report that cannot be parsed is a measurement that did not happen. jzap
   * writing a truncated file -- a killed build, a full disk -- must not read
   * as an absent one, and must never reach the score as zero mutants.
   */
  @Test
  void anUnreadableReportIsAFailureWithAReason(@TempDir Path dir) throws Exception {
    Path repo = prepared(dir, "{\"mutants\": [\n");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("unreadable");
    assertThat(out).as("an unparseable report is not an empty one%n%s", out)
        .doesNotContain("NOT-MEASURED");
  }
}
