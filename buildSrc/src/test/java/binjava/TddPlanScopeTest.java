// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Which Gradle task {@code tdd_scan.py} names for a test source path.
 *
 * <p>⚠️ {@code plan()} emitted the bare root task {@code test} for every source
 * outside {@code buildSrc/}. {@code ./gradlew test --tests <one class>} then
 * runs {@code test} in all eight modules at {@code org.gradle.parallel=true}.
 * A module with no test source is NO-SOURCE and is skipped — but a module that
 * has test sources and no match FAILS the build, and whether the owning module
 * wrote its JUnit XML before that failure is a scheduling race. Measured on
 * M1.0: {@code tdd-red.sh} recorded 2 of 7 ids, then 7 of 7 once scoped.
 *
 * <p>⚠️ An earlier draft of this class called {@code plan}, which resolves an id
 * to a file by globbing the live working tree. It named a class that was not in
 * the tree, so {@code plan} took its "cannot locate a test source" path and both
 * tests asserted against an ERROR message — identically before and after the
 * fix, so the red record proved nothing and deleting the fix survived. The rule
 * is a pure string transform and is now tested as one, over paths no on-disk
 * tree need contain (testing.md rule 1, non-negotiable 7).
 */
class TddPlanScopeTest {

  private static List<String> taskFor(String... sources) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    List<String> argv = new ArrayList<>(List.of("python3", "scripts/tdd_scan.py", "task-for"));
    argv.addAll(List.of(sources));
    Process p = new ProcessBuilder(argv).directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return List.of(out.strip().split("\n"));
  }

  @Test
  void eachModuleGetsItsOwnScopedTask() throws Exception {
    // ⚠️ TWO different modules. One would be satisfied by a hardcoded
    // ":ingest:test" -- the mutation that survived the first attempt at this
    // test -- so the module must be DERIVED from each path.
    assertThat(
            taskFor(
                "ingest/src/test/java/binjava/ingest/security/CredentialSourceTest.java",
                "format/src/test/java/binjava/format/SegmentTest.java",
                "buildSrc/src/test/java/binjava/CoverageGateTest.java"))
        .containsExactly(":ingest:test", ":format:test", "buildSrc:test");
    // ⚠️ buildSrc is folded in here rather than tested on its own because its
    // red is UNOBTAINABLE in isolation: tdd-red.sh resolves its OWN gradle task
    // through this function, so any mutation of the buildSrc branch breaks the
    // recorder that would witness it. Asserted here, where reverting the module
    // scoping already makes the case fail, it still kills the drop-the-branch
    // mutation -- that mutation emits ":buildSrc:test", which containsExactly
    // rejects.
  }

  @Test
  void theSourceSetNamesTheTaskNotJustTheModule() throws Exception {
    // ⚠️ check-tdd demands a red record for EVERY tier, so hardcoding `test`
    // would make an integrationTest or clusterTest id unrecordable.
    assertThat(
            taskFor(
                "ingest/src/integrationTest/java/binjava/ingest/FlushIT.java",
                "plugin/src/clusterTest/java/binjava/plugin/SearchIT.java"))
        .containsExactly(":ingest:integrationTest", ":plugin:clusterTest");
  }

  /**
   * ⚠️ The SHIPPED path. The three tests above exercise {@code task_for} through
   * the {@code task-for} CLI seam, but {@code tdd-red.sh} shells
   * {@code tdd_scan.py plan} and never touches that seam — so inlining the old
   * unscoped logic back into {@code plan()} leaves {@code task_for} a correct,
   * fully tested, DEAD function while the defect returns verbatim. That
   * mutation was green against every other test here.
   *
   * <p>⚠️ Runs against a synthetic tree, not this repository. {@code source_of}
   * globs {@code (*)/src/(*)/java/} relative to the working directory, so a real
   * id would tie this test to whatever sources happen to be checked out — which
   * is exactly how the first draft of this class ended up asserting against an
   * error message. Nothing here needs to exist in the repo.
   */
  @Test
  void planScopesTheShippedPathToTheOwningModule(@TempDir Path tree) throws Exception {
    Path scan = Path.of("../scripts/tdd_scan.py").toAbsolutePath().normalize();
    Files.createDirectories(tree.resolve("ingest/src/test/java/binjava/ingest"));
    Files.writeString(tree.resolve("ingest/src/test/java/binjava/ingest/FlushTest.java"), "");
    Files.createDirectories(tree.resolve("buildSrc/src/test/java/binjava"));
    Files.writeString(tree.resolve("buildSrc/src/test/java/binjava/GateTest.java"), "");

    Process p =
        new ProcessBuilder(
                "python3",
                scan.toString(),
                "plan",
                "binjava.ingest.FlushTest#a",
                "binjava.GateTest#b")
            .directory(tree.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();

    assertThat(out.strip().split("\n"))
        .as("plan() is what tdd-red.sh runs; it must emit scoped tasks\n%s", out)
        .containsExactly(
            ":ingest:test binjava.ingest.FlushTest.a", "buildSrc:test binjava.GateTest.b");
  }

  @Test
  void aNonTestSourceSetHasNoTask() throws Exception {
    assertThat(taskFor("ingest/src/main/java/binjava/ingest/Ingest.java"))
        .as("production code is not runnable as a test")
        .containsExactly("none");
  }
}
