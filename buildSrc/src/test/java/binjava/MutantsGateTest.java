// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * check-mutants.sh / mutants.py, through the production door (M0.14).
 *
 * <p>testing.md rule 9: 80% killed on changed code. The characteristic failure
 * of a threshold gate is not a wrong threshold -- it is reporting success while
 * measuring nothing, which is why the cases below are weighted toward
 * "measured nothing" rather than toward arithmetic.
 *
 * <p>⚠️ PIT ITSELF IS NOT RUN HERE. The gate's job is to SCOPE mutation to the
 * diff, invoke the build, and judge the report; PIT's job is to generate
 * mutants. Running the real thing would put a 15-second JVM in every case and
 * would test pitest rather than this gate, so `gradlew` is stubbed and the
 * report is a fixture.
 */
class MutantsGateTest {

  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-mutants.sh", "mutants.py", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("baselines"));
    Files.writeString(dir.resolve("baselines").resolve("mutants.txt"), "# survivors\n");
    // ⚠️ THE STUB REGENERATES THE REPORT, like CoverageGateTest's. The gate
    // DELETES any existing mutations.xml before invoking the build, so a stale
    // report from an earlier run cannot be judged as if it described the code
    // on disk now -- and a stub that merely exited 0 would make that deletion
    // invisible, leaving the anti-stale step with no regression protection.
    stubGradlew(dir, "for f in \"$(dirname \"$0\")\"/*/pit-fixture.xml; do\n"
        + "  [ -f \"$f\" ] || continue\n"
        + "  d=\"$(dirname \"$f\")/build/reports/pitest\"\n"
        + "  mkdir -p \"$d\" && cp \"$f\" \"$d/mutations.xml\"\n"
        + "done\nexit 0");
    return dir;
  }

  private void stubGradlew(Path dir, String body) throws Exception {
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "echo \"$@\" >> \"$(dirname \"$0\")/gradlew-invocations\"\n"
            + body + "\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
  }

  /** A production class in {@code module}, staged so the gate sees it changed. */
  private void productionClass(Path repo, String module, String cls) throws Exception {
    Path src = repo.resolve(module).resolve("src/main/java/binjava/" + module.replace('-', '_'));
    Files.createDirectories(src);
    Files.writeString(src.resolve(cls + ".java"), "class " + cls + " { }\n");
  }

  /** A PIT report naming each entry as {@code Class:status}. */
  private void pitReport(Path repo, String module, String... mutants) throws Exception {
    Files.createDirectories(repo.resolve(module));
    StringBuilder b = new StringBuilder("<?xml version=\"1.0\"?>\n<mutations>\n");
    for (String m : mutants) {
      String[] parts = m.split(":");
      b.append("<mutation detected='")
          .append("KILLED".equals(parts[1]))
          .append("' status='")
          .append(parts[1])
          .append("'><mutatedClass>binjava.")
          .append(module.replace('-', '_'))
          .append('.')
          .append(parts[0])
          .append("</mutatedClass><lineNumber>7</lineNumber>")
          .append("<mutator>org.pitest.mutationtest.engine.gregor.mutators.")
          .append("ConditionalsBoundaryMutator</mutator></mutation>\n");
    }
    b.append("</mutations>\n");
    Files.writeString(repo.resolve(module).resolve("pit-fixture.xml"), b.toString());
  }

  private void commit(Path repo) throws Exception {
    run(repo, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
  }

  private void run(Path repo, String cmd) throws Exception {
    Process p = new ProcessBuilder("bash", "-c", "set -o pipefail; " + cmd)
        .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
  }

  private String gate(Path repo) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder("bash", "scripts/check-mutants.sh").directory(repo.toFile());
    pb.environment().put("GATE_SCOPE", "full");
    for (String v : List.of("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "CHECK_RANGE")) {
      pb.environment().remove(v);
    }
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  /** ⚠️ The case the whole gate rests on: nothing to measure is NOT success. */
  @Test
  void reportsNotMeasuredRatherThanOkWhenNoProductionClassChanged(@TempDir Path dir)
      throws Exception {
    Path repo = scratch(dir);
    Files.writeString(repo.resolve("README.md"), "docs only\n");
    commit(repo);

    String out = gate(repo);

    assertThat(out).as(out).doesNotContain("[32mok");
    assertThat(out).as(out).containsIgnoringCase("not measured");
  }

  /**
   * ⚠️ A BUILD THAT PRODUCED NO REPORT IS A FAILURE, NOT A SKIP. This is the
   * shape that makes a threshold gate green forever: the run fails, no XML is
   * written, and a gate that treats "no report" as "nothing to judge" reports
   * success having measured nothing.
   */
  @Test
  void aMissingReportAfterAnInvocationFAILS(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    productionClass(repo, "format", "CommitDelta");
    commit(repo);

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).containsIgnoringCase("no mutation report");
  }

  /** The floor is 80%: 4 of 5 killed is exactly 80 and passes. */
  @Test
  void exactlyTheFloorPASSES(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    productionClass(repo, "format", "CommitDelta");
    commit(repo);
    pitReport(repo, "format", "CommitDelta:KILLED", "CommitDelta:KILLED",
        "CommitDelta:KILLED", "CommitDelta:KILLED", "CommitDelta:SURVIVED");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).contains("80.0%");
  }

  /** ⚠️ And one below it fails -- with the survivors NAMED, not just counted. */
  @Test
  void belowTheFloorFAILSAndNamesTheSurvivors(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    productionClass(repo, "format", "CommitDelta");
    commit(repo);
    pitReport(repo, "format", "CommitDelta:KILLED", "CommitDelta:SURVIVED",
        "CommitDelta:SURVIVED");

    String out = gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("CommitDelta");
    assertThat(out).as(out).containsIgnoringCase("survived");
  }

  /**
   * ⚠️ SCOPED TO THE DIFF. A mutant in a class this change did not touch must
   * not count in either direction -- neither to rescue a weak diff nor to sink
   * a good one. Without this the gate measures the module, and the 80% floor
   * becomes a statement about the whole codebase's history.
   */
  @Test
  void mutantsInUNCHANGEDClassesAreIGNORED(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    productionClass(repo, "format", "Untouched");
    commit(repo);
    // Only CommitDelta is added by the change under test.
    productionClass(repo, "format", "CommitDelta");
    run(repo, "git add -A");
    pitReport(repo, "format", "CommitDelta:KILLED", "CommitDelta:KILLED",
        "CommitDelta:KILLED", "CommitDelta:KILLED",
        "Untouched:SURVIVED", "Untouched:SURVIVED", "Untouched:SURVIVED");

    ProcessBuilder pb =
        new ProcessBuilder("bash", "scripts/check-mutants.sh").directory(repo.toFile());
    pb.environment().remove("GATE_SCOPE");
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = p.waitFor() + "\n" + new String(p.getInputStream().readAllBytes());

    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).contains("100.0%");
  }

  /** The invocation must actually scope PIT, or the gate pays for the module. */
  @Test
  void theBuildIsInvokedWithTheCHANGEDClassesAsTargets(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    productionClass(repo, "format", "CommitDelta");
    commit(repo);
    pitReport(repo, "format", "CommitDelta:KILLED");

    gate(repo);

    String invocations = Files.readString(repo.resolve("gradlew-invocations"));
    assertThat(invocations).as(invocations).contains(":format:pitest");
    assertThat(invocations).as(invocations).contains("binjava.format.CommitDelta");
  }
}
