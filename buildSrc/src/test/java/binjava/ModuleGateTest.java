// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The gate's PRODUCTION door — {@code check-module.sh} with no flag.
 *
 * <p>Every other suite enters through a test-only flag, so the gate's own
 * consumption of {@code plan()} was entered by nothing. Review demonstrated the
 * cost twice: mutations confined to those few lines left all 38 tests green
 * while the gate printed {@code ok no module changed} for every commit, which
 * is the single false green this file exists to prevent.
 *
 * <p>A scratch git repository is the only way to reach it, because the gate
 * derives its scope from {@code git diff --cached} and its orphan candidates
 * from {@code git ls-files} — neither of which can be faked in the real tree
 * without staging into it.
 *
 * <p>⚠️ {@code gradlew} is a stub, so this constrains SELECTION only — which
 * modules the gate decides to examine. The {@code rule5 tested N} counters
 * prove each rule's branch was ENTERED, never that the predicate inside it can
 * still refuse anything: swapping {@code HTTP_GROUPS} and {@code CLOUD_GROUPS}
 * leaves the summary byte-identical with both rules inert. <b>Nothing in this repository asserts
 * architecture.md rules 2, 4 or 5.</b> Emptying {@code HTTP_GROUPS}, reducing
 * {@code for conf in runtimeClasspath compileClasspath} to the runtime
 * configuration alone, and disabling the rule-4 {@code depends on http} check
 * each leave every test green — and green against the real tree too, because
 * no module violates a rule today, so the suite cannot tell an enforcing gate
 * from an inert one.
 *
 * <p>This matters more than it looks: this commit is what puts the hook on
 * every developer's {@code git commit} under the name "each module stays
 * inside its dependency surface". Half of that sentence is enforced and
 * tested; the other half is enforced and untested. Closing it needs a module
 * fixture that actually violates each rule — M0.23, deliberately not folded in
 * here, because the enforcement half belongs to M0.5's gate rather than to
 * wiring it up.
 */
class ModuleGateTest {

  private Path repo;

  private void sh(String script) throws Exception {
    Process p =
        new ProcessBuilder("bash", "-c", script)
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** Builds a throwaway repo carrying only what the gate reads. */
  private void scratch(Path dir) throws Exception {
    repo = dir;
    Path src = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-module.sh", "lib.sh")) {
      Files.copy(src.resolve("scripts").resolve(f), dir.resolve("scripts").resolve(f));
      dir.resolve("scripts").resolve(f).toFile().setExecutable(true);
    }
    // A stub: the gate only needs SOME dependency output to apply rule 5 to.
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "echo \"$@\" >> \"$(dirname \"$0\")/gradlew-invocations\"\n"
            + "echo '+--- project :alpha'\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
    Files.writeString(
        dir.resolve("settings.gradle.kts"), "include(\n  \"alpha\",\n  \"beta\",\n)\n");
    for (String m : List.of("alpha", "beta")) {
      Files.createDirectories(dir.resolve(m));
      Files.writeString(dir.resolve(m).resolve("build.gradle.kts"), "// stub\n");
    }
    Files.createDirectories(dir.resolve("docs"));
    Files.writeString(dir.resolve("docs").resolve("x.md"), "doc\n");
    Files.createDirectories(dir.resolve(".harness"));
    sh("git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
  }

  private String gate(String stageCmd) throws Exception {
    return gate(stageCmd, null, new String[0]);
  }

  /** @param scope value for GATE_SCOPE, or null to leave it unset (delta). */
  private String gate(String stageCmd, String scope, String... args) throws Exception {
    if (stageCmd != null) {
      sh(stageCmd);
    }
    List<String> argv = new ArrayList<>(List.of("scripts/check-module.sh"));
    argv.addAll(List.of(args));
    ProcessBuilder pb = new ProcessBuilder(argv).directory(repo.toFile());
    // ⚠️ CLEARED, not merely unset. These leak in from the developer's shell and
    // from CI, and the gate derives its whole scope from them: under
    // GATE_SCOPE=full + CHECK_RANGE=<base> -- the environment this commit's own
    // AGENTS.md row tells CI to use -- three scenarios failed on correct code,
    // and aDocAlongsideASharedBuildFileStillEscalates PASSED with the SC2046
    // mutation applied, which is the single defect it exists to kill.
    pb.environment().remove("GATE_SCOPE");
    pb.environment().remove("CHECK_RANGE");
    if (scope != null) {
      pb.environment().put("GATE_SCOPE", scope);
    }
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  /**
   * ⚠️ The gate has THREE selection modes and every scenario above enters only
   * the delta path. {@code elif [ "$GATE_SCOPE" = "full" ]; then} → {@code elif
   * false; then} survived all 55 tests: with nothing staged, CI would print
   * {@code ok no module changed}, exit 0 and make zero Gradle calls — the
   * vacuous green that the AGENTS.md row added by THIS commit says
   * {@code GATE_SCOPE=full} exists to prevent.
   */
  @Test
  void fullScopeExaminesEveryModuleEvenWithNothingStaged(@TempDir Path dir) throws Exception {
    scratch(dir);
    String out = gate(null, "full");
    assertThat(out).startsWith("0").contains("2 module(s) checked").contains("GATE_SCOPE=full");
    // ⚠️ The counter must COUNT, not restate a literal. `alpha`/`beta` appear in
    // neither rule list, so the honest answer here is zero -- while the real
    // tree reports 4 and 2 (RuleListTest). Hardcoding either number to satisfy
    // one of those assertions fails the other.
    assertThat(out).as(out).contains("rule5 tested 0, rule2 tested 0");
    assertThat(out).as(out).doesNotContain("no module changed");
    assertThat(repo.resolve("gradlew-invocations"))
        .as("full scope must actually build; a green that ran nothing is the bug")
        .exists();
  }

  /** The named-module door the header documents, entered by nothing until now. */
  @Test
  void modulesNamedOnTheCommandLineAreTheOnesExamined(@TempDir Path dir) throws Exception {
    scratch(dir);
    String out = gate(null, null, "beta");
    assertThat(out).startsWith("0").contains("1 module(s) checked").contains("named on the command line");
    assertThat(Files.readString(repo.resolve("gradlew-invocations")))
        .as("the NAMED module must be the one built")
        .contains(":beta:")
        .doesNotContain(":alpha:");
  }

  @Test
  void docsOnlyChangeSelectsNoModule(@TempDir Path dir) throws Exception {
    scratch(dir);
    assertThat(gate("echo more >> docs/x.md && git add docs/x.md"))
        .startsWith("0")
        .contains("no module changed");
  }

  @Test
  void aModuleChangeSelectsThatModuleOnly(@TempDir Path dir) throws Exception {
    scratch(dir);
    assertThat(gate("echo x >> alpha/build.gradle.kts && git add alpha/build.gradle.kts"))
        .startsWith("0")
        .contains("1 module(s) checked");
    // ⚠️ The POSITIVE half of the drift-halt assertion. Without it a stub that
    // never wrote this file would make `doesNotExist` in the ghost case pass
    // for the wrong reason -- an absence proves nothing unless the presence is
    // also observed.
    assertThat(repo.resolve("gradlew-invocations"))
        .as("the stub must record invocations, or asserting their absence is vacuous")
        .exists();
  }

  /**
   * The mutation that produced the false green: a shared-build change must
   * escalate to every module, because a shared build file can put a dependency
   * on a module's classpath without naming that module.
   */
  @Test
  void aSharedBuildChangeEscalatesToEveryModule(@TempDir Path dir) throws Exception {
    scratch(dir);
    assertThat(gate("echo x >> settings.gradle.kts && git add settings.gradle.kts"))
        .startsWith("0")
        .contains("2 module(s) checked")
        .contains("the shared build changed");
  }

  /**
   * ⚠️ Every scenario above stages exactly ONE file, so the gate's own call
   * never passed plan() more than one path. Quoting it as {@code plan
   * "$(changed_files)"} — precisely what shellcheck SC2046 advises — then makes
   * this commit print {@code ok no module changed}, because the two paths
   * arrive as a single argument matching no module directory and no shared
   * build file. That is the false green this whole file exists to prevent, and
   * it survived until a scenario staged two files with the deciding one second.
   */
  @Test
  void aDocAlongsideASharedBuildFileStillEscalates(@TempDir Path dir) throws Exception {
    scratch(dir);
    String out =
        gate("echo more >> docs/x.md && echo x >> settings.gradle.kts"
            + " && git add docs/x.md settings.gradle.kts");
    assertThat(out).startsWith("0").contains("2 module(s) checked");
    assertThat(out).as(out).doesNotContain("no module changed");
  }

  /**
   * A module whose build fails must fail the gate. {@code if ! ./gradlew
   * ":$m:build" ...} → {@code if ! true ./gradlew ...} otherwise survives:
   * every module "passes" without being built.
   */
  @Test
  void aModuleThatFailsToBuildFailsTheGate(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "echo \"$@\" >> \"$(dirname \"$0\")/gradlew-invocations\"\n"
            + "case \"$1\" in *:build) echo 'compile error' >&2; exit 1 ;; esac\n"
            + "echo '+--- project :alpha'\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
    assertThat(gate("echo x >> alpha/build.gradle.kts && git add alpha/build.gradle.kts"))
        .startsWith("1")
        .contains("build failed");
  }

  /**
   * ⚠️ An EMPTY dependency report must not read as compliance. The gate's own
   * comment says the exit status is checked because "a failed resolution, a
   * daemon killed under the 512 MiB cap, and a typo'd module name all looked
   * identical to compliance" — but a stub that exits 0 while printing nothing
   * reaches the same place, and {@code [ -n "$one" ] || ...} → {@code true}
   * survived every test.
   */
  @Test
  void anEmptyDependencyReportIsNotCompliance(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("gradlew"),
        "#!/usr/bin/env bash\n"
            + "echo \"$@\" >> \"$(dirname \"$0\")/gradlew-invocations\"\n"
            + "case \"$1\" in *:dependencies) exit 0 ;; esac\n"
            + "echo '+--- project :alpha'\n");
    dir.resolve("gradlew").toFile().setExecutable(true);
    String out = gate("echo x >> alpha/build.gradle.kts && git add alpha/build.gradle.kts");
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).doesNotContain("module(s) checked");
    // ⚠️ The REASON, not just the exit code. Without it, replacing the
    // `bad_conf` assignment with a bare `fail` still passes, while the module
    // is counted as checked and both rules grep an empty report.
    assertThat(out).as(out).contains("could not resolve");
  }

  /**
   * ⚠️ The counter must COUNT. Asserting 0 in a fixture with no rule-bearing
   * module and 4 in the real tree still let {@code applied_r5=$((applied_r5+1))}
   * become {@code applied_r5=4}: the branch never runs for alpha/beta, so zero
   * survives, and the real tree happens to equal the literal. A fixture holding
   * exactly ONE module that a rule names is what separates counting from
   * restating — the branch runs once and must report one.
   */
  @Test
  void theRuleCounterCountsRatherThanRestates(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("settings.gradle.kts"),
        "include(\n  \"alpha\",\n  \"format\",\n  \"binstore-spi\",\n  \"sequencer\",\n"
            + "  \"ingest\",\n  \"client\",\n)\n");
    for (String m : List.of("format", "binstore-spi", "sequencer", "ingest", "client")) {
      Files.createDirectories(dir.resolve(m));
      Files.writeString(dir.resolve(m).resolve("build.gradle.kts"), "// stub\n");
    }
    // `beta` leaves settings here, so its build file must go too -- otherwise
    // the orphan check fires, correctly, and masks what this test is asserting.
    Files.delete(dir.resolve("beta").resolve("build.gradle.kts"));
    Files.delete(dir.resolve("beta"));
    String out = gate("git add -A", "full");
    assertThat(out).startsWith("0").contains("6 module(s) checked");
    // ⚠️ ALL FOUR NO_HTTP modules are named here. With only `format` and
    // `ingest`, narrowing the loop's expansion to `case " format ingest " in`
    // left rule 5 inert for `binstore-spi` and `sequencer` with every test
    // green -- and `--print-rules` went on reporting the strong list, so
    // RuleListTest was satisfied by a copy. The count is the only witness that
    // the expansion the loop uses is the one the rule names.
    //
    // Counts across the suite: rule 5 sees 0, 1 and 4; rule 2 sees 0, 1 and 2.
    // No constant satisfies either pair, and no narrowing survives the 4.
    assertThat(out).as(out).contains("rule5 tested 4, rule2 tested 1");
  }

  /**
   * ⚠️ The SECOND counting fixture, and it is not redundant. With only one
   * non-zero expectation per rule, {@code applied_r5=$((applied_r5+1))} →
   * {@code applied_r5=2} survives: the assignment is INSIDE the branch, so the
   * scenarios expecting 0 are satisfied by the branch never running, and the
   * single non-zero case is satisfied by the constant. Two different non-zero
   * counts per rule leave no constant that fits — here rule 5 sees 1 and rule 2
   * sees 2, against 2 and 1 in the sibling.
   */
  @Test
  void theRuleCountersAreNotSatisfiedByAnyConstant(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("settings.gradle.kts"),
        "include(\n  \"alpha\",\n  \"format\",\n  \"client\",\n  \"plugin\",\n)\n");
    for (String m : List.of("format", "client", "plugin")) {
      Files.createDirectories(dir.resolve(m));
      Files.writeString(dir.resolve(m).resolve("build.gradle.kts"), "// stub\n");
    }
    Files.delete(dir.resolve("beta").resolve("build.gradle.kts"));
    Files.delete(dir.resolve("beta"));
    String out = gate("git add -A", "full");
    assertThat(out).startsWith("0").contains("4 module(s) checked");
    // `format` is in NO_HTTP; `client` and `plugin` are both in NO_CLOUD.
    assertThat(out).as(out).contains("rule5 tested 1, rule2 tested 2");
  }

  /**
   * W12. An untracked build file — a reference clone sitting at the repo root —
   * must not fail anything. A filesystem glob here blocked EVERY commit.
   */
  @Test
  void anUntrackedOrphanIsIgnored(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.createDirectories(dir.resolve("RefClone"));
    Files.writeString(dir.resolve("RefClone").resolve("build.gradle.kts"), "// not ours\n");
    assertThat(gate("echo more >> docs/x.md && git add docs/x.md")).startsWith("0");
  }

  /** But a TRACKED orphan is in the commit, and would be silently unchecked. */
  @Test
  void aTrackedOrphanFails(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.createDirectories(dir.resolve("sneaky"));
    Files.writeString(dir.resolve("sneaky").resolve("build.gradle.kts"), "// unchecked\n");
    assertThat(gate("git add sneaky/build.gradle.kts"))
        .startsWith("1")
        .contains("not in settings");
  }

  /** A module named in settings with no build file must halt the gate. */
  @Test
  void aGhostModuleInSettingsFails(@TempDir Path dir) throws Exception {
    scratch(dir);
    // ⚠️ The directory EXISTS and only the build file is missing. With no
    // directory at all, weakening `[ -f "$root/$m/build.gradle.kts" ]` to
    // `[ -d "$root/$m" ]` passes -- the check would then be satisfied by a
    // module that Gradle cannot build.
    Files.createDirectories(dir.resolve("ghost"));
    Files.writeString(dir.resolve("settings.gradle.kts"), "include(\n  \"alpha\",\n  \"ghost\",\n)\n");
    String out = gate("git add settings.gradle.kts");
    assertThat(out).startsWith("1").contains("ghost");
    // ⚠️ Exit 1 alone is satisfied by a gate that reports drift and then builds
    // anyway: deleting the early halt left the verdict AND the printed output
    // identical, because the "N module(s) checked" line is suppressed once
    // anything has failed. The only observable difference is whether Gradle ran
    // at all, so that is what this asserts. Drift must halt BEFORE building --
    // a tree whose settings and directories disagree produces build errors that
    // are noise, not signal.
    assertThat(repo.resolve("gradlew-invocations"))
        .as("drift must halt before any module is built")
        .doesNotExist();
  }
}
