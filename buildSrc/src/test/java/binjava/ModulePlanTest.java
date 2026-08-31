// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The gate's OWN composition — the module list it derives, why it selected what
 * it did, and what it will build — for a given set of changed paths.
 *
 * Three test-only seams existed before this and all three were tested, while the
 * production path was entered by nothing. Review demonstrated the cost: it
 * restored the SIGPIPE defect on the gate side only, and the entire suite stayed
 * green while the gate printed "ok no module changed" on a shared-build change,
 * which is the one thing it exists to catch. Testing the helpers is not testing
 * the gate.
 *
 * Assertions are on exact names, not counts — a count passes when the selection
 * builds the wrong module.
 */
class ModulePlanTest {

  private static final String EIGHT =
      "binstore-spi binstore-backends format sequencer ingest http client plugin";

  private static String plan(String... args) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    java.util.List<String> argv = new java.util.ArrayList<>(List.of("scripts/check-module.sh", "--plan"));
    argv.addAll(List.of(args));
    Process p =
        new ProcessBuilder(argv).directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return out;
  }

  /**
   * plan() must PROPAGATE the drift result, not merely compute it. The gate
   * halts on {@code PLAN_DRIFT != ok}; with nothing asserting the fail branch,
   * hardcoding it to {@code ok} passed every test while real drift — a module
   * in settings.gradle.kts with no build.gradle.kts — would have been reported
   * and then ignored.
   */
  @Test
  void planPropagatesDriftFailure() throws Exception {
    String bad = plan("--root", "buildSrc/src/test/resources/drift-fixtures/missing-build-file", "docs/x.md");
    assertThat(bad.lines().filter(l -> l.startsWith("DRIFT=")).toList())
        .as(bad)
        .containsExactly("DRIFT=fail checked=2");

    String good = plan("--root", "buildSrc/src/test/resources/drift-fixtures/clean", "docs/x.md");
    assertThat(good.lines().filter(l -> l.startsWith("DRIFT=")).toList())
        .as(good)
        .containsExactly("DRIFT=ok checked=2");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        // the shared build reaches every module's classpath
        "buildSrc/build.gradle.kts    | shared-build | ALL",
        "gradle/libs.versions.toml    | shared-build | ALL",
        "settings.gradle.kts          | shared-build | ALL",
        "gradle.properties            | shared-build | ALL",
        // a module directory selects that module, by name
        "format/README.md             | module-dirs  | format",
        "plugin/build.gradle.kts      | module-dirs  | plugin",
        // nothing outside a module selects nothing
        "docs/internal/product/x.md   | module-dirs  |",
        // ⚠️ MULTI-PATH, and the deciding path is never first. Every row above
        // passes one path, so `select_modules "$PLAN_ALL" "${1:-}"` inside
        // plan() -- and quoting the call site as `plan "$(changed_files)"`,
        // which is exactly what shellcheck SC2046 advises -- both survived.
        // The second makes a commit staging a doc alongside settings.gradle.kts
        // print "ok no module changed": the one false green this gate exists
        // to prevent.
        "docs/x.md settings.gradle.kts| shared-build | ALL",
        "docs/x.md format/README.md   | module-dirs  | format",
        "format/A.java plugin/B.java  | module-dirs  | format plugin",
      })
  void planNamesTheModulesItWillBuild(String changed, String why, String expected)
      throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    // ⚠️ One argv element per path. Passing the row whole made "$@" a SINGLE
    // element, so plan() never received more than one path and dropping the
    // tail was undetectable -- the same defect as ModuleSelectionTest had, one
    // layer up, and `--select-modules` cannot compensate because it bypasses
    // plan() entirely.
    java.util.List<String> argv =
        new java.util.ArrayList<>(List.of("scripts/check-module.sh", "--plan"));
    for (String one : changed.trim().split("\\s+")) argv.add(one);
    Process p =
        new ProcessBuilder(argv)
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();

    // ALL must come from settings.gradle.kts, not a literal restated in the gate.
    assertThat(out).as(out).contains("ALL=" + EIGHT);
    assertThat(out).as(out).contains("WHY=" + why.trim());
    // The gate must RUN the drift check, not merely define it.
    assertThat(out).as(out).contains("DRIFT=ok checked=8");
    String want = expected == null ? "" : expected.trim();
    // Whole line, anchored. `contains("MODULES=")` is true of the unconditional
    // echo even when the selection is wrong, so the empty row asserted nothing.
    assertThat(out.lines().filter(l -> l.startsWith("MODULES=")).toList())
        .as(out)
        .containsExactly("MODULES=" + ("ALL".equals(want) ? EIGHT : want));
  }
}
