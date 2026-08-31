// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Which modules a change selects.
 *
 * The shared-build escalation — a change to buildSrc/ or gradle/ must select
 * EVERY module, because the conventions plugin injects dependencies into all of
 * them — was dead for a whole review round: `changed_files | grep -q ...` under
 * `set -o pipefail` returns 141, since grep -q exits at the first match and the
 * producer dies of SIGPIPE. The gate printed "ok no module changed" on the exact
 * change class its own comment calls the one it must catch.
 *
 * Nothing noticed because the branch had no seam. This is that seam.
 */
class ModuleSelectionTest {

  private static final String ALL =
      "binstore-spi binstore-backends format sequencer ingest http client plugin";

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        // a shared-build change selects everything
        "buildSrc/build.gradle.kts   | 8",
        "gradle/libs.versions.toml   | 8",
        "settings.gradle.kts         | 8",
        "gradle.properties           | 8",
        // a module change selects that module
        "format/README.md            | 1",
        // a docs-only change selects nothing
        "docs/internal/standards/x.md| 0",
        // ⚠️ Every case above passes exactly ONE path, so the loops that walk
        // "$@" never iterate and `for p in "${1:-}"` survived the whole suite.
        // These four pass several, and the interesting path is never first.
        //
        // two module directories -- kills `printf '%s\n' "$1"` in select_modules
        "format/README.md ingest/A.java             | 2",
        // shared build LAST -- kills `for p in "${1:-}"` in is_shared_build_change,
        // and kills reinstating the historical `changed_files | grep -qE` shape,
        // which under `set -o pipefail` returns 141 and silently never escalates
        "format/README.md buildSrc/build.gradle.kts | 8",
        // a module path after a docs path -- kills dropping the tail generally
        "docs/x.md format/README.md                 | 1",
        // same module twice plus a non-module dir -- `sort -u` must dedupe, and
        // an unknown top-level directory must not become a module
        "format/A.java format/B.java nope/C.java    | 1",
      })
  void selectsTheModulesAChangeCanAffect(String changed, int expected) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    // ⚠️ One argv element per path. Passing `changed.trim()` whole made "$@" a
    // SINGLE argument no matter how many paths the case named, so every loop
    // over "$@" iterated once and `for p in "${1:-}"` was structurally
    // untestable -- the reason WT15 survived a suite written to catch it.
    List<String> argv = new ArrayList<>(List.of("scripts/check-module.sh", "--select-modules", ALL));
    Arrays.stream(changed.trim().split("\\s+")).forEach(argv::add);
    Process p =
        new ProcessBuilder(argv)
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
    List<String> got = out.lines().map(String::trim).filter(l -> !l.isEmpty()).toList();
    assertThat(got).as("selected for %s: %s", changed, got).hasSize(expected);
    // A count is satisfied by selecting the WRONG module. Pin the names too.
    if (expected == 1) {
      assertThat(got).as("selected for %s", changed).containsExactly("format");
    } else if (expected == 2) {
      assertThat(got).as("selected for %s", changed).containsExactly("format", "ingest");
    } else if (expected == 8) {
      assertThat(got).as("selected for %s", changed).containsExactlyInAnyOrder(ALL.split(" "));
    }
  }
}
