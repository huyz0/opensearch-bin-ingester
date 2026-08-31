// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code NO_HTTP} and {@code NO_CLOUD} in check-module.sh name modules, and
 * nothing tied those names to settings.gradle.kts. Rename a module and its rule
 * silently stops applying: the gate goes on printing {@code ok} while enforcing
 * strictly less, which is the failure mode this whole file exists to prevent.
 *
 * <p>It is asserted here rather than inside the gate because the gate must run
 * against fixture roots whose modules are deliberately NOT the real ones — a
 * runtime check would fail every scratch-repo scenario. Which module may hold
 * an HTTP server is a design decision, so the names cannot be derived; the most
 * that can be checked is that each one still exists.
 */
class RuleListTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  /**
   * ⚠️ Reads the value the gate ACTUALLY USES, via a flag dispatched directly
   * under the assignments and above every selection path — not the assignment
   * text, and no longer below the {@code no module changed} early exit, where
   * it printed nothing whenever the staged change touched no module and made
   * all three cases here a function of the index. Matching the assignment
   * with a regex constrained a copy: inserting {@code NO_HTTP=""} just above
   * that loop made rules 2 and 5 inert for every module while this test went on
   * reading the strong assignment, and all 55 tests stayed green.
   */
  private static List<String> value(String var) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder("scripts/check-module.sh", "--print-rules").directory(ROOT.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    String line =
        out.lines()
            .filter(l -> l.startsWith(var + "="))
            .findFirst()
            .orElseThrow(() -> new AssertionError(var + " not printed by the gate:\n" + out));
    String v = line.substring(var.length() + 1).trim();
    assertThat(v).as("%s is empty -- rule inert\n%s", var, out).isNotEmpty();
    return List.of(v.split("\\s+"));
  }

  private static List<String> modules() throws Exception {
    Process p =
        new ProcessBuilder("scripts/check-module.sh", "--list-modules")
            .directory(ROOT.toFile())
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).isZero();
    return out.lines().map(String::trim).filter(l -> !l.isEmpty()).toList();
  }

  /**
   * ⚠️ The rules must be APPLIED, not merely declared. The gate reports how many
   * modules each rule was tested against, counted inside the loop from the same
   * expansion the rule uses. Shadowing {@code NO_HTTP=""} anywhere above that
   * loop drops the count to zero while "8 module(s) checked" stays true — which
   * is what made the earlier text-reading and print-above-the-loop versions of
   * this test pass over inert rules.
   */
  @Test
  void everyModuleNamedByARuleStillExists() throws Exception {
    List<String> all = modules();
    assertThat(all).as("sanity: the module list must not be empty").isNotEmpty();
    // ⚠️ EXACT, not isSubsetOf. A subset assertion is one-directional: dropping
    // a module from NO_HTTP is the weakening a developer blocked by this gate
    // actually performs, and it left both cases green. These four and these two
    // are what architecture.md rules 5 and 2 name; changing either list is a
    // deliberate act that must edit this line, where the reviewer will see it
    // next to the production change.
    assertThat(value("NO_HTTP"))
        .as("architecture.md rule 5: no HTTP server in these modules")
        .containsExactlyInAnyOrder("format", "binstore-spi", "sequencer", "ingest");
    assertThat(value("NO_CLOUD"))
        .as("architecture.md rule 2: no cloud SDK in the OpenSearch JVM")
        .containsExactlyInAnyOrder("client", "plugin");
    // ...and every one of them must still BE a module, which is the half that
    // catches a rename rather than a deliberate edit.
    assertThat(value("NO_HTTP")).isSubsetOf(all);
    assertThat(value("NO_CLOUD")).isSubsetOf(all);
  }

  /** The lists must also stay disjoint — a module in both is a contradiction. */
  @Test
  void theRuleListsDoNotOverlap() throws Exception {
    assertThat(value("NO_HTTP")).doesNotContainAnyElementsOf(value("NO_CLOUD"));
  }
}
