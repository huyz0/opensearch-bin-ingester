// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A STATIC FACTORY CALLED FROM ANOTHER FILE IS A CONSTRUCTION (M8.25).
 *
 * <p>⚠️ MEASURED on the real tree: the plugin builds {@code NodeSubscriptions}
 * through {@code NodeSubscriptions.fetching(...)}, and a scan that accepted
 * only a literal {@code new T(} outside T's file reported it unwired. The
 * factory must still CONSTRUCT {@code T} -- a static call into a class that
 * never builds itself is not a construction of it.
 */
class WiredFactoryTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  /** ⚠️ Outside the repository, for the reason {@code WiredGateTest} gives. */
  @TempDir
  Path repo;

  private void write(String path, String body) throws Exception {
    Path file = repo.resolve(path);
    Files.createDirectories(file.getParent());
    Files.writeString(file, body);
  }

  /**
   * What the last {@link #scan()} printed. ⚠️ READ BY EVERY CASE (M11.18,
   * H13): an exit code of 1 is also what a Python traceback returns, so a case
   * asserting the code alone passed against a scanner that crashed.
   */
  private String output = "";

  private int scan() throws Exception {
    Process p = ProcessSupport.builder("python3", ROOT.resolve("scripts/wired_scan.py").toString(),
        repo.resolve("SPEC.md").toString(), repo.resolve("backlog.md").toString())
        .redirectErrorStream(true).start();
    try (var in = p.getOutputStream(); var files = Files.walk(repo)) {
      in.write(String.join("\n", files.map(Path::toString).toList()).getBytes());
    }
    output = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    return p.waitFor();
  }

  private void scratch() throws Exception {
    write("backlog.md", "| ID | Task | Serves | State |\n|---|---|---|---|\n");
    write("SPEC.md", "## The unwired set\n\n| Entry | What | Accepts | Else owned by |\n"
        + "|---|---|---|---|\n| M1.1 | w | `new Widget` | — |\n");
  }

  @Test
  void aFACTORYCalledFromANOTHERFileIsAConstruction() throws Exception {
    scratch();
    write("a/src/main/java/x/Widget.java",
        "class Widget { static Widget make() { return new Widget(); } }\n");
    write("b/src/main/java/x/Root.java", "class Root { Object w = Widget.make(); }\n");

    assertThat(scan()).isZero();
    assertThat(output).contains("M1.1: WIRED");
  }

  @Test
  void aSTATICCallIntoAClassThatNEVERBuildsItselfIsNot() throws Exception {
    scratch();
    write("a/src/main/java/x/Widget.java",
        "class Widget { static int answer() { return 42; } }\n");
    write("b/src/main/java/x/Root.java", "class Root { int n = Widget.answer(); }\n");

    assertThat(scan()).isEqualTo(1);
    assertThat(output).as("unwired by the scan's verdict, not by a crash")
        .contains("M1.1: UNWIRED");
  }

  @Test
  void aQUALIFIEDNestedConstructionDoesNotConstructItsOuterType() throws Exception {
    scratch();
    write("a/src/main/java/pkg/Widget.java",
        "package pkg; class Widget { static class Nested {} "
            + "static Widget create() { return new Widget(); } }\n");
    write("b/src/main/java/other/Root.java",
        "package other; class Root { Object value = new pkg.Widget.Nested(); }\n");

    assertThat(scan()).isEqualTo(1);
    assertThat(output).as("unwired by the scan's verdict, not by a crash")
        .contains("M1.1: UNWIRED");
  }

  /**
   * ⚠️ MULTI-SEGMENT package on purpose (M10.20): with a one-segment
   * {@code pkg.} the qualified-prefix repetition could be narrowed from
   * {@code *} to {@code ?} and every case would still pass.
   */
  @Test
  void aMULTISEGMENTQualifiedNestedConstructionDoesNotConstructItsOuterType()
      throws Exception {
    scratch();
    write("a/src/main/java/io/pkg/Widget.java",
        "package io.pkg; class Widget { static class Nested {} "
            + "static Widget create() { return new Widget(); } }\n");
    write("b/src/main/java/other/Root.java",
        "package other; class Root { Object value = new io.pkg.Widget.Nested(); }\n");

    assertThat(scan()).isEqualTo(1);
    assertThat(output).as("unwired by the scan's verdict, not by a crash")
        .contains("M1.1: UNWIRED");
  }
}
