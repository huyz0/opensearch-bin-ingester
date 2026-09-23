// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every predicate kind of the unwired set REFUSES as well as accepts (M8.25).
 *
 * <p>⚠️ **A KIND TESTED ONLY IN THE PASSING DIRECTION IS A KIND THAT MAY
 * ANSWER TRUE.** Review MEASURED it: {@code main}, {@code implements},
 * {@code returns} and {@code new-impl} each mutated to {@code return True}
 * left every other case green, and three of them guard real entries.
 */
class WiredRefusalTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  /** ⚠️ Outside the repository, for the reason {@code WiredGateTest} gives. */
  @TempDir
  Path repo;

  private void write(String path, String body) throws Exception {
    Path file = repo.resolve(path);
    Files.createDirectories(file.getParent());
    Files.writeString(file, body);
  }

  /** The exit code of a scan of one entry whose predicate is {@code predicate}. */
  private int scan(String predicate) throws Exception {
    write("backlog.md", "| ID | Task | Serves | State |\n|---|---|---|---|\n");
    write("SPEC.md", "## The unwired set\n\n| Entry | What | Accepts | Else owned by |\n"
        + "|---|---|---|---|\n| M1.1 | w | " + predicate + " | — |\n");
    Process p = ProcessSupport.builder("python3", ROOT.resolve("scripts/wired_scan.py").toString(),
        repo.resolve("SPEC.md").toString(), repo.resolve("backlog.md").toString())
        .redirectErrorStream(true).start();
    try (var in = p.getOutputStream(); var files = Files.walk(repo)) {
      in.write(String.join("\n", files.map(Path::toString).toList()).getBytes());
    }
    p.getInputStream().readAllBytes();
    return p.waitFor();
  }

  @Test
  void noMAINIsRefused() throws Exception {
    write("a/src/main/java/x/Root.java", "class Root { static void main(String[] a) {} }\n");
    assertThat(scan("`main`")).as("not public: the JVM cannot start it").isEqualTo(1);
  }

  @Test
  void noIMPLEMENTORIsRefused() throws Exception {
    write("a/src/main/java/x/Root.java", "class Root implements Other { }\n");
    assertThat(scan("`implements Positions`")).isEqualTo(1);
  }

  @Test
  void aDIFFERENTReturnTypeIsRefused() throws Exception {
    write("a/src/main/java/x/Gc.java",
        "class Gc { java.util.List<Other> chain() { return null; } }\n");
    assertThat(scan("`returns List<Delta>`")).isEqualTo(1);
  }

  @Test
  void anImplementationNOBODYConstructsIsRefused() throws Exception {
    write("a/src/main/java/x/Transport.java", "interface Transport {}\n");
    write("a/src/main/java/x/Http.java", "class Http implements Transport {}\n");
    assertThat(scan("`new-impl Transport`")).isEqualTo(1);
  }

  @Test
  void aCALLOnlyInsideTheDECLARINGFileIsRefused() throws Exception {
    write("a/src/main/java/x/Widget.java",
        "package x; class Widget { void spin() {} void go() { this.spin(); } }\n");
    assertThat(scan("`call x.Widget.spin`")).as("the class calling itself").isEqualTo(1);
  }

  @Test
  void aDIFFERENTReceiverTypeWithTheSameMethodIsNotTheNamedCall() throws Exception {
    write("a/src/main/java/x/Widget.java", "package x; class Widget { void spin() {} }\n");
    write("b/src/main/java/x/Other.java", "package x; class Other { void spin() {} }\n");
    write("b/src/main/java/x/Root.java", "package x; class Root { Other other; void run() { other.spin(); } }\n");
    assertThat(scan("`call x.Widget.spin`")).isEqualTo(1);
  }

  @Test
  void aSHADOWEDReceiverOfAnotherTypeDoesNotWireTheNamedCall() throws Exception {
    write("a/src/main/java/x/Widget.java", "package x; class Widget { void spin() {} }\n");
    write("b/src/main/java/x/Other.java", "package x; class Other {}\n");
    write("b/src/main/java/x/Root.java", "package x; class Root { Widget widget;\n"
        + " void run() { { Other widget; widget.spin(); } } }\n");
    assertThat(scan("`call x.Widget.spin`")).isEqualTo(1);
  }

  @Test
  void anUNQUALIFIEDCallBesideAValidConstructionIsRefused() throws Exception {
    write("a/src/main/java/x/Widget.java", "class Widget {}\n");
    write("b/src/main/java/x/Other.java", "class Other { void spin() {} }\n");
    write("b/src/main/java/x/Root.java", "class Root { Object built = new Widget();\n"
        + " Other other; void run() { other.spin(); } }\n");
    assertThat(scan("`new Widget`; `call spin`")).isEqualTo(1);
  }

  @Test
  void aSAMENamedReceiverFromAnotherPackageDoesNotWireTheNamedCall() throws Exception {
    write("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { void spin() {} }\n");
    write("b/src/main/java/other/Widget.java", "package other; class Widget { void spin() {} }\n");
    write("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " other.Widget widget; void run() { widget.spin(); }\n}\n");
    assertThat(scan("`call pkg.Widget.spin`")).isEqualTo(1);
  }

  @Test
  void aSTATICCallToAnotherPackagesSameNamedTypeDoesNotWireTheNamedCall() throws Exception {
    write("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { static void spin() {} }\n");
    write("b/src/main/java/other/Widget.java", "package other; class Widget { static void spin() {} }\n");
    write("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " void run() { other.Widget.spin(); }\n}\n");
    assertThat(scan("`call pkg.Widget.spin`")).isEqualTo(1);
  }

  @Test
  void aFullyQualifiedCallThroughTheNamedReceiverIsAccepted() throws Exception {
    write("a/src/main/java/x/Widget.java", "package x; class Widget { void spin() {} }\n");
    write("b/src/main/java/x/Root.java",
        "package x; class Root { Widget widget; void run() { widget.spin(); } }\n");
    assertThat(scan("`call x.Widget.spin`")).isZero();
  }

  @Test
  void aCONSTANTThatIsOnlyCOMPAREDIsNotDeclared() throws Exception {
    write("b/src/main/java/x/Root.java",
        "class Root { boolean b(int x) { return DEFAULT_BATCH == x; } }\n");
    write("b/src/main/java/x/Other.java", "class Other { int n = Gc.DEFAULT_BATCH; }\n");
    assertThat(scan("`constant DEFAULT_BATCH in b`"))
        .as("`==` is not a declaration, and nothing declares it").isEqualTo(1);
  }
}
