// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * One case per parser bug review actually found.
 *
 * Every one of these was discovered by an agent attacking the parser by hand,
 * across five review rounds costing minutes each. As a fixture the whole set
 * runs in milliseconds, so no future round re-derives them -- which is the
 * point: routing a predicate to an agent is what non-negotiable 9 forbids.
 *
 * The test execs the real script rather than reimplementing its logic, so it
 * constrains the artifact the gates actually run.
 */
class JavaTestParserTest {

  // ⚠️ Default display naming, deliberately. A custom name replaces the method
  // name in the JUnit XML, and scripts/tdd-red.sh maps a failing case back to
  // its method through exactly that field.
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        // fixture                | expected ids, semicolon-separated
        "brace-char-literal.java  | p.BraceTest#closes;p.BraceTest#opens;p.BraceTest#after",
        "value-source.java        | p.ParamTest#rejectsBadPartition;p.ParamTest#plain",
        "url-in-string.java       | p.StoreTest#endpointA;p.StoreTest#between;p.StoreTest#endpointB",
        "text-block-in-comment.java | p.SegTest#parsesMalformed;p.SegTest#hidden",
        "qualified-annotation.java| p.QualTest#fullyQualified;p.QualTest#plain",
        "nested-classes.java      | p.OuterTest#top;p.OuterTest$Alpha#same;p.OuterTest$Beta#same",
        "one-line-class.java      | p.OneLinerTest#inline",
        "annotation-type.java     | p.HolderTest#realTest",
        "all-test-annotations.java | p.AnnotationsTest#plain;p.AnnotationsTest#repeated;p.AnnotationsTest#factory;p.AnnotationsTest#template;p.AnnotationsTest#parameterised",
        "annotation-arg-braces.java | p.CsvTest#acceptsEachRow",
        "unicode-escape.java      | p.UnicodeTest#hiddenByEscape;p.UnicodeTest#plain",
        "disabled-and-overloads.java | p.OuterDisabledTest#inheritsClassDisabled;p.OverloadTest#accepts;p.OverloadTest#disabledBelowTest",
      })
  void findsExactlyTheseTestIds(String fixture, String expected) throws Exception {
    List<String> want =
        expected == null || expected.isBlank()
            ? List.of()
            : List.of(expected.trim().split("\\s*;\\s*"));
    assertThat(idsIn(fixture)).containsExactlyInAnyOrderElementsOf(want);
  }

  /**
   * A file the parser cannot trust must be REFUSED, not silently reported empty.
   * Nothing tested this before: every other case asserts exit 0, so the
   * fail-closed behaviour -- the thing that turns a parser bug into a visible
   * failure instead of a green gate -- had no coverage at all.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "refused/unbalanced-braces.java | braces do not balance",
        "refused/unpaired-quote.java    | unpaired quote",
        "refused/count-mismatch.java    | test annotation(s)",
      })
  void refusesFilesItCannotTrust(String fixture, String expected) throws Exception {
    Run r = run("ids", fixture.trim());
    assertThat(r.exit).as("must exit non-zero: %s", r.out).isNotZero();
    assertThat(r.out).contains(expected.trim());
  }

  /** @Disabled is how a test is neutered without deleting it, so it is asserted. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "disabled-and-overloads.java | p.OuterDisabledTest#inheritsClassDisabled DISABLED",
        "disabled-and-overloads.java | p.OverloadTest#disabledBelowTest DISABLED",
        "disabled-and-overloads.java | p.OverloadTest#accepts enabled",
      })
  void reportsDisabledState(String fixture, String expected) throws Exception {
    Run r = run("scan", fixture.trim());
    assertThat(r.exit).as(r.out).isZero();
    assertThat(r.out.lines().map(String::trim).anyMatch(l -> l.startsWith(expected.trim())))
        .as("no line starts with %s in:%n%s", expected.trim(), r.out)
        .isTrue();
  }

  /**
   * The method BODY is what {@code check-test-integrity} scores to detect a
   * weakened assertion. Nothing observed it, so replacing the field with an
   * empty string blinded that gate while every parser test stayed green.
   */
  @org.junit.jupiter.api.Test
  void reportsANonEmptyBodyForEachTest() throws Exception {
    Run r = run("scan", "brace-char-literal.java");
    assertThat(r.exit).as(r.out).isZero();
    assertThat(r.out.lines()).isNotEmpty();
    assertThat(r.out.lines())
        .as("every method needs a non-empty body, or strength() scores nothing")
        .allMatch(l -> l.matches(".*body=[1-9][0-9]*$"));
  }

  private record Run(int exit, String out) {}

  private static Run run(String cmd, String fixture) throws IOException, InterruptedException {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f = repo.resolve("buildSrc/src/test/resources/java-fixtures").resolve(fixture);
    assertThat(f).exists();
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", cmd, f.toString())
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Run(p.waitFor(), out);
  }

  private static List<String> idsIn(String fixture) throws IOException, InterruptedException {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f = repo.resolve("buildSrc/src/test/resources/java-fixtures").resolve(fixture);
    assertThat(f).exists();
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", "ids", f.toString())
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as("tdd_scan.py exit for %s: %s", fixture, out).isZero();
    return out.lines().filter(l -> !l.isBlank()).collect(Collectors.toList());
  }
}
