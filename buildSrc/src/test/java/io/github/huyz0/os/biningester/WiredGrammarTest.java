// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.GradleException;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Malformed wired predicates must not disappear when another predicate is valid. */
class WiredGrammarTest {

  @TempDir Path repo;

  @Test
  void anUnqualifiedCallIsNotDroppedBesideAValidConstruction() throws Exception {
    WiredGateTask task = task("`new Widget`; `call spin`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root { Object w = new Widget(); }\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("unreadable predicate cell");
  }

  @Test
  void aShadowedReceiverOfAnotherTypeDoesNotWireTheNamedCall() throws Exception {
    WiredGateTask task = task("`call pkg.Widget.spin`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { void spin() {} }\n");
    source("b/src/main/java/pkg/Other.java", "package pkg; class Other {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " Widget widget; void run() { { Other widget; widget.spin(); } }\n}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  @Test
  void aSameNamedReceiverFromAnotherPackageDoesNotWireTheNamedCall() throws Exception {
    WiredGateTask task = task("`call pkg.Widget.spin`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { void spin() {} }\n");
    source("b/src/main/java/other/Widget.java", "package other; class Widget { void spin() {} }\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " other.Widget widget; void run() { widget.spin(); }\n}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  @Test
  void aStaticCallToAnotherPackagesSameNamedTypeDoesNotWireTheNamedCall() throws Exception {
    WiredGateTask task = task("`call pkg.Widget.spin`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { static void spin() {} }\n");
    source("b/src/main/java/other/Widget.java", "package other; class Widget { static void spin() {} }\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " void run() { other.Widget.spin(); }\n}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  @Test
  void aQualifiedNestedConstructionDoesNotWireItsOuterType() throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {\n"
        + " static class Nested {}\n"
        + " static Widget create() { return new Widget(); }\n"
        + "}\n");
    source("b/src/main/java/other/Root.java", "package other; class Root {\n"
        + " Object value = new pkg.Widget.Nested();\n"
        + "}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  /**
   * ⚠️ A PACKAGE OF SEVERAL SEGMENTS (M10.13, harvested from 1ed333b's review
   * M8.77-TEST-1). With a one-segment package, narrowing the qualified
   * prefix's repetition from "any number" to "at most one" survived.
   */
  @Test
  void aQualifiedNestedConstructionInADeepPackageDoesNotWireItsOuterType() throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/a/b/c/Widget.java", "package a.b.c; class Widget {\n"
        + " static class Nested {}\n"
        + " static Widget create() { return new Widget(); }\n"
        + "}\n");
    source("b/src/main/java/other/Root.java", "package other; class Root {\n"
        + " Object value = new a.b.c.Widget.Nested();\n"
        + "}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  /**
   * ⚠️ A {@code //} OR {@code /*} INSIDE A STRING IS NOT A COMMENT (M10.13,
   * found by M10.19 on {@code DeltaFanOut}). Comments were stripped before
   * strings, so {@code "http://"} swallowed the rest of its line -- here, the
   * construction that wires the entry -- and a {@code "/*"} swallowed code up
   * to the next {@code *}{@code /} anywhere after it.
   */
  @Test
  void commentMarkersInsideStringsDoNotHideTheCodeAfterThem() throws Exception {
    WiredGateTask task = task("`new Widget`; `new Gadget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("a/src/main/java/pkg/Gadget.java", "package pkg; class Gadget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " String url = \"http://host\"; Object w = new Widget();\n"
        + " String open = \"/*\"; Object g = new Gadget(); String close = \"*/\";\n"
        + "}\n");

    task.verify();
  }

  /**
   * A quote inside a char literal opens no string: without the char-literal
   * lexeme, {@code '"'} would open one running to the next quote on the line
   * and hide the construction between them.
   */
  @Test
  void aQuoteInACharLiteralDoesNotOpenAString() throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " char quote = '\"'; Object w = new Widget(); String s = \"x\";\n"
        + "}\n");

    task.verify();
  }

  /** Nor does a quote inside a block comment, for the construction after it. */
  @Test
  void aQuoteInABlockCommentDoesNotOpenAString() throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " /* a \" quote */ Object w = new Widget(); String s = \"x\";\n"
        + "}\n");

    task.verify();
  }

  /**
   * ⚠️ A CONSTRUCTION ONLY MENTIONED IS NOT A WIRING, whether in a line
   * comment, a block comment or a text block: each is stripped before the
   * scan, and losing any of the three would let a mention satisfy the gate.
   */
  @Test
  void aConstructionOnlyInALineCommentDoesNotWire() throws Exception {
    assertUnwiredWith(" // Object w = new Widget();\n");
  }

  @Test
  void aConstructionOnlyInABlockCommentDoesNotWire() throws Exception {
    assertUnwiredWith(" /* Object w =\n new Widget(); */\n");
  }

  @Test
  void aConstructionOnlyInATextBlockDoesNotWire() throws Exception {
    assertUnwiredWith(" String t = \"\"\"\n new Widget()\n \"\"\";\n");
  }

  private void assertUnwiredWith(String body) throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n" + body + "}\n");

    assertThatThrownBy(task::verify)
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  /**
   * A long string literal is scanned without recursion: the alternation it
   * was matched with recursed once per character and overflowed the stack.
   */
  @Test
  void aVeryLongStringLiteralDoesNotOverflowTheScanner() throws Exception {
    WiredGateTask task = task("`new Widget`");
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " String big = \"" + "x\\\"".repeat(100_000) + "\";\n"
        + " Object w = new Widget();\n"
        + "}\n");

    task.verify();
  }

  private WiredGateTask task(String predicate) throws Exception {
    Path spec = repo.resolve("docs/internal/product/milestones/M8/SPEC.md");
    Files.createDirectories(spec.getParent());
    Files.writeString(spec, "## The unwired set\n\n"
        + "| Entry | What | Accepts | Else owned by |\n|---|---|---|---|\n"
        + "| M1.1 | widget | " + predicate + " | — |\n");
    Path backlog = repo.resolve("docs/internal/product/backlog.md");
    Files.createDirectories(backlog.getParent());
    Files.writeString(backlog, "| ID | Task | Serves | State |\n"
        + "|---|---|---|---|\n");
    WiredGateTask task = ProjectBuilder.builder().build().getTasks()
        .create("wired", WiredGateTask.class);
    task.getRepository().set(repo.toFile());
    return task;
  }

  private void source(String path, String text) throws Exception {
    Path file = repo.resolve(path);
    Files.createDirectories(file.getParent());
    Files.writeString(file, text);
  }
}
