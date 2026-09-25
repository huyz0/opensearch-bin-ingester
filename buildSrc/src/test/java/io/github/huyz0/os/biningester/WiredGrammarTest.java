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
