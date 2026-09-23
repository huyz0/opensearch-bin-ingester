// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.GradleException;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The enforced JVM check-wired task must reject its known false positives. */
class WiredGateTaskTest {

  @TempDir Path repo;

  private WiredGateTask task(String predicate) throws Exception {
    Path spec = repo.resolve("docs/internal/product/milestones/M8/SPEC.md");
    Files.createDirectories(spec.getParent());
    Files.writeString(spec, "## The unwired set\n\n"
        + "| Entry | What | Accepts | Else owned by |\n|---|---|---|---|\n"
        + "| M1.1 | widget | `" + predicate + "` | — |\n");
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

  @Test
  void aSameNamedCallOnAnotherReceiverTypeDoesNotSatisfyThePredicate() throws Exception {
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {\n"
        + " void spin() {}\n}\n");
    source("b/src/main/java/pkg/Other.java", "package pkg; class Other {\n"
        + " void spin() {}\n}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " Other other; void run() { other.spin(); }\n}\n");

    assertThatThrownBy(() -> task("call pkg.Widget.spin").verify())
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
  }

  @Test
  void aCallThroughAReceiverOfTheNamedTypeSatisfiesTheQualifiedPredicate() throws Exception {
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget {\n"
        + " void spin() {}\n}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root {\n"
        + " Widget widget; void run() { widget.spin(); }\n}\n");

    task("call pkg.Widget.spin").verify();
  }
}
