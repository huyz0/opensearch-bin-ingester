// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.GradleException;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Explicit imports resolve exactly; competing wildcard imports remain ambiguous. */
class WiredImportResolutionTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  @TempDir Path repo;

  @Test
  void explicitImportResolvesTheNamedReceiverInBothScanners() throws Exception {
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { void spin() {} }\n");
    source("b/src/main/java/consumer/Root.java", "package consumer; import pkg.Widget; class Root {\n"
        + " Widget widget; void run() { widget.spin(); }\n}\n");

    task("call pkg.Widget.spin").verify();
    assertThat(pythonScan()).isZero();
  }

  @Test
  void ambiguousWildcardImportsDoNotResolveTheNamedReceiverInEitherScanner() throws Exception {
    source("a/src/main/java/first/Widget.java", "package first; class Widget { void spin() {} }\n");
    source("b/src/main/java/second/Widget.java", "package second; class Widget { void spin() {} }\n");
    source("c/src/main/java/consumer/Root.java", "package consumer; import first.*; import second.*;\n"
        + " class Root { Widget widget; void run() { widget.spin(); } }\n");

    assertThatThrownBy(() -> task("call first.Widget.spin").verify())
        .isInstanceOf(GradleException.class)
        .hasMessageContaining("M1.1: unwired");
    assertThat(pythonScan()).isEqualTo(1);
  }

  private WiredGateTask task(String predicate) throws Exception {
    Files.createDirectories(repo.resolve("docs/internal/product/milestones/M8"));
    Files.writeString(repo.resolve("docs/internal/product/milestones/M8/SPEC.md"),
        "## The unwired set\n\n| Entry | What | Accepts | Else owned by |\n|---|---|---|---|\n"
            + "| M1.1 | widget | `" + predicate + "` | — |\n");
    Files.createDirectories(repo.resolve("docs/internal/product"));
    Files.writeString(repo.resolve("docs/internal/product/backlog.md"),
        "| ID | Task | Serves | State |\n|---|---|---|---|\n");
    var task = ProjectBuilder.builder().build().getTasks().create("wired", WiredGateTask.class);
    task.getRepository().set(repo.toFile());
    return task;
  }

  private void source(String relative, String text) throws Exception {
    Path file = repo.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, text);
  }

  private int pythonScan() throws Exception {
    // task(predicate) has already written the exact predicate under test.
    ProcessBuilder builder = ProcessSupport.builder("python3",
        ROOT.resolve("scripts/wired_scan.py").toString(),
        repo.resolve("docs/internal/product/milestones/M8/SPEC.md").toString(),
        repo.resolve("docs/internal/product/backlog.md").toString()).directory(repo.toFile());
    builder.redirectErrorStream(true);
    Process process = builder.start();
    try (var input = process.getOutputStream(); var files = Files.walk(repo)) {
      input.write(String.join("\n", files.map(Path::toString).toList()).getBytes());
    }
    process.getInputStream().readAllBytes();
    return process.waitFor();
  }
}
