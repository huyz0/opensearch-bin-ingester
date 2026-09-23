// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Wildcard imports resolve a call only when the source tree identifies one exact type. */
class WiredWildcardImportTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  @TempDir Path repo;

  @Test
  void wildcardImportedInstanceReceiverResolvesInBothScanners() throws Exception {
    sources(false);
    assertJvmWired();
    assertPythonWired();
  }

  @Test
  void wildcardImportedStaticReceiverResolvesInBothScanners() throws Exception {
    sources(true);
    assertJvmWired();
    assertPythonWired();
  }

  private void sources(boolean staticCall) throws Exception {
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { "
        + (staticCall ? "static " : "") + "void spin() {} }\n");
    source("b/src/main/java/consumer/Root.java", "package consumer; import pkg.*; class Root {\n"
        + (staticCall ? "void run() { Widget.spin(); }" : "Widget widget; void run() { widget.spin(); }")
        + "\n}\n");
  }

  private void source(String relative, String text) throws Exception {
    Path file = repo.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, text);
  }

  private void specAndBacklog() throws Exception {
    Files.createDirectories(repo.resolve("docs/internal/product/milestones/M8"));
    Files.writeString(repo.resolve("docs/internal/product/milestones/M8/SPEC.md"),
        "## The unwired set\n\n| Entry | What | Accepts | Else owned by |\n|---|---|---|---|\n"
            + "| M1.1 | widget | `call pkg.Widget.spin` | — |\n");
    Files.createDirectories(repo.resolve("docs/internal/product"));
    Files.writeString(repo.resolve("docs/internal/product/backlog.md"),
        "| ID | Task | Serves | State |\n|---|---|---|---|\n");
  }

  private void assertJvmWired() throws Exception {
    specAndBacklog();
    var task = ProjectBuilder.builder().build().getTasks().create("wired", WiredGateTask.class);
    task.getRepository().set(repo.toFile());
    task.verify();
  }

  private int pythonScan() throws Exception {
    specAndBacklog();
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

  private void assertPythonWired() throws Exception {
    assertThat(pythonScan()).isZero();
  }
}
