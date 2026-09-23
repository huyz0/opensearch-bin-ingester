// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A same-named receiver in another method must not shadow a valid call here. */
class WiredScopeTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  @TempDir Path repo;

  @Test
  void aReceiverInAnotherMethodDoesNotHideTheValidFieldCallInBothScanners() throws Exception {
    source("a/src/main/java/pkg/Widget.java", "package pkg; class Widget { void spin() {} }\n");
    source("a/src/main/java/pkg/Other.java", "package pkg; class Other {}\n");
    source("b/src/main/java/pkg/Root.java", "package pkg; class Root { Widget widget;\n"
        + " void run() { widget.spin(); }\n"
        + " void other() throws Exception { Other widget; widget.spin(); }\n}\n");

    Files.createDirectories(repo.resolve("docs/internal/product/milestones/M8"));
    Files.writeString(repo.resolve("docs/internal/product/milestones/M8/SPEC.md"),
        "## The unwired set\n\n| Entry | What | Accepts | Else owned by |\n|---|---|---|---|\n"
            + "| M1.1 | widget | `call pkg.Widget.spin` | — |\n");
    Files.createDirectories(repo.resolve("docs/internal/product"));
    Files.writeString(repo.resolve("docs/internal/product/backlog.md"),
        "| ID | Task | Serves | State |\n|---|---|---|---|\n");
    var task = ProjectBuilder.builder().build().getTasks().create("wired", WiredGateTask.class);
    task.getRepository().set(repo.toFile());
    task.verify();
    assertThat(pythonScan()).isZero();
  }

  private void source(String relative, String text) throws Exception {
    Path file = repo.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, text);
  }

  private int pythonScan() throws Exception {
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
