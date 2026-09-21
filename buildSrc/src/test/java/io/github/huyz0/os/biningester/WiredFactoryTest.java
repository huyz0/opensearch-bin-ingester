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

  private int scan() throws Exception {
    Process p = ProcessSupport.builder("python3", ROOT.resolve("scripts/wired_scan.py").toString(),
        repo.resolve("SPEC.md").toString(), repo.resolve("backlog.md").toString())
        .redirectErrorStream(true).start();
    try (var in = p.getOutputStream(); var files = Files.walk(repo)) {
      in.write(String.join("\n", files.map(Path::toString).toList()).getBytes());
    }
    p.getInputStream().readAllBytes();
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
  }

  @Test
  void aSTATICCallIntoAClassThatNEVERBuildsItselfIsNot() throws Exception {
    scratch();
    write("a/src/main/java/x/Widget.java",
        "class Widget { static int answer() { return 42; } }\n");
    write("b/src/main/java/x/Root.java", "class Root { int n = Widget.answer(); }\n");

    assertThat(scan()).isEqualTo(1);
  }
}
