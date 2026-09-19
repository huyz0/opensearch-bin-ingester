// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code scripts/wired_scan.py}: each entry of M8's unwired set is WIRED by
 * its own predicate, or OWNED by an open backlog row (M8.25, criterion 16).
 *
 * <p>⚠️ **A MENTION IS NOT A WIRING.** "Referenced" was MEASURED green on the
 * real tree for five mechanisms whose every hit was a javadoc sentence saying
 * the mechanism is NOT wired, so the scan reads CODE with comments and
 * strings blanked, and only {@code src/main}.
 */
class WiredGateTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

  private record Run(int exit, String out) {
  }

  /**
   * ⚠️ OUTSIDE THE REPOSITORY: a fixture holding {@code src/main} trees under
   * the gitignored {@code build/} is refused by the gate that finds ignored
   * sources, and rightly -- it is exactly what that gate looks for.
   */
  @TempDir
  Path repo;

  private void scratch() throws Exception {
    backlog("| M9.1 | the open owner | FR-1 | todo |\n| M9.2 | a closed row | FR-1 | done |\n");
  }

  private void backlog(String rows) throws Exception {
    Files.writeString(repo.resolve("backlog.md"),
        "# Backlog\n\n| ID | Task | Serves | State |\n|---|---|---|---|\n" + rows);
  }

  /** A spec holding exactly one entry of the table. */
  private void entry(String predicate, String owner) throws Exception {
    Files.writeString(repo.resolve("SPEC.md"), "# M9\n\n## The unwired set\n\n"
        + "| Entry | What is unwired | What `check-wired.sh` accepts as wired | Else owned by |\n"
        + "|---|---|---|---|\n"
        + "| M1.1 | the widget | " + predicate + " | " + owner + " |\n\n## Requirements\n");
  }

  private void source(String path, String body) throws Exception {
    Path file = repo.resolve(path);
    Files.createDirectories(file.getParent());
    Files.writeString(file, body);
  }

  private Run scan() throws Exception {
    ProcessBuilder pb = new ProcessBuilder("python3",
        ROOT.resolve("scripts/wired_scan.py").toString(), repo.resolve("SPEC.md").toString(),
        repo.resolve("backlog.md").toString()).directory(repo.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    // ⚠️ THE FILE LIST ON STDIN, as `check-wired.sh` hands it from
    // `workspace_files`: the scanner never walks a tree itself.
    try (var in = p.getOutputStream(); var files = Files.walk(repo)) {
      in.write(String.join("\n", files.map(Path::toString).toList()).getBytes());
    }
    String out = new String(p.getInputStream().readAllBytes());
    return new Run(p.waitFor(), out);
  }

  @Test
  void aCONSTRUCTIONInSrcMainIsWIRED() throws Exception {
    scratch();
    entry("`new Widget`", "—");
    source("a/src/main/java/x/Widget.java", "class Widget {}\n");
    source("b/src/main/java/x/Root.java", "class Root { Object w = new Widget(); }\n");

    Run run = scan();

    assertThat(run.exit()).as(run.out()).isZero();
    assertThat(run.out()).contains("M1.1").contains("WIRED");
  }

  @Test
  void aMENTIONInACommentOrAStringIsNOTAWiring() throws Exception {
    scratch();
    entry("`new Widget`", "—");
    source("a/src/main/java/x/Widget.java", "class Widget {}\n");
    source("b/src/main/java/x/Root.java", "/** nothing calls new Widget() yet */\n"
        + "class Root { // new Widget()\n String s = \"new Widget()\"; }\n");

    Run run = scan();

    assertThat(run.exit()).as("⚠️ THE FIVE JAVADOC HITS review MEASURED%n%s", run.out())
        .isEqualTo(1);
    assertThat(run.out()).contains("M1.1").contains("UNWIRED");
  }

  @Test
  void aCONSTRUCTIONInATESTSourceSetIsNOTAWiring() throws Exception {
    scratch();
    entry("`new Widget`", "—");
    source("a/src/main/java/x/Widget.java", "class Widget {}\n");
    source("b/src/test/java/x/WidgetTest.java", "class WidgetTest { Object w = new Widget(); }\n");
    source("b/src/testFixtures/java/x/Fake.java", "class Fake { Object w = new Widget(); }\n");

    assertThat(scan().exit()).isEqualTo(1);
  }

  @Test
  void aTypeCONSTRUCTINGITSELFInItsOWNFileIsNOTAWiring() throws Exception {
    scratch();
    entry("`new Widget`", "—");
    source("a/src/main/java/x/Widget.java",
        "class Widget { static Widget make() { return new Widget(); } }\n");

    assertThat(scan().exit())
        .as("a static factory nobody calls is the class talking to itself").isEqualTo(1);
  }

  @Test
  void EVERYPredicateInACellMustHOLD() throws Exception {
    scratch();
    entry("`new Widget`; `call spin`", "—");
    source("a/src/main/java/x/Widget.java", "class Widget { void spin() {} }\n");
    source("b/src/main/java/x/Root.java", "class Root { Object w = new Widget(); }\n");

    assertThat(scan().exit())
        .as("constructed and never run is M5.91c's exact defect").isEqualTo(1);

    source("b/src/main/java/x/Root.java",
        "class Root { void go() { new Widget().spin(); } }\n");
    assertThat(scan().exit()).isZero();
  }

  @Test
  void anUNWIREDEntryNAMINGAnOPENRowIsOWNEDAndPasses() throws Exception {
    scratch();
    entry("`new Widget`", "M9.1");
    source("a/src/main/java/x/Widget.java", "class Widget {}\n");

    Run run = scan();

    assertThat(run.exit()).as(run.out()).isZero();
    assertThat(run.out()).contains("OWNED").contains("M9.1");
  }

  @Test
  void anOwnerThatIsDONEOrDOESNOTEXISTOwnsNothing() throws Exception {
    scratch();
    source("a/src/main/java/x/Widget.java", "class Widget {}\n");

    entry("`new Widget`", "M9.2");
    assertThat(scan().exit())
        .as("⚠️ A DONE ROW THAT LEFT IT UNWIRED IS THE CLAIM THIS GATE EXISTS TO REFUSE")
        .isEqualTo(1);

    entry("`new Widget`", "M9.77");
    assertThat(scan().exit()).as("an owner with no row behind it").isEqualTo(1);
  }

  @Test
  void aCellTheScanCANNOTREADFailsRatherThanPassing() throws Exception {
    scratch();
    entry("something constructs it somewhere", "M9.1");

    Run run = scan();

    assertThat(run.exit())
        .as("⚠️ an unreadable predicate is the spec's defect, fixed THERE (SPEC § The "
            + "unwired set), never a silent pass or a silent OWNED%n%s", run.out())
        .isEqualTo(1);
    assertThat(run.out()).contains("M1.1");
  }

  @Test
  void theOTHERKindsAreReadAsWritten() throws Exception {
    scratch();
    entry("`main`; `implements Positions`; `returns List<Delta>`; "
        + "`constant DEFAULT_BATCH in b`; `new-impl Transport`", "—");
    source("a/src/main/java/x/Transport.java", "interface Transport {}\n");
    source("a/src/main/java/x/Http.java", "class Http implements Transport {}\n");
    source("a/src/main/java/x/Gc.java",
        "class Gc { static final int DEFAULT_BATCH = 1000;\n"
            + " java.util.List<Delta> chain() { return null; } }\n");
    source("b/src/main/java/x/Main.java", "class Main implements Reporter.Positions {\n"
        + " public static void main(String[] a) { new Http(); int n = Gc.DEFAULT_BATCH; } }\n");

    Run run = scan();
    assertThat(run.exit()).as(run.out()).isZero();

    source("b/src/main/java/x/Main.java", "class Main implements Reporter.Positions {\n"
        + " public static void main(String[] a) { new Http(); } }\n");
    assertThat(scan().exit())
        .as("a constant the named module never reads is a number in a file").isEqualTo(1);
  }
}
