// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A MOVED test file must not blind the two gates that read it (M0.53).
 *
 * <p>⚠️ Git emits only the DESTINATION path for a rename, so both scanners
 * asked {@code git show HEAD:<destination>} — which does not exist. In
 * {@code test_integrity.py} that is {@code None}, so the loop takes its
 * "new test file: nothing to weaken" branch and the SOURCE path never appears
 * at all, so the removal side is never examined either. In
 * {@code tdd_scan.py} it is the empty string, so every moved test reads as
 * NEW and a gutted-but-still-falsifiable assertion earns a perfectly good red
 * record.
 *
 * <p>⚠️ TWO SHAPES, because they fail differently and round 1 conflated them.
 * A SOURCE-SET move (test -> integrationTest) keeps the package, so ids are
 * stable and a gutting reads as a WEAKENING. A MODULE move (M4.2's own shape)
 * changes the package — and module and top-level package are the same thing
 * here — so every id changes, and merely following the rename made a pure move
 * read as a wholesale REMOVAL. An earlier draft of this class claimed the
 * source-set fixture WAS M4.2's shape; it is not, and that sentence would have
 * been the one a reader trusted when concluding M4.2 was covered. The constructed input that first proved this
 * (M4.2's rename plus six deleted assertions and one
 * {@code isEqualTo(3)} → {@code isNotNull()}) was scored R095 by git and the
 * gate printed "ok no test weakened, disabled or removed" and exited 0.
 */
class RenameBlindnessTest {

  private static final String PRODUCTION =
      "package binjava;\nclass Foo { int n() { return %d; } }\n";

  /**
   * ⚠️ A MODULE PREFIX, because both scanners match {@code /src/main/java/}
   * and {@code /src/test/java/} with a LEADING slash. A fixture rooted at
   * {@code src/...} matches neither, and the first draft of this test was
   * vacuous for exactly that reason: the gate answered "no production change
   * in this commit" and the test read it as the defect.
   */
  private static final String MOD = "mod/";

  private static final String STRONG_TEST = """
      package binjava;
      class FooTest {
        @Test void keepsItsPromise() {
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void alsoHoldsUnderLoad() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void andAtTheBoundary() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void andWhenEmpty() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
      }
      """;

  /** ⚠️ ONE method gutted, not all four: git must still score this a rename
   * (measured R091), which is the whole input under test. */
  private static final String GUTTED_TEST = """
      package binjava;
      class FooTest {
        @Test void keepsItsPromise() {
          assertThat(new Foo().n()).isNotNull();
        }
        @Test void alsoHoldsUnderLoad() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void andAtTheBoundary() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void andWhenEmpty() {
          assertThat(new Foo().n()).isEqualTo(3);
          assertThat(new Foo().n()).isEqualTo(3);
        }
        @Test void aGenuinelyNewOne() {
          assertThat(new Foo().n()).isEqualTo(4);
        }
      }
      """;

  private static final String FROM = MOD + "src/test/java/binjava/FooTest.java";
  private static final String TO = MOD + "src/integrationTest/java/binjava/FooTest.java";
  /** ⚠️ OUTSIDE {@code TEST_PATH} — a destination the gate's loop never reads. */
  private static final String FIXTURES = MOD + "src/testFixtures/java/binjava/FooTest.java";
  private static final String BAR = MOD + "src/test/java/binjava/BarTest.java";
  private static final String PROD = MOD + "src/main/java/binjava/Foo.java";

  /**
   * A repository holding one production file and one strong test, committed —
   * then the test MOVED between source sets and gutted, alongside a real
   * production change, all staged together.
   */
  private Path movedAndGutted(Path dir) throws Exception {
    copyScripts(dir);
    write(dir, MOD + "src/main/java/binjava/Foo.java", PRODUCTION.formatted(3));
    write(dir, FROM, STRONG_TEST);
        // ⚠️ RENAME DETECTION OFF IN CONFIG, so `-M` in the scanner is what finds
    // the rename. Without this the fixture inherits git's default
    // `diff.renames=true` and dropping `-M` survives the whole suite.
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git config diff.renames false"
        + " && git add -A && git commit -qm base");

    // ⚠️ A MOVE, made the way a person makes one, so git scores it a rename.
    run(dir, "mkdir -p " + MOD + "src/integrationTest/java/binjava"
        + " && git mv " + FROM + " " + TO);
    write(dir, TO, GUTTED_TEST);
    write(dir, MOD + "src/main/java/binjava/Foo.java", PRODUCTION.formatted(4));
    run(dir, "git add -A");
    return dir;
  }

  private void copyScripts(Path dir) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("check-test-integrity.sh", "test_integrity.py", "tdd_scan.py",
        "java_tests.py", "git_renames.py", "lib.sh")) {
      Path src = repo.resolve("scripts").resolve(f);
      // ⚠️ Tolerant, so the RED this test first produced is the defect itself
      // -- the gate exiting 0 on a moved-and-gutted test -- and not a missing
      // helper that had simply not been written yet.
      if (!Files.exists(src)) {
        continue;
      }
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(src, dst);
      dst.toFile().setExecutable(true);
    }
  }

  private void write(Path dir, String rel, String body) throws Exception {
    Path f = dir.resolve(rel);
    Files.createDirectories(f.getParent());
    Files.writeString(f, body);
  }

  private String run(Path dir, String script) throws Exception {
    Process p = strip(new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
    return out;
  }

  /**
   * ⚠️ Ambient git state is cleared: {@code GIT_INDEX_FILE} is inherited, so a
   * fixture that shells out to {@code git add} would otherwise write into
   * whatever index its caller had exported.
   */
  private static ProcessBuilder strip(ProcessBuilder b) {
    b.environment().keySet().removeIf(k -> k.startsWith("GIT_"));
    return b;
  }

  @Test
  void aMovedTestFileCannotHIDEAWeakenedAssertion(@TempDir Path dir) throws Exception {
    // ⚠️ THE ASSERTION THAT MATTERS. This is the gate's whole subject —
    // "production changed to satisfy the test" — and a `git mv` was enough to
    // walk past it with exit 0.
    Path repo = movedAndGutted(dir);
    Files.writeString(repo.resolve("msg.txt"), "M0.53 a commit with no trailer\n");

    Process p = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh", "msg.txt")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor())
        .as("a moved-and-gutted test must be refused, not waved through:\n" + out)
        .isNotZero();
    assertThat(out)
        .as("and it must name the test and the drop, not merely complain")
        .contains("binjava.FooTest#keepsItsPromise")
        .contains("strength fell 3 -> 1");
  }

  /**
   * Stage the gate against {@code repo}, collecting its output.
   *
   * <p>The commit message carries no {@code Test-removed:} trailer, because a
   * trailer is the documented way to ACCEPT a removal and would suppress
   * exactly what these cases exist to observe.
   */
  private int gate(Path repo, StringBuilder sink) throws Exception {
    Files.writeString(repo.resolve("msg.txt"), "M0.53 a commit with no trailer\n");
    Process p = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh", "msg.txt")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    sink.append(new String(p.getInputStream().readAllBytes()));
    return p.waitFor();
  }

  @Test
  void aTestThatLEFTTheTestTierIsARemovalNotAnAbsence(@TempDir Path dir) throws Exception {
    // ⚠️ SEEDED WITH GIT'S DEFAULT RENAME DETECTION, and that omission IS the
    // fixture. Every other case here sets `diff.renames false` so the scanner's
    // explicit `-M` is what finds the move — which left the configuration this
    // repository and CI actually run entirely untested. Under it `--name-only`
    // emits ONLY the destination, so a test leaving `src/test` has no path the
    // loop reads as a test at all: measured, this staging scored R092 and the
    // gate printed "ok no test weakened, disabled or removed", exit 0. The
    // dropping half of the rename fix cannot reach it — with detection ON the
    // source was never in the list to be dropped.
    copyScripts(dir);
    write(dir, PROD, PRODUCTION.formatted(3));
    write(dir, FROM, STRONG_TEST);
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    run(dir, "mkdir -p " + MOD + "src/testFixtures/java/binjava"
        + " && git mv " + FROM + " " + FIXTURES);
    write(dir, FIXTURES, GUTTED_TEST);
    write(dir, PROD, PRODUCTION.formatted(4));
    run(dir, "git add -A");

    StringBuilder out = new StringBuilder();
    int code = gate(dir, out);

    assertThat(code).as("a test gutted on its way out of the tier must be refused:\n" + out)
        .isNotZero();
    // ⚠️ THE ID IS PART OF THE NEEDLE. The SUCCESS line reads "no test weakened,
    // disabled or removed alongside the production change", so asserting on
    // "removed alongside" by itself matches the pass too and could never fail.
    assertThat(out.toString()).as("and must name the test it lost:\n" + out)
        .contains("binjava.FooTest#keepsItsPromise removed alongside");
  }

  @Test
  void aTestRenamedToANameTheLoopCannotReadIsStillALostTest(@TempDir Path dir) throws Exception {
    // ⚠️ A WEAKENING RELATIVE TO HEAD — non-negotiable 2, not merely a gap.
    // `git mv FooTest.java FooTest.java.bak` (equally the in-place `.java` →
    // `.kt` conversion) leaves a destination that still matches TEST_PATH, so a
    // predicate asking only TEST_PATH dropped the source as redundant while the
    // loop, which also requires `.java`, never read the destination. Measured
    // with detection off, alongside a production change: HEAD refused, exit 1,
    // naming both methods; the draft that asked three different questions
    // printed `ok`, exit 0.
    copyScripts(dir);
    write(dir, PROD, PRODUCTION.formatted(3));
    write(dir, FROM, STRONG_TEST);
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git config diff.renames false && git add -A && git commit -qm base");
    run(dir, "git mv " + FROM + " " + FROM + ".bak");
    write(dir, PROD, PRODUCTION.formatted(4));
    run(dir, "git add -A");

    StringBuilder out = new StringBuilder();
    int code = gate(dir, out);

    assertThat(code).as("a test renamed beyond the loop's reach must be refused:\n" + out)
        .isNotZero();
    assertThat(out.toString()).as("and must be named, not silently dropped:\n" + out)
        .contains("binjava.FooTest#keepsItsPromise removed alongside");
  }

  @Test
  void aProductionFileThatLEFTSrcMainIsStillAProductionChange(@TempDir Path dir) throws Exception {
    // ⚠️ THE SHORT-CIRCUIT, NOT THE LOOP. Under the default config a `main` file
    // moved into a test source set leaves no `/src/main/java/` path staged at
    // all, so the "no production change in this commit" early return skipped the
    // ENTIRE gate and a weakening in an unrelated test rode along unexamined.
    // Measured on the pre-fix bytes: exit 0.
    copyScripts(dir);
    write(dir, PROD, PRODUCTION.formatted(3));
    write(dir, BAR, STRONG_TEST.replace("FooTest", "BarTest"));
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    run(dir, "git mv " + PROD + " " + MOD + "src/test/java/binjava/Foo.java");
    write(dir, BAR, GUTTED_TEST.replace("FooTest", "BarTest"));
    run(dir, "git add -A");

    StringBuilder out = new StringBuilder();
    int code = gate(dir, out);

    assertThat(code).as("the gate must not skip itself:\n" + out).isNotZero();
    assertThat(out.toString()).as("and must name the weakening it would have skipped:\n" + out)
        .contains("binjava.BarTest#keepsItsPromise")
        .contains("strength fell 3 -> 1");
  }

  @Test
  void thePackageComesFromTheDeclarationNotACommentThatLooksLikeOne(@TempDir Path dir)
      throws Exception {
    // ⚠️ `package_of` normalises before searching, and its docstring NAMED this
    // input while the suite did not contain it — reverting to `PKG.search(src)`
    // left all of buildSrc green. It matters because a wrong package makes
    // `rekey`'s prefix guard decline to strip: every re-keyed id is then
    // double-qualified, a moved test stops matching itself, and a weakening
    // reads as a wholesale removal of the file.
    copyScripts(dir);
    write(dir, "x.java", "/*\npackage binjava.wrong;\n*/\npackage binjava.right;\nclass X {}\n");

    Process p = strip(new ProcessBuilder("python3", "-c",
        "import sys; sys.path.insert(0, 'scripts'); import java_tests;"
            + " print(java_tests.package_of(open('x.java').read()))")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as(out).isZero();
    assertThat(out.trim()).as("a commented-out package line must not beat the declaration")
        .isEqualTo("binjava.right");
  }

  @Test
  void onlyAGENUINELYNewTestIsDemandedARecordNotTheOnesThatMerelyMoved(@TempDir Path dir)
      throws Exception {
    // ⚠️ ROUND-1 REVIEW FOUND THE VACUITY THIS CLOSES: asserting only that the
    // moved ids are absent passes just as well when the scanner examines
    // NOTHING -- narrowing tdd_scan's TEST_PATH so the moved file is filtered
    // out entirely prints the same "no new tests" line and stayed green. The
    // discriminating shape is a POSITIVE: the moved file also gains one genuinely new
    // method, and the scanner must demand a record for exactly that id and for
    // none of the four that moved.
    Path repo = movedAndGutted(dir);

    Process p = strip(new ProcessBuilder("python3", "scripts/tdd_scan.py", "check")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    int code = p.waitFor();

    assertThat(out)
        .as("the new method must be demanded:\n" + out)
        .contains("binjava.FooTest#aGenuinelyNewOne");
    assertThat(out)
        .as("and the ones that merely moved must not be:\n" + out)
        .doesNotContain("binjava.FooTest#keepsItsPromise")
        .doesNotContain("binjava.FooTest#alsoHoldsUnderLoad")
        .doesNotContain("binjava.FooTest#andAtTheBoundary")
        .doesNotContain("binjava.FooTest#andWhenEmpty");
    assertThat(code).as("a genuinely new test with no record is a refusal").isNotZero();
  }

  @Test
  void theNameStatusParserFollowsRenamesButNotCopies() throws Exception {
    // ⚠️ THE PURE TRANSFORM, over text no fixture repository can be made to
    // emit. Round-1 review measured that dropping the `R` guard and swapping
    // source with destination both survived a suite that could only reach the
    // parser through git.
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Process p = strip(new ProcessBuilder("python3", "scripts/git_renames.py")
        .directory(repo.toFile())).start();
    p.getOutputStream().write(("R091\tmod/a/Old.java\tmod/b/New.java\n"
        + "C100\tmod/a/Copied.java\tmod/b/CopyOf.java\n"
        + "M\tmod/a/Touched.java\n"
        + "A\tmod/a/Added.java\n").getBytes());
    p.getOutputStream().close();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();

    assertThat(out.strip())
        .as("a rename maps destination -> source, and nothing else is a rename")
        .isEqualTo("mod/b/New.java\tmod/a/Old.java");
  }

  @Test
  void theCIModeFollowsRenamesToo(@TempDir Path dir) throws Exception {
    // ⚠️ THE MODE CI ACTUALLY RUNS, and round-1 review measured it untested:
    // pinning the argument list to `--cached` left the whole suite green while
    // `CHECK_RANGE` mode went straight back to `ok ... exit 0` on a
    // moved-and-gutted test. AGENTS.md's own gate table says
    // check-test-integrity runs in CI ONLY with CHECK_RANGE, so the untested
    // branch was the only branch that runs on a push.
    Path repo = movedAndGutted(dir);
    String base = run(repo, "git rev-parse HEAD").strip();
    run(repo, "git commit -qm 'the move'");
    Files.writeString(repo.resolve("msg.txt"), "M0.53 no trailer\n");

    ProcessBuilder b = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh",
        "msg.txt").directory(repo.toFile()));
    b.environment().put("CHECK_RANGE", base);
    Process p = b.redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("CI mode must see the weakening too:\n" + out).isNotZero();
    assertThat(out).contains("strength fell 3 -> 1");
  }

  /** A module move: package changes with the path, so every test id changes. */
  private Path movedBetweenModules(Path dir, boolean gut) throws Exception {
    copyScripts(dir);
    write(dir, "ingest/src/main/java/binjava/ingest/CommitLog.java",
        "package binjava.ingest;\nclass CommitLog { int n() { return 3; } }\n");
    write(dir, "ingest/src/test/java/binjava/ingest/CommitLogTest.java", MODULE_TEST);
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git config diff.renames false && git add -A && git commit -qm base");

    run(dir, "mkdir -p sequencer/src/main/java/binjava/sequencer"
        + " sequencer/src/test/java/binjava/sequencer"
        + " && git mv ingest/src/main/java/binjava/ingest/CommitLog.java"
        + " sequencer/src/main/java/binjava/sequencer/CommitLog.java"
        + " && git mv ingest/src/test/java/binjava/ingest/CommitLogTest.java"
        + " sequencer/src/test/java/binjava/sequencer/CommitLogTest.java");
    write(dir, "sequencer/src/main/java/binjava/sequencer/CommitLog.java",
        "package binjava.sequencer;\nclass CommitLog { int n() { return 4; } }\n");
    write(dir, "sequencer/src/test/java/binjava/sequencer/CommitLogTest.java",
        (gut ? MODULE_TEST.replaceFirst("isEqualTo\\(3\\)", "isNotNull()") : MODULE_TEST)
            .replace("package binjava.ingest;", "package binjava.sequencer;"));
    run(dir, "git add -A");
    return dir;
  }

  private static final String MODULE_TEST = """
      package binjava.ingest;
      class CommitLogTest {
        @Test void appliesDelta() {
          assertThat(new CommitLog().n()).isEqualTo(3);
          assertThat(new CommitLog().n()).isEqualTo(3);
        }
        @Test void refusesEmptyCommit() {
          assertThat(new CommitLog().n()).isEqualTo(3);
          assertThat(new CommitLog().n()).isEqualTo(3);
        }
      }
      """;

  @Test
  void aPUREModuleMoveIsNotAnAccusationOfRemovingEveryTestInIt(@TempDir Path dir)
      throws Exception {
    // ⚠️ M4.2'S OWN SHAPE, and the regression round 1 of this task introduced:
    // following the rename to the source blob is not enough, because module and
    // top-level package are the same thing here, so every id changes and a pure
    // move read as a wholesale REMOVAL. That is a false accusation, and its
    // documented workaround is worse -- `Test-removed:` is matched over the
    // WHOLE commit body, so silencing the phantoms silences a real deletion in
    // the same commit too.
    Path repo = movedBetweenModules(dir, false);
    Files.writeString(repo.resolve("msg.txt"), "M0.53 a pure module move\n");

    Process p = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh", "msg.txt")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("a pure move accuses nobody:\n" + out).isZero();
    // ⚠️ NOT `doesNotContain("removed alongside")` -- that phrase is a
    // substring of the gate's own SUCCESS line ("no test weakened, disabled or
    // removed alongside the production change"), so it could never fail. Name
    // the ids the round-1 regression actually accused.
    assertThat(out)
        .as("and names nothing:\n" + out)
        .doesNotContain("binjava.ingest.CommitLogTest#appliesDelta")
        .doesNotContain("binjava.ingest.CommitLogTest#refusesEmptyCommit");

    // ⚠️ THE OTHER SCANNER, ON THE SAME FIXTURE. Round-2 review measured that
    // `tdd_scan`'s half of the re-qualification was completely unfalsified:
    // both module tests called only the integrity gate, and the one test that
    // did run `tdd_scan` used the source-set fixture, where the packages match
    // and `rekey` returns on its first line. Deleting `tdd_scan`'s block left
    // the suite green while a pure module move went back to demanding a fresh
    // red record for every test in the moved file -- the half M4.2's row
    // records as costing nine re-earned records.
    Process t = strip(new ProcessBuilder("python3", "scripts/tdd_scan.py", "check")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    String tOut = new String(t.getInputStream().readAllBytes());
    assertThat(t.waitFor()).as("nothing moved is new:\n" + tOut).isZero();
    assertThat(tOut)
        .as("under either package spelling:\n" + tOut)
        .doesNotContain("CommitLogTest#appliesDelta")
        .doesNotContain("CommitLogTest#refusesEmptyCommit");
  }

  @Test
  void aModuleMoveStillCatchesAWeakenedAssertionAsAWeakening(@TempDir Path dir) throws Exception {
    // ⚠️ THE OTHER SIDE of the same fixture: re-qualifying ids must not buy the
    // false-positive fix by going blind again. The gutting has to surface, and
    // as a WEAKENING -- naming the drop -- rather than as a removal.
    Path repo = movedBetweenModules(dir, true);
    Files.writeString(repo.resolve("msg.txt"), "M0.53 a module move that also guts\n");

    Process p = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh", "msg.txt")
        .directory(repo.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("the gutting must still be refused:\n" + out).isNotZero();
    assertThat(out)
        .as("named as a weakening of the re-qualified id, not a removal:\n" + out)
        .contains("binjava.sequencer.CommitLogTest#appliesDelta")
        .contains("strength fell");
  }
}
