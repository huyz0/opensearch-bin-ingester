// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The BOUNDS on following a rename (M0.53).
 *
 * <p>{@link RenameBlindnessTest} pins that the two scanners follow a rename at
 * all. This class pins the two limits on that, each of which was a way of being
 * WEAKER than HEAD rather than merely uncovered — so both are non-negotiable 2,
 * not gaps.
 *
 * <p>⚠️ Kept separate rather than added to its sibling for two reasons that both
 * matter: that file is within a few lines of the 500-line limit, and editing it
 * would invalidate the ten red records bound to its bytes, forcing every one of
 * them to be re-observed to prove something about a file this change does not
 * touch.
 */
class RenameFollowingBoundsTest {

  private static final String MOD = "mod/";
  private static final String PRODUCTION =
      "package binjava;\nclass Foo { int n() { return %d; } }\n";
  private static final String INTO = MOD + "src/test/java/binjava/FooTest.java";

  /** ⚠️ Long enough that git scores a MOVE rather than an add plus a delete. */
  private static String test(String assertion) {
    StringBuilder b = new StringBuilder("package binjava;\nclass FooTest {\n");
    for (int i = 0; i < 12; i++) {
      b.append("  // padding line ").append(i).append(" to lift the similarity index\n");
    }
    return b.append("  @Test void keepsItsPromise() { assertThat(new Foo().n())")
        .append(assertion).append("; }\n")
        .append("  @Test void alsoHoldsUnderLoad() { assertThat(new Foo().n()).isEqualTo(3); }\n")
        .append("}\n").toString();
  }

  @Test
  void aRenameSourceTheGateNEVERScannedCannotSupplyABefore(@TempDir Path dir) throws Exception {
    // ⚠️ A TWO-COMMIT, GREEN-TREE BYPASS of non-negotiable 3, opened by
    // following renames without asking where they came FROM. Commit one adds
    // `docs/drafts/FooTest.java`, which no gate reads. Commit two `git mv`s it
    // into the test tier: the scanner took the draft as the "before", subtracted
    // its ids from the new ones, and asked for no red record at all. Measured on
    // HEAD, which demands both records, so this was a WEAKENING.
    copyScripts(dir);
    write(dir, "docs/drafts/FooTest.java", test(".isEqualTo(3)"));
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    run(dir, "mkdir -p " + MOD + "src/test/java/binjava"
        + " && git mv docs/drafts/FooTest.java " + INTO + " && git add -A");

    Process p = strip(new ProcessBuilder("python3", "scripts/tdd_scan.py", "check")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor())
        .as("a test arriving from a path no gate scans is NEW and owes a record:\n" + out)
        .isNotZero();
    assertThat(out)
        .as("and both of its tests must be demanded, not just named:\n" + out)
        .contains("binjava.FooTest#keepsItsPromise")
        .contains("binjava.FooTest#alsoHoldsUnderLoad");
  }

  @Test
  void aRenameSourceDifferingONLYInExtensionIsAlsoUnscanned(@TempDir Path dir) throws Exception {
    // ⚠️ THE OTHER CONJUNCT of `reads(origin)`. Its sibling above varies only
    // the TEST_PATH half — `docs/drafts/FooTest.java` fails the path while
    // PASSING `.endswith('.java')` — so the mutant `if not
    // TEST_PATH.search(origin)` survived it with the whole suite green. A
    // `.bak` file INSIDE the test tier is equally ungated, because the `paths`
    // filter requires `.java`, so committing one and then `git mv`-ing it onto
    // the real name printed `ok no new tests in this diff`, exit 0: round 5's
    // bypass one path variant over.
    copyScripts(dir);
    write(dir, INTO + ".bak", test(".isEqualTo(3)"));
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    run(dir, "git mv " + INTO + ".bak " + INTO + " && git add -A");

    Process p = strip(new ProcessBuilder("python3", "scripts/tdd_scan.py", "check")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor())
        .as("an ungated `.bak` sibling cannot stand in for a red record:\n" + out)
        .isNotZero();
    assertThat(out)
        .as("and both of its tests must be demanded:\n" + out)
        .contains("binjava.FooTest#keepsItsPromise")
        .contains("binjava.FooTest#alsoHoldsUnderLoad");
  }

  @Test
  void theDepartureIsRecoveredUnderGitsDEFAULTConfigToo(@TempDir Path dir) throws Exception {
    // ⚠️ NO `diff.renames false` HERE, and that is the entire point. Its sibling
    // pins this shape with detection OFF — where `--name-only` lists the source
    // anyway, so the `departed` add-back is inert and the case passes without
    // it. Under git's DEFAULT only the destination is listed, it fails the
    // loop's `.java` guard, and `departed` is the sole thing that recovers the
    // file. Measured: reverting `departed` alone leaves the whole suite green
    // while this shape silently returns to exit 0.
    copyScripts(dir);
    write(dir, MOD + "src/main/java/binjava/Foo.java", PRODUCTION.formatted(3));
    write(dir, INTO, test(".isEqualTo(3)"));
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    run(dir, "git mv " + INTO + " " + INTO + ".bak");
    write(dir, MOD + "src/main/java/binjava/Foo.java", PRODUCTION.formatted(4));
    run(dir, "git add -A");
    Files.writeString(dir.resolve("msg.txt"), "M0.53 a commit with no trailer\n");

    Process p = strip(new ProcessBuilder("bash", "scripts/check-test-integrity.sh", "msg.txt")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("the departed test must be refused:\n" + out).isNotZero();
    // ⚠️ The id is part of the needle: the SUCCESS line also contains
    // "removed alongside", so the bare phrase could never fail.
    assertThat(out).as("and named:\n" + out)
        .contains("binjava.FooTest#keepsItsPromise removed alongside");
  }

  private void copyScripts(Path dir) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("check-test-integrity.sh", "test_integrity.py", "tdd_scan.py",
        "java_tests.py", "git_renames.py", "lib.sh")) {
      Path src = repo.resolve("scripts").resolve(f);
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

  /** ⚠️ {@code GIT_INDEX_FILE} is inherited, and a fixture that shells out to
   * {@code git add} would otherwise write into the caller's private index. */
  private static ProcessBuilder strip(ProcessBuilder b) {
    b.environment().keySet().removeIf(k -> k.startsWith("GIT_"));
    return b;
  }
}
