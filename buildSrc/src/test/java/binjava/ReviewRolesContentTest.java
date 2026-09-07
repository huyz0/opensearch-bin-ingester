// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Role routing by what a diff CHANGES, not merely by which files it touches
 * (M0.80).
 *
 * <p>{@link ReviewRolesTest} routes by PATH, so a {@code .java} diff always
 * summons both. That is right for a code change and wrong for a comment one:
 * M4.10c spent two of its six rounds on diffs whose every edit was a javadoc
 * sentence, where a test-reviewer has nothing to mutate and its only moves are
 * a vacuous verdict or a blocked commit -- the same bind ReviewRolesTest was
 * written to remove for documentation.
 *
 * <p>⚠️ THIS LOOSENS A GATE, so every case below that could wrongly buy a
 * one-role commit is asserted in the STRICT direction. The comparison is over
 * {@code hashable}, the same comment-dropping lexer the TDD gate binds its red
 * records with, and anything it cannot decide is executable.
 */
class ReviewRolesContentTest {

  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("review-roles.sh", "lib.sh", "diff_shape.py", "java_tests.py")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    return dir;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
        .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** Commits {@code before}, then stages {@code after} at the same path. */
  private String rolesAfterEdit(Path dir, String path, String before, String after)
      throws Exception {
    Path f = dir.resolve(path);
    Files.createDirectories(f.getParent());
    Files.writeString(f, before);
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    if (after == null) {
      run(dir, "git rm -q '" + path + "'");
    } else {
      Files.writeString(f, after);
      run(dir, "git add -A");
    }
    Process p =
        new ProcessBuilder("bash", "scripts/review-roles.sh").directory(dir.toFile()).start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return out.replace("\n", " ").trim();
  }

  private static final String CODE =
      "class A {\n  int f() {\n    return 1;\n  }\n}\n";

  /** The case the row exists for: only a javadoc sentence moved. */
  @Test
  void aCommentOnlyJavaDiffSummonsTheReviewerONLY(@TempDir Path dir) throws Exception {
    String out = rolesAfterEdit(scratch(dir), "format/src/main/java/binjava/format/A.java",
        CODE, "/** A doc sentence. */\n" + CODE);

    assertThat(out).as(out).isEqualTo("reviewer");
  }

  /** ⚠️ And one executable character still summons both. */
  @Test
  void aOneCharacterEXECUTABLEChangeSummonsBOTH(@TempDir Path dir) throws Exception {
    String out = rolesAfterEdit(scratch(dir), "format/src/main/java/binjava/format/A.java",
        CODE, CODE.replace("return 1;", "return 2;"));

    assertThat(out).as(out).contains("test-reviewer");
  }

  /**
   * ⚠️ A STRING LITERAL IS NOT PROSE. {@code hashable} keeps literal interiors
   * verbatim for exactly this reason -- an assertion's expected value can be
   * rewritten to whatever the code produces, which is the weakening
   * non-negotiable 2 names.
   */
  @Test
  void changingATextLITERALSummonsBOTH(@TempDir Path dir) throws Exception {
    String before = "class A {\n  String s = \"hello  world\";\n}\n";
    String out = rolesAfterEdit(scratch(dir), "format/src/test/java/binjava/format/ATest.java",
        before, before.replace("hello  world", "hello world"));

    assertThat(out).as(out).contains("test-reviewer");
  }

  /** ⚠️ Deleting a test is the case non-negotiable 2 names. Never one role. */
  @Test
  void DELETINGAJavaFileSummonsBOTH(@TempDir Path dir) throws Exception {
    String out = rolesAfterEdit(scratch(dir), "format/src/test/java/binjava/format/ATest.java",
        CODE, null);

    assertThat(out).as(out).contains("test-reviewer");
  }

  /** A new file has no prior version to be prose-identical to. */
  @Test
  void ADDINGAJavaFileSummonsBOTH(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    Files.writeString(repo.resolve("README.md"), "base\n");
    run(repo, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    Path f = repo.resolve("format/src/main/java/binjava/format/A.java");
    Files.createDirectories(f.getParent());
    Files.writeString(f, CODE);
    run(repo, "git add -A");
    Process p =
        new ProcessBuilder("bash", "scripts/review-roles.sh").directory(repo.toFile()).start();
    String out = new String(p.getInputStream().readAllBytes()).replace("\n", " ").trim();
    p.waitFor();

    assertThat(out).as(out).contains("test-reviewer");
  }

  /**
   * ⚠️ A COMMENT-ONLY JAVA EDIT ALONGSIDE AN EXECUTABLE SCRIPT IS NOT PROSE.
   * The whole diff is judged, not its most innocent file.
   */
  @Test
  void aCommentOnlyJavaEditBesideAChangedSCRIPTSummonsBOTH(@TempDir Path dir) throws Exception {
    Path repo = scratch(dir);
    Path f = repo.resolve("format/src/main/java/binjava/format/A.java");
    Files.createDirectories(f.getParent());
    Files.writeString(f, CODE);
    Files.writeString(repo.resolve("scripts/thing.sh"), "#!/bin/sh\necho one\n");
    run(repo, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    Files.writeString(f, "/** doc. */\n" + CODE);
    Files.writeString(repo.resolve("scripts/thing.sh"), "#!/bin/sh\necho two\n");
    run(repo, "git add -A");
    Process p =
        new ProcessBuilder("bash", "scripts/review-roles.sh").directory(repo.toFile()).start();
    String out = new String(p.getInputStream().readAllBytes()).replace("\n", " ").trim();
    p.waitFor();

    assertThat(out).as(out).contains("test-reviewer");
  }

  /** ⚠️ FAILS CLOSED: java the lexer cannot parse is executable, not prose. */
  @Test
  void UNPARSEABLEJavaSummonsBOTH(@TempDir Path dir) throws Exception {
    String out = rolesAfterEdit(scratch(dir), "format/src/main/java/binjava/format/A.java",
        CODE, "class A { String s = \"unterminated;\n");

    assertThat(out).as(out).contains("test-reviewer");
  }
}
