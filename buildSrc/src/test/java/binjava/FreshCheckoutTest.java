// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Gates must work in a checkout that has never been developed in.
 *
 * <p>⚠️ Two gitignored directories — {@code .tmp/} and {@code .harness/} — exist
 * on every developer machine and in no fresh clone. Two gates depended on them
 * without saying so, and both failed in any checkout that lacked them:
 *
 * <p>{@code check-harness-tests.sh} redirected into {@code .harness/} without
 * creating it. Bash evaluates redirects before the command, so Gradle never
 * started and the gate reported the suite <b>failed</b>, citing a log that did
 * not exist — the inversion of the "reported 0 tests, it did not run" antidote
 * five lines below it.
 *
 * <p>⚠️ {@code check-gate-scope.sh} had the same class of bug and is fixed in
 * M0.31 — but getting there took four attempts, each wrong in a NEW direction,
 * so the cases below are a specification rather than a sample: a fresh checkout
 * (false RED), the developer's own ignore files, a negation, a pattern covering
 * one probe path but not its siblings, and a rule quarantining one clone rather
 * than the directory.
 *
 * <p>⚠️ This is a false RED, not a false green, which is why every
 * mutation-hunting suite here missed it: the batteries ask whether a gate can
 * pass while checking nothing, and this one FAILS while checking nothing. It
 * was found only by running the gate somewhere that had never run it.
 */
class FreshCheckoutTest {

  /** A checkout holding exactly the tracked files, and nothing a dev machine accumulates. */
  /**
   * Deletes a fixture directory BEST EFFORT, and never fails a test for it.
   *
   * <p>⚠️ CLEANUP IS NOT THIS SUITE'S SUBJECT. {@code run} asserts exit 0 on
   * every command, so using it for cleanup turned an environmental race into a
   * failure of a test about what {@code check-gate-scope.sh} does to a
   * .gitignore pattern -- and {@code @TempDir} threw from its OWN cleanup for
   * the same reason, which is why these fixtures now use
   * {@code CleanupMode.NEVER} and discard here instead.
   *
   * <p>MEASURED ON CI, twice: {@code rm: cannot remove '/tmp/fresh...':
   * Directory not empty} -- rm unlinked the children and found the directory
   * repopulated. That is a property of the runner's filesystem rather than of
   * the gate, and it reproduces neither locally nor under CI's environment on
   * this machine. M0.87 carries it.
   *
   * <p>⚠️ NOT SILENT, and NOT in a finally. A leak is PRINTED, because a
   * fixture that survives is worth knowing about even when it must not fail the
   * run; and a FAILING test deliberately keeps its directory, which is the one
   * case where the fixture is evidence.
   */
  private void discard(Path dir) {
    try {
      Process p = new ProcessBuilder("bash", "-c", "rm -rf " + dir)
          .directory(dir.getParent().toFile()).redirectErrorStream(true).start();
      String said = new String(p.getInputStream().readAllBytes());
      if (p.waitFor() != 0) {
        System.err.println("FreshCheckoutTest: leaked fixture " + dir + " -- " + said.trim());
      }
    } catch (Exception leaked) {
      System.err.println("FreshCheckoutTest: could not discard " + dir + " -- " + leaked);
    }
  }

  private Path freshCheckout(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    // ⚠️ tar over `git ls-files -z`, and NOT `git checkout-index`.
    //
    // Two earlier shapes were each wrong. Copying the ls-files list with
    // Files.copy dropped the tracked SYMLINK .claude/skills silently, because
    // isRegularFile follows the link to a directory -- a fixture that is not
    // the checkout it claims to be, in a test about what a checkout looks like.
    // checkout-index fixes the symlink but reads the INDEX, so the fixture
    // showed the STAGED scripts: every mutation to the working tree was
    // invisible and all six died-by-mutation checks reported SURVIVES against
    // tests that were, in fact, correct. A test that cannot see the edit under
    // review constrains nothing.
    //
    // tar reads the working tree, preserves modes and symlinks, and takes
    // exactly the tracked set.
    run(repo, "git ls-files -z | tar --null -T - -cf - | tar -C " + dir + " -xf -");
    assertThat(dir.resolve("scripts/check-harness-tests.sh"))
        .as("sanity: the tar copy must have produced the script under test")
        .exists();
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    // ⚠️ Post-condition, not more copying: assert the fixture's own tracked list
    // equals the source's. The first version copied `git ls-files` and dropped
    // the tracked SYMLINK .claude/skills silently -- a fixture that is not the
    // checkout it claims to be, in a test about what a checkout looks like.
    assertThat(trackedIn(dir))
        .as("the fixture must hold exactly the tracked tree")
        .isEqualTo(trackedIn(Path.of("..").toAbsolutePath().normalize()));
    // ⚠️ .harness/ absent is the whole point: it is gitignored, so a real
    // checkout lacks it, and the gate under test redirects into it.
    assertThat(dir.resolve(".harness"))
        .as(".harness/ must NOT exist -- that is what this suite is about")
        .doesNotExist();
    return dir;
  }

  /**
   * ⚠️ Every git invocation in this class scrubs the same variables. They were
   * on {@code gate()} alone, which only READS; {@code run()} does
   * {@code git init && git add -A && git commit} and {@code trackedIn()} reads
   * the list the post-condition compares. With an absolute {@code GIT_DIR} the
   * outer repository's index is rewritten, the fixture's own {@code .git} is
   * left empty, and both sides of that comparison read the leaked repo — so it
   * passes having compared a list to itself.
   */
  private static void scrub(ProcessBuilder pb, Path home) {
    for (String v : List.of("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "GIT_OBJECT_DIRECTORY",
        "GIT_CEILING_DIRECTORIES", "CHECK_RANGE")) {
      pb.environment().remove(v);
    }
    pb.environment().put("GIT_CONFIG_GLOBAL", "/dev/null");
    pb.environment().put("GIT_CONFIG_SYSTEM", "/dev/null");
    // ⚠️ HOME and XDG belong here, not only in gate(). GIT_CONFIG_GLOBAL does
    // NOT disable $XDG_CONFIG_HOME/git/ignore, so a personal ignore matching
    // any tracked path dropped that file from the fixture's own `git add -A`.
    pb.environment().put("HOME", home.toString());
    pb.environment().put("XDG_CONFIG_HOME", home.resolve("xdg").toString());
  }

  private List<String> trackedIn(Path dir) throws Exception {
    ProcessBuilder tb = new ProcessBuilder("git", "ls-files").directory(dir.toFile());
    scrub(tb, dir);
    Process p = tb.start();
    List<String> out = new String(p.getInputStream().readAllBytes()).lines().sorted().toList();
    assertThat(p.waitFor()).isZero();
    assertThat(out).as("sanity: %s must list tracked files", dir).isNotEmpty();
    return out;
  }

  /**
   * ⚠️ Pins the same environment {@code gate()} does. This runs the fixture's
   * own {@code git add -A}, and a personal excludes file matching any tracked
   * path would drop it from the fixture — the environment deciding the answer
   * in the test whose whole thesis is that it must not.
   */
  private void run(Path dir, String script) throws Exception {
    // ⚠️ pipefail. Without it `bash -c` reports the LAST command's status, so
    // in `git ls-files | tar -cf - | tar -xf -` a producer that fails to read
    // the repository is invisible and the assertion below passes over a short
    // fixture. Reproduced: removing a tracked file gives tar "Cannot stat" and
    // a pipeline status of 0.
    ProcessBuilder rb =
        new ProcessBuilder("bash", "-c", "set -o pipefail; " + script).directory(dir.toFile());
    scrub(rb, dir);
    rb.redirectErrorStream(true);
    Process p = rb.start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  private String gate(Path dir, String script) throws Exception {
    Files.createDirectories(dir.resolve("xdg"));
    ProcessBuilder pb = new ProcessBuilder("bash", script).directory(dir.toFile());
    pb.environment().put("GATE_SCOPE", "full");
    scrub(pb, dir);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }






  /** Strips or rewrites the {@code .tmp/} line of the fixture's .gitignore. */
  private Path withGitignore(Path fresh, String replacement) throws Exception {
    Path ignore = fresh.resolve(".gitignore");
    StringBuilder out = new StringBuilder();
    for (String l : Files.readString(ignore).split("\n", -1)) {
      if (l.strip().equals(".tmp/")) {
        if (replacement != null) {
          out.append(replacement).append("\n");
        }
      } else {
        out.append(l).append("\n");
      }
    }
    // ⚠️ Line-wise, not substring. `.tmp/` is .gitignore's FIRST line, so
    // searching for "\n.tmp/\n" could never match and the assertion passed
    // whether or not the strip happened -- a sanity check that could not fail.
    assertThat(out.toString().lines().map(String::strip).toList())
        .as("the fixture must lose the real pattern")
        .doesNotContain(".tmp/");
    Files.writeString(ignore, out.toString());
    return fresh;
  }

  @Test
  void gateScopePassesInAFreshCheckout(@TempDir(cleanup = CleanupMode.NEVER) Path dir)
      throws Exception {
    String out = gate(freshCheckout(dir), "scripts/check-gate-scope.sh");
    assertThat(out).as(out).startsWith("0");
    assertThat(out).as(out).doesNotContain("quarantine");
    discard(dir);
  }

  /**
   * ⚠️ Each row defeated one of the four earlier fixes. {@code null} deletes the
   * pattern; the rest replace it with something that looks like a quarantine and
   * is not — a bare probe match, a single-clone rule, a glob covering one file,
   * and a negation.
   *
   * <p>⚠️ These are seven rows, not seven independent defects — measured, not
   * assumed. {@code probe}, {@code .tmp/probe} and {@code *probe*} all fail
   * through the same branch for the same reason (no probe path contains
   * "probe"), and are jointly killed only by shrinking the probe list. They are
   * kept because each is a pattern someone might actually write, but the count
   * overstates the coverage and the table should not be read as seven kills.
   *
   * <p>⚠️ The single-negation row here is caught for the MUNDANE reason — with
   * {@code .tmp/} replaced rather than appended, nothing ignores the probe at
   * all, so it fails identically to the deleted-pattern test. The case that
   * actually distinguishes {@code -q} from {@code -v} is
   * {@link #gateScopeRefusesWhenEveryProbeIsOnlyNegated}, which negates ALL
   * four probes: {@code -v} then exits 0 with a {@code .gitignore} source for
   * every one of them, and a gate trusting that exit status prints {@code ok}
   * over a repository with no quarantine.
   *
   * <p>⚠️ The negation row is caught by the gate's {@code check-ignore -q} EXIT
   * STATUS, not by any test for a leading {@code !}. Git reports the winning
   * rule, so a winning negation means the path is not ignored. An explicit
   * negation branch existed here and was deleted as unreachable — this comment
   * says which line actually carries the row, because the previous one credited
   * the branch that did not.
   */
  @ParameterizedTest
  @ValueSource(strings = {"probe", ".tmp/probe", "*probe*", "README",
      ".tmp/ourclone/", ".tmp/**/README", "!.tmp/ourclone/README"})
  void gateScopeRefusesAnythingShortOfAQuarantine(String pattern) throws Exception {
    Path dir = Files.createTempDirectory("fresh");
    try {
      String out = gate(withGitignore(freshCheckout(dir), pattern), "scripts/check-gate-scope.sh");
      assertThat(out).as("pattern %s\n%s", pattern, out).startsWith("1");
      assertThat(out).as(out).contains("not quarantined");
    } finally {
      discard(dir);
    }
  }

  /**
   * ⚠️ THE case that separates {@code check-ignore -q} from {@code -v}, and the
   * one the single-negation row was wrongly credited with. Every probe path is
   * negated and nothing else mentions {@code .tmp/}, so {@code -v} exits 0 and
   * names a {@code .gitignore} rule for all four — a gate reading that exit
   * status reports {@code ok} while the quarantine does not exist. Reproduced
   * before writing this: {@code -q} refuses, {@code -v} passes.
   *
   * <p>Note {@code .tmp/} cannot simply be left in place alongside the
   * negations: git refuses to re-include a file whose parent directory is
   * excluded, so the negations would be inert and the quarantine would hold.
   */
  @Test
  void gateScopeRefusesWhenEveryProbeIsOnlyNegated(@TempDir(cleanup = CleanupMode.NEVER) Path dir)
      throws Exception {
    Path fresh = freshCheckout(dir);
    Path ignore = fresh.resolve(".gitignore");
    StringBuilder out = new StringBuilder();
    for (String l : Files.readString(ignore).split("\n", -1)) {
      if (!l.strip().equals(".tmp/")) {
        out.append(l).append("\n");
      }
    }
    for (String p : List.of(".tmp/ourclone/README", ".tmp/ourclone/src/Main.java",
        ".tmp/other/pom.xml", ".tmp/scratch.txt")) {
      out.append("!").append(p).append("\n");
    }
    Files.writeString(ignore, out.toString());
    String res = gate(fresh, "scripts/check-gate-scope.sh");
    assertThat(res).as(res).startsWith("1");
    assertThat(res).as(res).contains("is not ignored at all");
    discard(dir);
  }

  @Test
  void gateScopeRefusesWhenTmpIsNotIgnoredAtAll(@TempDir(cleanup = CleanupMode.NEVER) Path dir)
      throws Exception {
    String out = gate(withGitignore(freshCheckout(dir), null), "scripts/check-gate-scope.sh");
    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("is not ignored at all");
    discard(dir);
  }

  /**
   * ⚠️ The developer's own ignore files are not this repository's decision. With
   * the pattern gone from .gitignore but present in a personal ignore or in
   * {@code $GIT_DIR/info/exclude} — the latter disabled by no config setting —
   * the gate must still refuse, and must say which file matched instead.
   */
  @ParameterizedTest
  @ValueSource(strings = {"personal", "info-exclude"})
  void gateScopeIgnoresIgnoreFilesThatAreNotThisRepositorys(String where) throws Exception {
    Path dir = Files.createTempDirectory("fresh");
    try {
      Path fresh = withGitignore(freshCheckout(dir), null);
      if (where.equals("personal")) {
        for (String at : List.of("xdg/git", ".config/git")) {
          Files.createDirectories(fresh.resolve(at));
          Files.writeString(fresh.resolve(at).resolve("ignore"), ".tmp/\n");
        }
      } else {
        Files.writeString(fresh.resolve(".git/info/exclude"), "# fixture\n.tmp/\n");
      }
      String out = gate(fresh, "scripts/check-gate-scope.sh");
      assertThat(out).as("%s\n%s", where, out).startsWith("1");
      assertThat(out).as(out).contains("not by this repository's .gitignore");
    } finally {
      discard(dir);
    }
  }

  /**
   * ⚠️ Asserts the ABSENCE of the shell's redirect error, not merely the exit
   * code: the gate exited 1 either way, so only the message distinguishes "the
   * suite ran and failed" from "the suite never started".
   */
  @Test
  void harnessTestsCreatesItsLogDirectoryBeforeRedirecting(@TempDir(cleanup = CleanupMode.NEVER) Path dir)
      throws Exception {
    Path fresh = freshCheckout(dir);
    // A stub that fails immediately, so this test does not recursively run the
    // whole suite. The redirect is evaluated before the command either way,
    // which is exactly what the bug was.
    Files.writeString(
        fresh.resolve("gradlew"), "#!/usr/bin/env bash\necho GRADLE_STUB_BOOM >&2\nexit 7\n");
    fresh.resolve("gradlew").toFile().setExecutable(true);
    String out = gate(fresh, "scripts/check-harness-tests.sh");
    // ⚠️ The directory assertion is the load-bearing one: bash's redirect
    // message comes from strerror and is locale-dependent, so the string check
    // alone could pass in a non-English locale over the same bug.
    assertThat(out).as(out).doesNotContain("No such file or directory");
    // ⚠️ The FILE, not just the directory. Asserting the directory left a
    // mutation alive: keep the mkdir, redirect to /dev/null, and both the
    // message and directory assertions pass while the gate still says
    // "see .harness/harness-tests.log" about a log nothing wrote. The contract
    // is the log the message points at.
    // ⚠️ CONTENT, not existence. Keeping the mkdir, truncating the file and
    // sending Gradle to /dev/null left every other assertion green over a
    // zero-byte log -- a cited log that explains nothing is the same defect as
    // a missing one. The stub writes to stderr so 2>&1 is pinned too.
    assertThat(fresh.resolve(".harness/harness-tests.log"))
        .as("the gate cites this log; it must exist and hold the failure")
        .exists()
        .content()
        .contains("GRADLE_STUB_BOOM");
    // TR3: and the gate must actually report the stub's failure.
    assertThat(out).as(out).startsWith("1").contains("harness tests failed");
    discard(dir);
  }
}
