// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two reviewers each get a fresh tree and each ran Gradle from cold in it.
 *
 * <p>⚠️ WHAT IS BEING SHARED AND WHAT IS NOT, because getting that backwards is
 * the defect {@code review-tree.sh} exists to prevent. A per-tree {@code build/}
 * is the isolation that matters: two reviewers writing one of those clobber the
 * test-results XML that every pass/fail count reads. {@code GRADLE_USER_HOME} is
 * not that — it holds the dependency cache, the wrapper distribution and the
 * build cache, none of which is evidence, and re-fetching it per tree is pure
 * cold-start cost on a round already priced at 7–13 minutes.
 *
 * <p>⚠️ SO THE TEST THAT MATTERS IS THE ONE ASSERTING BOTH AT ONCE: same home,
 * different trees. Sharing the home is only safe while the trees stay apart, and
 * a test for either half alone would pass through the regression that matters.
 *
 * <p>⚠️ And an unused optimisation is not one: the packet has to actually tell
 * the reviewer to use it, or the shared home exists and every build still runs
 * cold.
 */
class ReviewSharedBuildTest {

  private String out(Path dir, Map<String, String> env, String... cmd) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true);
    pb.environment().putAll(env);
    Process p = pb.start();
    String s = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(String.join(" ", cmd) + "\n" + s).isZero();
    return s.trim();
  }

  /** A scratch repository with one staged file, plus its own TMPDIR. */
  private Path scratch(Path parent, String name) throws Exception {
    Path dir = parent.resolve(name);
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("review-tree.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.writeString(dir.resolve("a.txt"), "staged\n");
    Process p = new ProcessBuilder("bash", "-c",
        "git init -q . && git config user.email t@e && git config user.name t && git add -A")
        .directory(dir.toFile()).redirectErrorStream(true).start();
    assertThat(p.waitFor()).isZero();
    return dir;
  }

  private Map<String, String> tmp(Path parent, String name) throws Exception {
    Path t = parent.resolve(name);
    Files.createDirectories(t);
    return Map.of("TMPDIR", t.toAbsolutePath().toString());
  }

  /** One dependency cache, two trees. Both halves, because either alone hides the other. */
  @Test
  void bothRolesShareOneGradleHomeAndStillGetSeparateTrees(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "repo");
    Map<String, String> env = tmp(dir, "tmp");

    String homeA = out(d, env, "bash", "scripts/review-tree.sh", "--gradle-home");
    String homeB = out(d, env, "bash", "scripts/review-tree.sh", "--gradle-home");
    assertThat(homeA)
        .as("re-fetching the wrapper and every dependency per tree is pure cold-start cost")
        .isEqualTo(homeB);
    assertThat(Path.of(homeA)).exists();

    String treeA = out(d, env, "bash", "scripts/review-tree.sh", "reviewer");
    String treeB = out(d, env, "bash", "scripts/review-tree.sh", "test-reviewer");
    assertThat(treeA)
        .as("one build/ between two reviewers clobbers the test-results XML every "
            + "pass/fail count reads -- that isolation is the point of this script")
        .isNotEqualTo(treeB);
    assertThat(Path.of(treeA).resolve("a.txt")).exists();
    assertThat(Path.of(treeB).resolve("a.txt")).exists();
  }

  /**
   * ⚠️ Outside the repository, on the same reasoning as the trees: anything that
   * walks the tree rather than asking git would otherwise read a second copy of
   * the world as if it were the real one.
   */
  @Test
  void theSharedGradleHomeIsOutsideTheRepository(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "repo");
    String home = out(d, tmp(dir, "tmp"), "bash", "scripts/review-tree.sh", "--gradle-home");
    assertThat(Path.of(home).toRealPath().startsWith(d.toRealPath()))
        .as("a Gradle home inside the working tree is a second copy of the world: %s under %s",
            home, d)
        .isFalse();
  }
}
