// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Which reviewer roles a diff summons.
 *
 * <p>⚠️ This routes a GATE, so getting it wrong loosens review silently. The
 * failure that matters is a code change routed to one reviewer: test weakness is
 * invisible to every other gate in this repository, so a production diff that
 * never reaches a test-reviewer is unreviewed in the one dimension nothing else
 * covers.
 *
 * <p>The opposite failure — a docs diff routed to two — is what this replaces:
 * a test-reviewer with nothing to evaluate either files a vacuous verdict or
 * blocks the commit, and a vacuous verdict is indistinguishable downstream from
 * one over a diff nobody read.
 */
class ReviewRolesTest {

  private String roles(Path dir, String path) throws Exception {
    Path f = dir.resolve(path);
    Files.createDirectories(f.getParent() == null ? dir : f.getParent());
    Files.writeString(f, "x\n");
    run(dir, "git add -A -- '" + path + "'");
    Process p =
        new ProcessBuilder("bash", "scripts/review-roles.sh").directory(dir.toFile()).start();
    String out = new String(p.getInputStream().readAllBytes());
    p.waitFor();
    return out.replace("\n", " ").trim();
  }

  private void run(Path dir, String script) throws Exception {
    Process p =
        new ProcessBuilder("bash", "-c", "set -o pipefail; " + script)
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  private Path scratch(Path dir) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("review-roles.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    return dir;
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        // documentation and config carry nothing a test could constrain
        "docs/internal/product/backlog.md | one",
        "README.md                        | one",
        "licenses/SPDX.txt                | one",
        // anything executable, or anything a test constrains, needs both
        "binstore-spi/src/main/java/binjava/binstore/X.java | two",
        "binstore-spi/src/test/java/binjava/binstore/XTest.java | two",
        "scripts/check-links.sh           | two",
        "scripts/coverage.py              | two",
        "buildSrc/build.gradle.kts        | two",
        ".github/workflows/ci.yml         | two",
        ".pre-commit-config.yaml          | two",
      })
  void routesByWhatTheDiffContains(String path, String expected, @TempDir Path dir)
      throws Exception {
    String out = roles(scratch(dir), path.trim());
    if (expected.trim().equals("two")) {
      assertThat(out).as("%s must summon both\n%s", path, out).contains("test-reviewer");
    } else {
      assertThat(out).as("%s must summon the reviewer only\n%s", path, out)
          .isEqualTo("reviewer");
    }
  }
}
