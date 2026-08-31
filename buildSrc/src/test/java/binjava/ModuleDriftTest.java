// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The two-way cross-check between settings.gradle.kts and the build files is
 * what makes a short module list LOUD instead of silent — the defect that let
 * check-module print "ok 7 module(s) checked" over a real Helidon dependency in
 * the module it dropped.
 *
 * It is 100% failure path, so review found it could be deleted entire with the
 * whole suite still green. These fixtures constrain it. The `clean` case
 * deliberately contains a four-character module name (`omega`), because a
 * length-based derivation is invisible to the other fixtures — every name there
 * is five characters or more — and is killed only by this check.
 */
class ModuleDriftTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "missing-build-file     | 1 | has no build.gradle.kts",
        "missing-settings-entry | 1 | not in settings",
        "clean                  | 0 | no module drift",
      })
  void refusesDriftInEitherDirection(String fixture, int expectedExit, String expectedText)
      throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path dir =
        repo.resolve("buildSrc/src/test/resources/drift-fixtures").resolve(fixture.trim());
    assertThat(dir).exists();
    Process p =
        new ProcessBuilder("scripts/check-module.sh", "--check-drift", dir.toString())
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isEqualTo(expectedExit);
    assertThat(out).contains(expectedText.trim());
  }
}
