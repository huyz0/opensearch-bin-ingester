// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * check-module.sh derives its module list from settings.gradle.kts, and a short
 * list is indistinguishable from compliance: the gate prints "ok N module(s)
 * checked" over whatever it dropped.
 *
 * Two line-based derivations shipped before this one. The first required a
 * trailing comma; the second anchored to end-of-line, so an inline comment
 * dropped the entry. Both were caught in review, and reverting either left every
 * hook green because nothing exercised the derivation. One fixture per drop.
 */
class ModuleListDerivationTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "inline-comment.kts    | binstore-spi,ingest,plugin",
        "no-trailing-comma.kts | binstore-spi,ingest,plugin",
        "two-per-line.kts      | binstore-spi,ingest,plugin",
      })
  void derivesEveryModuleRegardlessOfLineShape(String fixture, String expected) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f =
        repo.resolve("buildSrc/src/test/resources/settings-fixtures").resolve(fixture.trim());
    assertThat(f).exists();
    Process p =
        new ProcessBuilder("scripts/check-module.sh", "--list-modules", f.toString())
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
    assertThat(out.lines().map(String::trim).filter(l -> !l.isEmpty()).toList())
        .containsExactlyElementsOf(List.of(expected.trim().split(",")));
  }
}
