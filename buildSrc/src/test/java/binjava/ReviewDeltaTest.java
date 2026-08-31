// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The verify-round packet: what changed since the last review, and what was
 * left open.
 *
 * <p>⚠️ This REDUCES what a reviewer is told to focus on, so its failure mode is
 * a round that skips something. Two cases matter: round one must get no delta
 * (there is nothing already reviewed), and a verify round must surface every
 * blocking/major finding from earlier rounds — a finding raised and not fixed is
 * the thing a verify round exists to catch.
 */
class ReviewDeltaTest {

  private Path reviews(Path dir) throws Exception {
    Path d = dir.resolve(".harness/review");
    Files.createDirectories(d);
    Files.copy(
        Path.of("..").toAbsolutePath().normalize().resolve("scripts/review_delta.py"),
        dir.resolve("review_delta.py"));
    return d;
  }

  private void verdict(Path reviewDir, String task, String sha, String severity, String id)
      throws Exception {
    String findings =
        severity == null
            ? "[]"
            : "[{\"id\":\"" + id + "\",\"severity\":\"" + severity + "\","
                + "\"summary\":\"s\",\"failure_scenario\":\"fs\"}]";
    Files.writeString(
        reviewDir.resolve(sha + ".reviewer.json"),
        "{\"task\":\"" + task + "\",\"diff_sha256\":\"" + sha + "\","
            + "\"verdict\":\"changes-requested\",\"findings\":" + findings + "}");
  }

  private String run(Path dir, String task, String sha) throws Exception {
    Process p =
        new ProcessBuilder("python3", "review_delta.py", task, sha, ".harness/review")
            .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    return p.waitFor() + "\n" + out;
  }

  /** Round one: nothing has been reviewed, so the caller must show everything. */
  @Test
  void roundOneProducesNoDelta(@TempDir Path dir) throws Exception {
    reviews(dir);
    assertThat(run(dir, "M0.X", "a".repeat(64))).startsWith("1");
  }

  /** A verify round must name every blocking and major finding still open. */
  @Test
  void aVerifyRoundSurfacesOpenBlockingAndMajorFindings(@TempDir Path dir) throws Exception {
    Path rd = reviews(dir);
    verdict(rd, "M0.X", "a".repeat(64), "blocking", "B1");
    verdict(rd, "M0.X", "b".repeat(64), "major", "M1");
    String out = run(dir, "M0.X", "c".repeat(64));
    assertThat(out).startsWith("0");
    assertThat(out).contains("NOT ROUND ONE").contains("B1").contains("M1");
  }

  /** ⚠️ A minor is not what a verify round is for; surfacing it invites re-litigation. */
  @Test
  void doesNotResurfaceMinors(@TempDir Path dir) throws Exception {
    Path rd = reviews(dir);
    verdict(rd, "M0.X", "a".repeat(64), "minor", "N1");
    assertThat(run(dir, "M0.X", "c".repeat(64))).doesNotContain("N1");
  }

  /** Another task's findings must never leak into this one's packet. */
  @Test
  void doesNotLeakAcrossTasks(@TempDir Path dir) throws Exception {
    Path rd = reviews(dir);
    verdict(rd, "M0.OTHER", "a".repeat(64), "blocking", "OTHER1");
    assertThat(run(dir, "M0.X", "c".repeat(64))).startsWith("1");
  }
}
