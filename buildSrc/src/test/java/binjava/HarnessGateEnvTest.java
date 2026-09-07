// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code check-harness-tests.sh} must hand its suite NO inherited git state
 * (M0.57).
 *
 * <p>⚠️ This is the line that had no test at all. {@code review.sh context} runs
 * the whole gate loop, so generating a review packet under a private index used
 * to run the buildSrc suite with that index INHERITED — and harness tests shell
 * out to {@code git add} inside their own temp repositories, writing THEIR
 * fixture paths into it. Measured repeatedly on this task: a private index going
 * from hundreds of entries to a handful, with the suite reporting BUILD
 * SUCCESSFUL each time.
 *
 * <p>⚠️ {@link GitEnv} does NOT cover this. It strips the environment of
 * children spawned by {@link ReviewTreeTest} only; every other harness suite
 * inherits whatever the gate hands Gradle, so this one shell line is the sole
 * protection. Round-4 test review measured reverting it and leaving the entire
 * buildSrc suite green.
 *
 * <p>Kept out of {@link ReviewTreeTest} because it is about a different script,
 * and because that file is near the 500-line limit.
 */
class HarnessGateEnvTest {

  @Test
  void theHarnessGateRunsItsSuiteWithNOInheritedGitState(@TempDir Path dir) throws Exception {
    // ⚠️ EVERY `GIT_*`, not the three obvious ones -- the same predicate as the
    // strip in `review-tree.sh`, which is why `GIT_OBJECT_DIRECTORY` is among
    // the decoys: it alone redirects a child's objects into the caller's store,
    // and an explicit three-key list would leave it inherited.
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("check-harness-tests.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    // A stub Gradle that records the environment it was handed, so the
    // assertion is about what the CHILD received rather than about the text of
    // the line that spawned it.
    Path gradlew = dir.resolve("gradlew");
    Files.writeString(gradlew, "#!/usr/bin/env bash\nenv | grep '^GIT_' || echo NO_GIT_VARS\n");
    gradlew.toFile().setExecutable(true);
    Process init = GitEnv.stripped(new ProcessBuilder("bash", "-c",
        "git init -q . && git config user.email t@e && git config user.name t"
            + " && git add -A && git commit -qm base")
        .directory(dir.toFile())).redirectErrorStream(true).start();
    assertThat(init.waitFor()).as("fixture repository must initialise").isZero();

    ProcessBuilder b = GitEnv.stripped(
        new ProcessBuilder("bash", "scripts/check-harness-tests.sh").directory(dir.toFile()));
    b.environment().put("GATE_SCOPE", "full");          // skip the delta short-circuit
    b.environment().put("GIT_INDEX_FILE", dir.resolve("decoy.idx").toString());
    b.environment().put("GIT_DIR", dir.resolve(".git").toString());
    b.environment().put("GIT_OBJECT_DIRECTORY", dir.resolve("decoy-objects").toString());
    b.redirectErrorStream(true).start().waitFor();

    assertThat(Files.readString(dir.resolve(".harness/harness-tests.log")))
        .as("the suite must be handed no git state at all, or its fixtures "
            + "write into the caller's index")
        .contains("NO_GIT_VARS");
  }

  /**
   * ⚠️ AND THE GATE-SCOPING VARIABLES, for the same reason as {@code GIT_*}.
   * CI exports {@code GATE_SCOPE=full} and {@code CHECK_RANGE=<base sha>} for
   * the whole Gates step, and this suite runs inside it. Harness tests spawn
   * the real gate scripts against their OWN scratch repositories, where that
   * sha does not exist — so every gate that reads a range dies with
   * {@code fatal: ambiguous argument}.
   *
   * <p>MEASURED: 31 of them, across 8 classes, the first time CI ever reached
   * this step. It never had before, because `dependencyLicenses` failed earlier
   * on all 10 previous runs. Fixing it per-test is 31 fixes and one forgotten
   * test away from returning; stripping it here is one line that cannot be
   * forgotten.
   */
  @Test
  void theSuiteIsHandedNoInheritedGATESCOPEOrCHECKRANGE(@TempDir Path dir) throws Exception {
    String script = Files.readString(Path.of("..", "scripts", "check-harness-tests.sh"));

    int envLine = script.indexOf("./gradlew -p buildSrc test");
    assertThat(envLine).as("the suite invocation must be findable").isGreaterThan(0);
    String invocation = script.substring(Math.max(0, envLine - 400), envLine);

    assertThat(invocation)
        .as("GATE_SCOPE must be stripped before the suite runs:\n%s", invocation)
        .contains("GATE_SCOPE");
    assertThat(invocation)
        .as("CHECK_RANGE must be stripped before the suite runs:\n%s", invocation)
        .contains("CHECK_RANGE");
  }
}
