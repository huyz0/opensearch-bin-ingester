// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Review packets must not rerun expensive manual gates for identical bytes. */
class ReviewPacketCostTest {

  private static final String TASK = "M0.69";

  private Path scratch(Path dir) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String file :
        List.of(
            "review.sh",
            "lib.sh",
            "review_rounds.py",
            "review_delta.py",
            "which-standards.sh",
            "review-lenses.sh")) {
      Path destination = dir.resolve("scripts").resolve(file);
      Files.copy(repo.resolve("scripts").resolve(file), destination);
      destination.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(
        dir.resolve("docs/internal/product/backlog.md"),
        "| " + TASK + " | packet cost | — | todo |\n");
    Files.writeString(
        dir.resolve(".pre-commit-config.yaml"),
        "      - id: check-mutants\n"
            + "        entry: scripts/check-mutants.sh\n"
            + "        stages:\n"
            + "          - manual\n"
            + "      - id: check-counter\n"
            + "        entry: \"scripts/check-counter.sh\"\n"
            + "        stages: [pre-commit]\n"
            + "      - id: check-both\n"
            + "        entry: scripts/check-both.sh\n"
            + "        stages:\n"
            + "          - manual\n"
            + "          - pre-commit\n"
            + "      - id: build-index\n"
            + "        entry: scripts/build-index.sh --check\n"
            + "        stages: [pre-commit]\n"
            + "      - id: check-reviewed\n"
            + "        entry: scripts/check-reviewed.sh\n"
            + "        stages: [pre-commit]\n");
    Files.createDirectories(dir.resolve(".harness"));
    Files.writeString(
        dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' counter >> .harness/invocations\n");
    Files.writeString(
        dir.resolve("scripts/check-mutants.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' mutants >> .harness/invocations\n");
    Files.writeString(
        dir.resolve("scripts/check-both.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' both >> .harness/invocations\n");
    Files.writeString(
        dir.resolve("scripts/build-index.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' index >> .harness/invocations\n");
    Files.writeString(
        dir.resolve("scripts/check-reviewed.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' self-gate-ran >> .harness/invocations\nexit 99\n");
    for (String file :
        List.of(
            "check-counter.sh",
            "check-mutants.sh",
            "check-both.sh",
            "build-index.sh",
            "check-reviewed.sh")) {
      dir.resolve("scripts").resolve(file).toFile().setExecutable(true);
    }
    Files.writeString(
        dir.resolve(".gitignore"),
        ".harness/\n"
            + "src/*.java\n"
            + "src/*.kt\n"
            + "src/*.kts\n"
            + "*/src/*.java\n"
            + "*/src/*.kt\n"
            + "*/src/*.kts\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t");
    Files.writeString(dir.resolve("subject.txt"), "staged\n");
    run(dir, "git add -A && git commit -qm base && git add -- subject.txt");
    return dir;
  }

  private String packet(Path dir) throws Exception {
    return packet(dir, new String[0]);
  }

  private String packet(Path dir, String... environment) throws Exception {
    Process process = startPacket(dir, environment);
    String output = new String(process.getInputStream().readAllBytes());
    assertThat(process.waitFor()).as(output).isZero();
    return output;
  }

  private int packetExit(Path dir) throws Exception {
    return packetExit(dir, new String[0]);
  }

  private int packetExit(Path dir, String... environment) throws Exception {
    Process process = startPacket(dir, environment);
    process.getInputStream().readAllBytes();
    return process.waitFor();
  }

  private Process startPacket(Path dir) throws Exception {
    return startPacket(dir, new String[0]);
  }

  private Process startPacket(Path dir, String... environment) throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder("bash", "scripts/review.sh", "context", "--task", TASK)
            .directory(dir.toFile())
            .redirectErrorStream(true);
    for (int i = 0; i < environment.length; i += 2) {
      builder.environment().put(environment[i], environment[i + 1]);
    }
    return builder.start();
  }

  private void run(Path dir, String command) throws Exception {
    Process process =
        new ProcessBuilder("bash", "-c", "set -o pipefail; " + command)
            .directory(dir.toFile())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes());
    assertThat(process.waitFor()).as(command + "\n" + output).isZero();
  }

  @Test
  void manualMutationGateIsNotRunAndPassedGatesAreCached(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.write(dir.resolve("payload.bin"), new byte[] {0, 1, 2});
    run(dir, "git add -- payload.bin");

    packet(dir);
    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index");

    packet(dir);
    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index");

    Files.write(dir.resolve("payload.bin"), new byte[] {0, 1, 3});
    run(dir, "git add -- payload.bin");
    packet(dir);
    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void aFailedGateIsNotCached(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\n"
            + "if [ ! -f .harness/failure-seen ]; then touch .harness/failure-seen; exit 1; fi\n"
            + "printf '%s\\n' counter >> .harness/invocations\n");

    assertThat(packetExit(dir)).isNotZero();
    assertThat(packetExit(dir)).isZero();
    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "both", "index");
  }

  @Test
  void concurrentPacketsShareOneGateRun(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\n"
            + "touch .harness/started\n"
            + "sleep 1\n"
            + "printf '%s\\n' counter >> .harness/invocations\n");

    Process first = startPacket(dir);
    for (int i = 0; i < 100 && !Files.exists(dir.resolve(".harness/started")); i++) {
      Thread.sleep(20);
    }
    Process second = startPacket(dir);
    String firstOutput = new String(first.getInputStream().readAllBytes());
    String secondOutput = new String(second.getInputStream().readAllBytes());
    assertThat(first.waitFor()).isZero();
    assertThat(second.waitFor()).as(secondOutput + "\nfirst:\n" + firstOutput).isZero();
    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index");
  }

  @Test
  void gateLockHasABoundedWait(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\ntouch .harness/started\nsleep 2\nprintf '%s\\n' counter >> .harness/invocations\n");

    Process first = startPacket(dir);
    for (int i = 0; i < 100 && !Files.exists(dir.resolve(".harness/started")); i++) {
      Thread.sleep(20);
    }
    Process second = startPacket(dir, "REVIEW_GATE_LOCK_TIMEOUT", "0");
    second.getInputStream().readAllBytes();
    first.getInputStream().readAllBytes();
    assertThat(second.waitFor(5, TimeUnit.SECONDS)).isTrue();
    assertThat(first.waitFor(5, TimeUnit.SECONDS)).isTrue();
    assertThat(second.exitValue()).isNotZero();
    assertThat(first.exitValue()).isZero();
  }

  @Test
  void aCachePublicationFailureDoesNotPass(@TempDir Path dir) throws Exception {
    scratch(dir);
    Path bin = dir.resolve("bin");
    Files.createDirectories(bin);
    Path failingMv = bin.resolve("mv");
    Files.writeString(failingMv, "#!/usr/bin/env bash\nexit 1\n");
    failingMv.toFile().setExecutable(true);

    assertThat(packetExit(dir, "PATH", bin + ":" + System.getenv("PATH"))).isNotZero();
    try (var files = Files.list(dir.resolve(".harness/review"))) {
      assertThat(files.filter(path -> path.toString().endsWith(".gates")).count()).isZero();
    }
  }

  @Test
  void aGateScriptChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.writeString(
        dir.resolve("scripts/check-counter.sh"),
        "#!/usr/bin/env bash\nprintf '%s\\n' changed >> .harness/invocations\n");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "changed", "both", "index");
  }

  @Test
  void executionScopeChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir, "GATE_SCOPE", "delta");
    packet(dir, "GATE_SCOPE", "full");

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void sharedGateDependencyChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.writeString(
        dir.resolve("scripts/review_delta.py"),
        Files.readString(dir.resolve("scripts/review_delta.py")) + "\n# changed dependency\n");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void configAndCheckRangeChangesInvalidateTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.writeString(
        dir.resolve(".pre-commit-config.yaml"),
        Files.readString(dir.resolve(".pre-commit-config.yaml")) + "# changed config\n");
    packet(dir);
    packet(dir, "CHECK_RANGE", "HEAD");

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder(
            "counter", "both", "index", "counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void tddEvidenceChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.createDirectories(dir.resolve(".harness/tdd"));
    Files.writeString(dir.resolve(".harness/tdd/evidence"), "changed\n");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void reviewEvidenceChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.writeString(dir.resolve(".harness/review/other-verdict.json"), "changed\n");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void executableModeChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    run(dir, "chmod 744 scripts/check-counter.sh");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void ignoredSourceChangeInvalidatesTheCache(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Files.createDirectories(dir.resolve("src"));
    Files.writeString(dir.resolve("src/ignored.java"), "class Ignored {}\n");
    packet(dir);
    Files.createDirectories(dir.resolve("module/src"));
    Files.writeString(dir.resolve("module/src/ignored.java"), "class NestedIgnored {}\n");
    packet(dir);
    Files.writeString(dir.resolve("module/src/ignored.kt"), "class NestedIgnoredKt\n");
    packet(dir);
    Files.writeString(dir.resolve("module/src/ignored.kts"), "val nestedIgnored = true\n");
    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder(
            "counter", "both", "index", "counter", "both", "index", "counter", "both", "index",
            "counter", "both", "index", "counter", "both", "index");
  }

  @Test
  void configuredHookWithArgumentsIsRun(@TempDir Path dir) throws Exception {
    scratch(dir);
    Files.writeString(
        dir.resolve("scripts/check-args.sh"),
        "#!/usr/bin/env bash\n"
            + "[ \"$1\" = --mode ] && [ \"$2\" = full ] || exit 2\n"
            + "printf '%s\\n' args >> .harness/invocations\n");
    dir.resolve("scripts/check-args.sh").toFile().setExecutable(true);
    Files.writeString(
        dir.resolve(".pre-commit-config.yaml"),
        Files.readString(dir.resolve(".pre-commit-config.yaml"))
            + "\n"
            + "      - id: check-args\n"
            + "        entry: scripts/check-args.sh --mode full\n"
            + "        stages: [pre-commit]\n");

    packet(dir);

    assertThat(Files.readAllLines(dir.resolve(".harness/invocations")))
        .containsExactlyInAnyOrder("counter", "both", "index", "args");
  }

  @Test
  void aCorruptedCacheIsRejected(@TempDir Path dir) throws Exception {
    scratch(dir);
    packet(dir);
    Path cache;
    try (var files = Files.list(dir.resolve(".harness/review"))) {
      cache = files.filter(path -> path.toString().endsWith(".gates")).findFirst().orElseThrow();
    }
    Files.writeString(cache, "truncated\n");

    assertThat(packetExit(dir)).isNotZero();
  }
}
