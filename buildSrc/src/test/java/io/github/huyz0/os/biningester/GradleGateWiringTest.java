// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class GradleGateWiringTest {
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }

    @Test
    void preCommitUsesGradleAndDoesNotLaunchLegacyInterpreters() throws Exception {
        String hooks = stagedFile(".pre-commit-config.yaml");

        assertThat(hooks).contains("entry: ./gradlew.bat gates");
        assertThat(hooks).contains("entry: ./gradlew.bat checkReviewed");
        assertThat(hooks).contains("entry: ./gradlew.bat checkTdd");
        assertThat(hooks).contains("entry: ./gradlew.bat checkTestIntegrity");
        assertThat(hooks).contains("entry: ./gradlew.bat checkMutants");
        assertThat(hooks).contains("entry: ./gradlew.bat checkCommitMessage");
        assertThat(hooks).doesNotContain("scripts/");
        assertThat(hooks).doesNotContain("python ");
        assertThat(hooks).doesNotContain("bash ");
    }

    @Test
    void l1JobRunsProductTestsAndRefusesZeroResults() throws Exception {
        String workflow = stagedFile(".github/workflows/ci.yml");

        assertThat(workflow)
                .contains("name: \"L1 — unit tests\"")
                .contains("run: ./gradlew test --no-daemon")
                .contains("        run: python scripts/check-l1-tests.py\n");
        assertThat(workflow.substring(workflow.indexOf("\n  l1:")))
                .doesNotContain("continue-on-error:", "\n    if:", "\n        if:");
    }

    @Test
    void measurementWorkflowRequiresTheNightlySoakJob() throws Exception {
        Path root = repository();
        Path workflow = root.resolve(".github/workflows/measurement.yml");
        Run valid = checkMeasurementWorkflow(workflow);
        assertThat(valid.exitCode()).isZero();
        assertThat(valid.output()).contains("scheduled L2S soak job");

        Path mutated = root.resolve("buildSrc/build/tmp/measurement-workflow")
                .resolve(UUID.randomUUID().toString() + ".yml");
        Files.createDirectories(mutated.getParent());
        Files.writeString(mutated, Files.readString(workflow)
                .replace("        run: ./gradlew soakTest --no-daemon\n", ""));
        Run missingSoak = checkMeasurementWorkflow(mutated);
        assertThat(missingSoak.exitCode()).isEqualTo(1);
        assertThat(missingSoak.output()).contains("soak");

        String original = Files.readString(workflow);
        Files.writeString(mutated, original.replace("'17 3 * * *'", "'17 3 1 1 *'"));
        Run nonNightly = checkMeasurementWorkflow(mutated);
        assertThat(nonNightly.exitCode()).isEqualTo(1);
        assertThat(nonNightly.output()).contains("nightly");

        Files.writeString(mutated, original.replace("  soak:\n", "  soak:\n    if: false\n"));
        Run disabledSoak = checkMeasurementWorkflow(mutated);
        assertThat(disabledSoak.exitCode()).isEqualTo(1);
        assertThat(disabledSoak.output()).contains("conditionally disabled");

        Files.writeString(mutated, original.replace("        run: ./gradlew soakTest --no-daemon\n",
                "        if: false\n        run: ./gradlew soakTest --no-daemon\n"));
        Run disabledStep = checkMeasurementWorkflow(mutated);
        assertThat(disabledStep.exitCode()).isEqualTo(1);
        assertThat(disabledStep.output()).contains("conditionally disabled");

        Files.writeString(mutated, original.replace("    timeout-minutes: 10", "    timeout-minutes: 11"));
        Run excessiveTimeout = checkMeasurementWorkflow(mutated);
        assertThat(excessiveTimeout.exitCode()).isEqualTo(1);
        assertThat(excessiveTimeout.output()).contains("ten-minute");

        Files.writeString(mutated, original.replace("        run: ./gradlew soakTest --no-daemon\n",
                "        run: ./gradlew soakTest --no-daemon\n        continue-on-error: ${{ true }}\n"));
        Run ignoredSoakFailure = checkMeasurementWorkflow(mutated);
        assertThat(ignoredSoakFailure.exitCode()).isEqualTo(1);
        assertThat(ignoredSoakFailure.output()).contains("ignore a failed soakTest step");
    }

    @Test
    void wiringTestReadsTheStagedHookBlob() throws Exception {
        Process process = new ProcessBuilder("git", "show", ":.pre-commit-config.yaml")
                .directory(repository().toFile()).redirectErrorStream(true).start();
        String staged = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(process.waitFor()).isZero();
        assertThat(staged).contains("entry: ./gradlew.bat gates");
        assertThat(stagedFile("gradlew")).contains(":windows").contains("gradlew.bat");
        assertThat(stagedFile("gradlew.bat")).contains("#!/bin/sh").contains("gradlew\"");
        assertThat(stagedFile("build.gradle.kts")).contains("dependsOn(\"checkWired\", \"checkOverride\", \"checkHarnessTests\")");
        assertThat(stagedFile("build.gradle.kts")).contains("tasks.named(\"gates\") { dependsOn(\"checkCostLatencyCurve\") }");
    }

    @Test
    void wrapperActuallyLaunchesOnThisOperatingSystem() throws Exception {
        String[] command = System.getProperty("os.name").toLowerCase().contains("win")
                ? new String[]{"cmd", "/c", "gradlew.bat", "--version"}
                : new String[]{"sh", "gradlew.bat", "--version"};
        Process process = new ProcessBuilder(command).directory(repository().toFile()).redirectErrorStream(true).start();
        assertThat(process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(process.exitValue()).isZero();
    }

    private static String stagedFile(String path) throws Exception {
        Process process = new ProcessBuilder("git", "show", ":" + path).directory(repository().toFile()).start();
        String text = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(process.waitFor()).isZero();
        return text;
    }

    private static Run checkMeasurementWorkflow(Path workflow) throws Exception {
        Process process = ProcessSupport.builder("python", "scripts/check-measurement-workflow.py",
                "--workflow", workflow.toString()).directory(repository().toFile())
                .redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        return new Run(process.waitFor(), output);
    }

    private record Run(int exitCode, String output) {}
}
