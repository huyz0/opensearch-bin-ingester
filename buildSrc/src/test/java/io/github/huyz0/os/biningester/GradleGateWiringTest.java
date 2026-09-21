// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
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
    void wiringTestReadsTheStagedHookBlob() throws Exception {
        Process process = new ProcessBuilder("git", "show", ":.pre-commit-config.yaml")
                .directory(repository().toFile()).redirectErrorStream(true).start();
        String staged = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(process.waitFor()).isZero();
        assertThat(staged).contains("entry: ./gradlew.bat gates");
        assertThat(stagedFile("gradlew")).contains(":windows").contains("gradlew.bat");
        assertThat(stagedFile("gradlew.bat")).contains("#!/bin/sh").contains("gradlew\"");
        assertThat(stagedFile("build.gradle.kts")).contains("dependsOn(\"checkWired\", \"checkOverride\", \"checkHarnessTests\")");
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
}
