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
    void measurementWorkflowRequiresFullAndFastCostJobs() throws Exception {
        Path root = repository();
        Path measurement = root.resolve(".github/workflows/measurement.yml");
        Path ci = root.resolve(".github/workflows/ci.yml");
        Run valid = checkMeasurementWorkflow(measurement, ci);
        assertThat(valid.exitCode()).isZero();
        assertThat(valid.output()).contains("full cost points", "fast L1 cost subset");

        Path fixture = root.resolve("buildSrc/build/tmp/measurement-ci")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        Path missingNightly = fixture.resolve("measurement.yml");
        Files.writeString(missingNightly, Files.readString(measurement)
                .replace("  cost:\n", "  cost_job_removed:\n"));
        Run noCostJob = checkMeasurementWorkflow(missingNightly, ci);
        assertThat(noCostJob.exitCode()).isEqualTo(1);
        assertThat(noCostJob.output()).contains("cost");

        Path missingFast = fixture.resolve("ci.yml");
        Files.writeString(missingFast, Files.readString(ci)
                .replace("./gradlew :server:integrationTest", "./gradlew :server:test"));
        Run noFastSubset = checkMeasurementWorkflow(measurement, missingFast);
        assertThat(noFastSubset.exitCode()).isEqualTo(1);
        assertThat(noFastSubset.output()).contains("fast");

        Path conditionalFast = fixture.resolve("ci-conditional.yml");
        Files.writeString(conditionalFast, Files.readString(ci)
                .replace("      - name: Cost assertions (fast subset)\n",
                        "      - name: Cost assertions (fast subset)\n        if: false\n"));
        Run skippedFast = checkMeasurementWorkflow(measurement, conditionalFast);
        assertThat(skippedFast.exitCode()).isEqualTo(1);
        assertThat(skippedFast.output()).contains("conditional");

        Path conditionalRefusal = fixture.resolve("ci-conditional-refusal.yml");
        Files.writeString(conditionalRefusal, Files.readString(ci)
                .replace("      - name: Refuse skipped fast cost tests\n",
                        "      - name: Refuse skipped fast cost tests\n        if: false\n"));
        Run skippedRefusal = checkMeasurementWorkflow(measurement, conditionalRefusal);
        assertThat(skippedRefusal.exitCode()).isEqualTo(1);
        assertThat(skippedRefusal.output()).contains("conditional");

        Path conditionalNightly = fixture.resolve("measurement-conditional-cost.yml");
        Files.writeString(conditionalNightly, Files.readString(measurement)
                .replace("  cost:\n", "  cost:\n    if: false\n"));
        Run skippedNightly = checkMeasurementWorkflow(conditionalNightly, ci);
        assertThat(skippedNightly.exitCode()).isEqualTo(1);
        assertThat(skippedNightly.output()).contains("conditional");

        Path conditionalNightlyRefusal = fixture.resolve("measurement-conditional-refusal.yml");
        Files.writeString(conditionalNightlyRefusal, Files.readString(measurement)
                .replace("      - name: Refuse skipped full cost tests\n",
                        "      - name: Renamed skipped-test check\n"
                                + "        if: github.event_name == 'workflow_dispatch'\n"));
        Run skippedNightlyRefusal = checkMeasurementWorkflow(conditionalNightlyRefusal, ci);
        assertThat(skippedNightlyRefusal.exitCode()).isEqualTo(1);
        assertThat(skippedNightlyRefusal.output()).contains("conditional");

        Path ignoredNightlyRefusal = fixture.resolve("measurement-continue-on-error.yml");
        Files.writeString(ignoredNightlyRefusal, Files.readString(measurement)
                .replace("      - name: Refuse skipped full cost tests\n",
                        "      - name: Renamed ignored refusal\n"
                                + "        continue-on-error: true\n"));
        Run ignoredRefusal = checkMeasurementWorkflow(ignoredNightlyRefusal, ci);
        assertThat(ignoredRefusal.exitCode()).isEqualTo(1);
        assertThat(ignoredRefusal.output()).contains("gate the measurement");
    }

    @Test
    void costResultCheckRequiresEveryNamedTestToExecute() throws Exception {
        Path root = repository();
        Path fixture = root.resolve("buildSrc/build/tmp/cost-results")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        Run empty = checkCostResults(fixture, "fast");
        assertThat(empty.exitCode()).isEqualTo(1);
        assertThat(empty.output()).contains("WriteRequestRateIT");

        Path report = fixture.resolve("TEST-write.xml");
        Files.writeString(report, "<testsuite tests=\"1\" skipped=\"1\"><testcase "
                + "classname=\"io.github.huyz0.os.biningester.server.chaos.WriteRequestRateIT\" "
                + "name=\"sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates(RatePoint) at "
                + "RatePoint[mibPerSecond=40.0] MiB/s\"><skipped/>"
                + "</testcase></testsuite>");
        Run skipped = checkCostResults(fixture, "fast");
        assertThat(skipped.exitCode()).isEqualTo(1);
        assertThat(skipped.output()).contains("skipped");

        Files.writeString(report, "<testsuite tests=\"3\"><testcase "
                + "classname=\"io.github.huyz0.os.biningester.server.chaos.WriteRequestRateIT\" "
                + "name=\"sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates(RatePoint) at "
                + "RatePoint[mibPerSecond=40.0] MiB/s\"/>"
                + "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.ReadRequestRateIT\" "
                + "name=\"storeGetsStayFlatAcrossConsumerNodesAndShards\"/>"
                + "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.CrossAzBytesIT\" "
                + "name=\"crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget\"/>"
                + "</testsuite>");
        Run executed = checkCostResults(fixture, "fast");
        assertThat(executed.exitCode()).isZero();
        assertThat(executed.output()).contains("fast cost tests executed");

        Files.writeString(report, Files.readString(report).replace("mibPerSecond=40.0",
                "mibPerSecond=80.0"));
        Run wrongFastRate = checkCostResults(fixture, "fast");
        assertThat(wrongFastRate.exitCode()).isEqualTo(1);
        assertThat(wrongFastRate.output()).contains("required M9.8 rate points");

        String fullReportsWithoutLowRate = "<testsuite tests=\"5\">"
                + writeRateCases(3)
                + "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.ReadRequestRateIT\" "
                + "name=\"storeGetsStayFlatAcrossConsumerNodesAndShards\"/>"
                + "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.CrossAzBytesIT\" "
                + "name=\"crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget\"/>"
                + "</testsuite>";
        Files.writeString(report, fullReportsWithoutLowRate);
        Run missingLowRate = checkCostResults(fixture, "full");
        assertThat(missingLowRate.exitCode()).isEqualTo(1);
        assertThat(missingLowRate.output()).contains("LowRateWriteBudgetIT");

        String fullReportsWithTwoRates = fullReportsWithoutLowRate
                .replace("<testsuite tests=\"5\">", "<testsuite tests=\"4\">")
                .replace(writeRateCases(3), writeRateCases(2))
                .replace("</testsuite>", "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.LowRateWriteBudgetIT\" "
                        + "name=\"lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings\"/>"
                        + "</testsuite>");
        Files.writeString(report, fullReportsWithTwoRates);
        Run missingRates = checkCostResults(fixture, "full");
        assertThat(missingRates.exitCode()).isEqualTo(1);
        assertThat(missingRates.output()).contains("expected 3 executed point(s), found 2");

        String duplicateRates = fullReportsWithoutLowRate
                .replace(writeRateCases(3), writeRateCases(1).repeat(3))
                .replace("</testsuite>", "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.LowRateWriteBudgetIT\" "
                        + "name=\"lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings\"/>"
                        + "</testsuite>");
        Files.writeString(report, duplicateRates);
        Run missingDistinctRates = checkCostResults(fixture, "full");
        assertThat(missingDistinctRates.exitCode()).isEqualTo(1);
        assertThat(missingDistinctRates.output()).contains("required M9.8 rate points");

        Files.writeString(report, fullReportsWithoutLowRate.replace("</testsuite>",
                "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.LowRateWriteBudgetIT\" "
                        + "name=\"lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings\"/>"
                        + "</testsuite>"));
        Run fullExecuted = checkCostResults(fixture, "full");
        assertThat(fullExecuted.exitCode()).isZero();
        assertThat(fullExecuted.output()).contains("full cost tests executed");

        Files.writeString(report, fullReportsWithoutLowRate.replace("</testsuite>",
                "<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.LowRateWriteBudgetIT\" "
                        + "name=\"lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings\"><skipped/>"
                        + "</testcase></testsuite>"));
        Run skippedLowRate = checkCostResults(fixture, "full");
        assertThat(skippedLowRate.exitCode()).isEqualTo(1);
        assertThat(skippedLowRate.output()).contains("skipped");
    }

    private static String writeRateCases(int count) {
        StringBuilder cases = new StringBuilder();
        int[] rates = {40, 80, 160};
        for (int i = 0; i < count; i++) {
            cases.append("<testcase classname=\"io.github.huyz0.os.biningester.server.chaos.WriteRequestRateIT\" ")
                    .append("name=\"sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates(RatePoint) at ")
                    .append("RatePoint[mibPerSecond=").append(rates[i]).append(".0] MiB/s\"/>");
        }
        return cases.toString();
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

    private static Run checkMeasurementWorkflow(Path workflow, Path ci) throws Exception {
        Process process = ProcessSupport.builder("python", "scripts/check-measurement-workflow.py",
                "--workflow", workflow.toString(), "--ci", ci.toString())
                .directory(repository().toFile()).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        return new Run(process.waitFor(), output);
    }

    private static Run checkCostResults(Path results, String profile) throws Exception {
        Process process = ProcessSupport.builder("python", "scripts/check-cost-test-results.py",
                "--results", results.toString(), "--profile", profile)
                .directory(repository().toFile()).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        return new Run(process.waitFor(), output);
    }

    private record Run(int exitCode, String output) {}
}
