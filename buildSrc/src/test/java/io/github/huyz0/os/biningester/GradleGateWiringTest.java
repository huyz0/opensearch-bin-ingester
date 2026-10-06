// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    /**
     * The nightly SIGTERM shutdown job (M13.56) is required, blocking and
     * refused when a case did not run (M13.60): it is the only run of the
     * shutdown hook and the drain anywhere, so losing it loses them.
     */
    @Test
    void measurementWorkflowRequiresTheBlockingShutdownJob() throws Exception {
        Path root = repository();
        Path measurement = root.resolve(".github/workflows/measurement.yml");
        Path ci = root.resolve(".github/workflows/ci.yml");
        String original = Files.readString(measurement);
        Path fixture = root.resolve("buildSrc/build/tmp/measurement-l2")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);

        String refusal = "      - name: Refuse a shutdown case that did not run\n"
                + "        run: |\n"
                + "          for c in ConfigExitCodeIT ShutdownDrainIT; do\n";
        String header = "  shutdown:\n    name: \"L2 — SIGTERM shutdown\"\n    runs-on: ubuntu-latest\n"
                + "    timeout-minutes: 10\n";
        // each mutation, and the refusal that must name it (M13.60 review T4)
        Map<String, String[]> broken = new LinkedHashMap<>();
        broken.put("removed", new String[] {
            original.replace("  shutdown:\n", "  shutdown_removed:\n"),
            "must define the shutdown job"});
        broken.put("step-ignored", new String[] {original.replace(
                "      - name: Shutdown hook and SIGTERM drain\n",
                "      - name: Shutdown hook and SIGTERM drain\n        continue-on-error: true\n"),
            "fail the measurement workflow"});
        broken.put("job-ignored", new String[] {
            original.replace("  shutdown:\n", "  shutdown:\n    continue-on-error: true\n"),
            "fail the measurement workflow"});
        broken.put("step-conditional", new String[] {original.replace(
                "      - name: Refuse a shutdown case that did not run\n",
                "      - name: Refuse a shutdown case that did not run\n        if: false\n"),
            "conditionally skipped"});
        broken.put("job-conditional", new String[] {
            original.replace("  shutdown:\n", "  shutdown:\n    if: false\n"),
            "conditionally skipped"});
        broken.put("refusal-emptied", new String[] {original.replace(refusal,
                "      - name: Refuse a shutdown case that did not run\n        run: |\n"
                        + "          true\n          for c in nothing; do\n"),
            "refuse a case that did not run"});
        broken.put("refusal-one-class", new String[] {original.replace(
                "          for c in ConfigExitCodeIT ShutdownDrainIT; do\n",
                "          for c in ConfigExitCodeIT; do\n"),
            "refuse a case that did not run"});
        broken.put("refusal-no-result-check", new String[] {original.replace(
                "            test -f \"$f\" || { echo \"::error::$c did not run\"; exit 1; }\n", ""),
            "refuse a case that did not run"});
        broken.put("refusal-no-skip-check", new String[] {original.replace(
                "            grep -q 'skipped=\"0\"' \"$f\" || { echo \"::error::$c skipped a case\";"
                        + " exit 1; }\n", ""),
            "refuse a case that did not run"});
        broken.put("step-first-key-conditional", new String[] {original.replace(
                "      - name: Refuse a shutdown case that did not run\n",
                "      - if: false\n        name: Refuse a shutdown case that did not run\n"),
            "conditionally skipped"});
        broken.put("no-config-exit", new String[] {original.replace(
                "          --tests '*server.ConfigExitCodeIT'\n", ""),
            "must run ConfigExitCodeIT"});
        broken.put("no-shutdown-drain", new String[] {original.replace(
                "          --tests '*server.ShutdownDrainIT'\n", ""),
            "must run ShutdownDrainIT"});
        broken.put("timeout-100", new String[] {original.replace(header,
                header.replace("timeout-minutes: 10\n", "timeout-minutes: 100\n")),
            "ten-minute"});
        broken.put("timeout-at-a-step", new String[] {original.replace(header,
                header.replace("    timeout-minutes: 10\n", "")).replace(
                "      - name: Shutdown hook and SIGTERM drain\n",
                "      - name: Shutdown hook and SIGTERM drain\n        timeout-minutes: 10\n"),
            "ten-minute"});
        for (Map.Entry<String, String[]> each : broken.entrySet()) {
            String workflowText = each.getValue()[0];
            assertThat(workflowText).as("the premise: %s mutates", each.getKey())
                    .isNotEqualTo(original);
            Path workflow = fixture.resolve(each.getKey() + ".yml");
            Files.writeString(workflow, workflowText);
            Run run = checkMeasurementWorkflow(workflow, ci);
            assertThat(run.exitCode()).as("%s: %s", each.getKey(), run.output()).isEqualTo(1);
            // ⚠️ THE CHECK's OWN WORDS FOR THIS FIXTURE, NOT A CRASH's: a
            // traceback also exits 1 and can name the job it fell over in.
            assertThat(run.output()).as(each.getKey()).contains(each.getValue()[1])
                    .doesNotContain("Traceback");
        }
        Path valid = fixture.resolve("valid.yml");
        Files.writeString(valid, original);
        Run alone = checkMeasurementWorkflow(valid);
        assertThat(alone.exitCode()).as("checked without --ci too: %s", alone.output()).isZero();
        assertThat(alone.output()).contains("SIGTERM shutdown job");
        // ⚠️ AND REFUSED WITHOUT --ci (its review round 2, T6): the nightly job
        // is checked whether or not the CI workflow is.
        Run removedAlone = checkMeasurementWorkflow(fixture.resolve("removed.yml"));
        assertThat(removedAlone.exitCode()).as(removedAlone.output()).isEqualTo(1);
        assertThat(removedAlone.output()).contains("must define the shutdown job");
    }

    /**
     * CI's cache steps (M13.63; M13.61, M13.62): the dependency save under
     * the restore's own key, L1 restoring both caches under the keys
     * gradle-deps fills, and gradle-deps compiling only -- each of which
     * regressed, or could, with every other gate green.
     */
    @Test
    void ciWorkflowPinsItsCacheSteps() throws Exception {
        Path root = repository();
        Path measurement = root.resolve(".github/workflows/measurement.yml");
        String original = Files.readString(root.resolve(".github/workflows/ci.yml"));
        Path fixture = root.resolve("buildSrc/build/tmp/ci-cache")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        String depsKey = "key: gradle-deps-${{ runner.os }}-${{ hashFiles('**/*.gradle.kts', "
                + "'gradle/wrapper/gradle-wrapper.properties', 'gradle/libs.versions.toml') }}";
        String buildKey = "key: gradle-build-${{ runner.os }}-${{ github.sha }}";

        Map<String, String[]> broken = new LinkedHashMap<>();
        broken.put("deps-job-removed", new String[] {
            original.replace("  gradle-deps:\n", "  gradle-deps-removed:\n"),
            "must define the gradle-deps job"});
        broken.put("save-key-recomputed", new String[] {original.replace(
                "          key: ${{ steps.gradle-deps.outputs.cache-primary-key }}\n",
                "          " + depsKey + "\n"),
            "the restore's cache-primary-key"});
        broken.put("deps-runs-build", new String[] {original.replace(
                "        run: ./gradlew testClasses integrationTestClasses --no-daemon\n",
                "        run: ./gradlew build --no-daemon\n"),
            "compile only"});
        // ⚠️ COMPILE *ONLY* (its review round 1, T1): the compile kept, a
        // step that runs tests added beside it -- in both of YAML's forms.
        String compile = "        run: ./gradlew testClasses integrationTestClasses --no-daemon\n";
        broken.put("deps-adds-a-build-step", new String[] {original.replace(compile,
                compile + "      - name: And the tests\n        run: ./gradlew build --no-daemon\n"),
            "compile only"});
        broken.put("deps-adds-a-dash-run-step", new String[] {original.replace(compile,
                compile + "      - run: ./gradlew test --no-daemon\n"),
            "compile only"});
        // (its review round 1, T2) the dependency restore step deleted
        broken.put("deps-restore-removed", new String[] {original.replaceFirst(
                "      - name: Restore Gradle dependencies\n        id: gradle-deps\n"
                        + "(?:.*\n){6}", ""),
            "one gradle-deps key"});
        broken.put("l1-build-cache-unrestored", new String[] {original.replace(
                "      - name: Restore this commit's build cache\n"
                        + "        uses: actions/cache/restore@v4\n"
                        + "        with:\n"
                        + "          path: ~/.gradle/caches/build-cache-1\n"
                        + "          " + buildKey + "\n", ""),
            "L1 must restore the build cache"});
        broken.put("l1-deps-key-drifted", new String[] {replaceLast(original,
                "          " + depsKey + "\n",
                "          " + depsKey.replace("'gradle/libs.versions.toml'", "'other.toml'")
                        + "\n"),
            "L1 must restore the dependencies under gradle-deps' key"});
        broken.put("build-cache-saved-elsewhere", new String[] {original.replace(
                "          path: ~/.gradle/caches/build-cache-1\n          " + buildKey + "\n"
                        + "\n  l1:",
                "          path: ~/.gradle/caches/build-cache-1\n"
                        + "          key: gradle-build-${{ runner.os }}-other\n\n  l1:"),
            "gradle-deps must save the build cache"});
        for (Map.Entry<String, String[]> each : broken.entrySet()) {
            String text = each.getValue()[0];
            assertThat(text).as("the premise: %s mutates", each.getKey()).isNotEqualTo(original);
            Path ci = fixture.resolve(each.getKey() + ".yml");
            Files.writeString(ci, text);
            Run run = checkMeasurementWorkflow(measurement, ci);
            assertThat(run.exitCode()).as("%s: %s", each.getKey(), run.output()).isEqualTo(1);
            assertThat(run.output()).as(each.getKey()).contains(each.getValue()[1])
                    .doesNotContain("Traceback");
        }
    }

    /** L1's unit step on the runner's four cores, two test forks each (M13.57, M13.67). */
    @Test
    void ciUnitStepRunsFourWorkersAndTwoForks() throws Exception {
        Path root = repository();
        Path measurement = root.resolve(".github/workflows/measurement.yml");
        String original = Files.readString(root.resolve(".github/workflows/ci.yml"));
        Path fixture = root.resolve("buildSrc/build/tmp/ci-forks")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        String step = "        run: ./gradlew test --no-daemon --max-workers=4 -Ptest.forks=2\n";
        for (String weaker : List.of("        run: ./gradlew test --no-daemon --max-workers=4\n",
                "        run: ./gradlew test --no-daemon -Ptest.forks=2\n")) {
            String text = original.replace(step, weaker);
            assertThat(text).as("the premise: %s", weaker).isNotEqualTo(original);
            Path ci = fixture.resolve(UUID.randomUUID() + ".yml");
            Files.writeString(ci, text);
            Run run = checkMeasurementWorkflow(measurement, ci);
            assertThat(run.exitCode()).as(run.output()).isEqualTo(1);
            assertThat(run.output()).contains("unit step").doesNotContain("Traceback");
        }
    }

    private static String replaceLast(String text, String target, String replacement) {
        int at = text.lastIndexOf(target);
        return at < 0 ? text : text.substring(0, at) + replacement
                + text.substring(at + target.length());
    }

    @Test
    void fullMeasurementUsesTheExtendedIntegrationTestTimeout() throws Exception {
        Path root = repository();
        Path measurement = root.resolve(".github/workflows/measurement.yml");
        Path ci = root.resolve(".github/workflows/ci.yml");
        String validWorkflow = Files.readString(measurement).replace(
                "          ./gradlew :server:integrationTest\n",
                "          ./gradlew :server:integrationTest -Pm9.fullMeasurement=true\n");
        Path fixture = root.resolve("buildSrc/build/tmp/measurement-timeout")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        Path withOverride = fixture.resolve("measurement.yml");
        Files.writeString(withOverride, validWorkflow);
        Run valid = checkMeasurementWorkflow(withOverride, ci);
        assertThat(valid.exitCode()).isZero();

        Path withoutOverride = fixture.resolve("measurement-without-timeout.yml");
        Files.writeString(withoutOverride, validWorkflow.replace(
                " -Pm9.fullMeasurement=true", ""));
        Run missingOverride = checkMeasurementWorkflow(withoutOverride, ci);
        assertThat(missingOverride.exitCode()).isEqualTo(1);
        assertThat(missingOverride.output()).contains("extended integration-test timeout");

        Path commentedOverride = fixture.resolve("measurement-commented-timeout.yml");
        Files.writeString(commentedOverride, validWorkflow.replace(
                "          ./gradlew :server:integrationTest -Pm9.fullMeasurement=true\n",
                "          ./gradlew :server:integrationTest\n"
                        + "          # ./gradlew :server:integrationTest -Pm9.fullMeasurement=true\n"));
        Run nonExecutableOverride = checkMeasurementWorkflow(commentedOverride, ci);
        assertThat(nonExecutableOverride.exitCode()).isEqualTo(1);
        assertThat(nonExecutableOverride.output()).contains("extended integration-test timeout");

    }

    @Test
    void fullMeasurementTimeoutAppliesOnlyToServerIntegrationTest() throws Exception {
        Path root = repository();
        Path fixture = root.resolve("buildSrc/build/tmp/measurement-timeout-scope")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        Path initScript = fixture.resolve("timeout-scope.init.gradle");
        Files.writeString(initScript, """
                gradle.projectsEvaluated {
                    def serverProject = gradle.rootProject.findProject(':server')
                    def pluginProject = gradle.rootProject.findProject(':plugin')
                    if (serverProject != null && pluginProject != null) {
                        [serverProject, pluginProject].each { targetProject ->
                            def task = targetProject.tasks.getByName('integrationTest')
                            println("M9_TIMEOUT_SCOPE:${targetProject.path}:${task.timeout.get().toMinutes()}")
                        }
                    }
                }
                """);
        String[] command = System.getProperty("os.name").toLowerCase().contains("win")
                ? new String[]{"cmd", "/c", "gradlew.bat", ":server:help", ":plugin:help",
                    "-Pm9.fullMeasurement=true", "--init-script", initScript.toString(),
                    "--no-configuration-cache", "--no-daemon"}
                : new String[]{"./gradlew", ":server:help", ":plugin:help",
                    "-Pm9.fullMeasurement=true", "--init-script", initScript.toString(),
                    "--no-configuration-cache", "--no-daemon"};
        Path outputFile = fixture.resolve("gradle.log");
        Process process = new ProcessBuilder(command).directory(root.toFile())
                .redirectErrorStream(true).redirectOutput(outputFile.toFile()).start();
        boolean finished = process.waitFor(120, java.util.concurrent.TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        String output = Files.exists(outputFile) ? Files.readString(outputFile) : "";
        assertThat(finished).as("Gradle timeout configuration probe must finish").isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("M9_TIMEOUT_SCOPE::server:60", "M9_TIMEOUT_SCOPE::plugin:10");
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

    @Test
    void milestoneEvidenceCheckerRejectsMissingCriterionLine() throws Exception {
        Path fixture = repository().resolve("buildSrc/build/tmp/milestone-verified")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);
        Files.writeString(fixture.resolve("SPEC.md"), "## Acceptance criteria\n"
                + "1. first behaviour\n2. second behaviour\n\n## Design\n");
        Path verified = fixture.resolve("VERIFIED.md");
        Files.writeString(verified, "1. FirstTest#first proves the first behaviour.\n"
                + "2. SecondTest#second proves the second behaviour.\n");

        Run complete = checkMilestoneVerified(fixture);
        assertThat(complete.exitCode()).isZero();

        Files.writeString(verified, "1. FirstTest#first proves the first behaviour.\n");
        Run missing = checkMilestoneVerified(fixture);
        assertThat(missing.exitCode()).isEqualTo(1);
        assertThat(missing.output()).contains("criterion 2 has no evidence line");
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

    private static Run checkMilestoneVerified(Path milestone) throws Exception {
        Process process = ProcessSupport.builder("python", "scripts/run-gate.py",
                "scripts/check-milestone-verified.sh", milestone.toString())
                .directory(repository().toFile()).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        return new Run(process.waitFor(), output);
    }

    private record Run(int exitCode, String output) {}
}
