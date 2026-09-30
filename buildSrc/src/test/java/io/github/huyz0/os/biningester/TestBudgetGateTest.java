// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * M13.9 (M12 harvest R7): the test budget is a file predicate over the limits
 * the runtime enforces -- the Gradle daemon's heap, a compile daemon and a test
 * JVM per worker, and every compose service's {@code mem_limit} -- summed
 * against the ceiling build.md states. Before it, {@code testBudget} checked
 * only that the Gradle limits were present, and nothing read the compose file
 * or the ceiling: the retired {@code scripts/check-test-budget.sh} did.
 */
class TestBudgetGateTest {

    private static final String CONVENTIONS =
            "buildSrc/src/main/kotlin/io.github.huyz0.os.biningester.java-conventions.gradle.kts";
    private static final String ONE_SERVICE = """
            services:
              store:
                image: x
                mem_limit: 1g
            """;

    @TempDir
    Path root;

    @Test
    void theRepositoryItselfSumsToItsMeasuredLimitsWithinItsCeiling() {
        List<String> failures = new ArrayList<>();
        Map<String, Integer> parts = TestBudget.INSTANCE.parts(repository(), failures);

        assertThat(failures).isEmpty();
        assertThat(parts).as("the limits the runtime enforces, as build.md tabulates them")
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "Gradle daemon heap", 1024,
                        "compile daemons (2 x 512 MiB)", 1024,
                        "test JVMs (2 x 512 MiB)", 1024,
                        "docker-compose.test.yml/rustfs", 1024));
        List<String> checked = new ArrayList<>();
        TestBudget.INSTANCE.check(repository(), checked);
        assertThat(checked).as("4,096 MiB inside build.md's 6 GiB").isEmpty();
        assertThat(TestBudget.INSTANCE.ceilingMib(read(repository().resolve(
                "docs/internal/standards/build.md")))).isEqualTo(6144);
    }

    @Test
    void aServiceWithoutALimitIsRefusedEvenIfALaterServiceHasOne() throws Exception {
        List<String> failures = check("""
                services:
                  bare:
                    image: x
                  limited:
                    image: y
                    mem_limit: 256m
                """, 6, 2);

        assertThat(failures).as("⚠️ WITHIN ITS OWN BLOCK: the next service's limit is not its")
                .containsExactly("docker-compose.test.yml: service 'bare' declares no memory limit");
    }

    @Test
    void onlyTheServicesAreAskedForALimit() {
        assertThat(TestBudget.INSTANCE.composeLimits("""
                version: "3"
                services:
                  store:
                    image: x
                    mem_limit: "512m"
                    healthcheck:
                      retries: 30
                  other:
                    deploy:
                      resources:
                        limits:
                          memory: 2g
                volumes:
                  data:
                    driver: local
                """))
                .as("a nested key is not a service, nor is a top-level volume")
                .containsExactlyInAnyOrderEntriesOf(Map.of("store", "512m", "other", "2g"));
    }

    @Test
    void anUnreadableLimitIsRefused() throws Exception {
        assertThat(check(ONE_SERVICE.replace("1g", "lots"), 6, 2))
                .containsExactly("docker-compose.test.yml: service 'store' has an unreadable"
                        + " memory limit 'lots'");
    }

    @Test
    void theSumMayReachTheCeilingButNotPassIt() throws Exception {
        assertThat(check(ONE_SERVICE.replace("1g", "3g"), 6, 2))
                .as("3,072 + 3 x 1,024 = 6,144 MiB, the ceiling itself").isEmpty();
        assertThat(check(ONE_SERVICE.replace("1g", "3073m"), 6, 2))
                .containsExactly("the configured memory limits sum to 6145 MiB, above"
                        + " build.md's 6144 MiB ceiling");
    }

    @Test
    void theCeilingIsTheOneBuildMdStates() throws Exception {
        assertThat(check(ONE_SERVICE, 4, 2)).as("4,096 MiB at a 4 GiB ceiling").isEmpty();
        assertThat(check(ONE_SERVICE, 3, 2)).containsExactly(
                "the configured memory limits sum to 4096 MiB, above build.md's 3072 MiB ceiling");
        assertThat(TestBudget.INSTANCE.ceilingMib("The ceiling is whatever feels right."))
                .as("a build.md that states none").isNull();
    }

    @Test
    void aBuildMdWithoutACeilingIsRefused() throws Exception {
        write(ONE_SERVICE, 6, 2);
        Files.writeString(root.resolve("docs/internal/standards/build.md"), "# Build\n");
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);

        assertThat(failures).containsExactly(
                "docs/internal/standards/build.md states no **Ceiling: <n> GiB**");
    }

    @Test
    void everyWorkerHoldsItsOwnCompileDaemonAndTestJvm() throws Exception {
        write(ONE_SERVICE, 6, 3);
        List<String> failures = new ArrayList<>();

        assertThat(TestBudget.INSTANCE.parts(root, failures)).containsExactlyInAnyOrderEntriesOf(Map.of(
                "Gradle daemon heap", 1024,
                "compile daemons (3 x 512 MiB)", 1536,
                "test JVMs (3 x 512 MiB)", 1536,
                "docker-compose.test.yml/store", 1024));
        assertThat(failures).isEmpty();
    }

    @Test
    void aMissingGradleLimitIsRefused() throws Exception {
        write(ONE_SERVICE, 6, 2);
        Files.writeString(root.resolve("gradle.properties"), "org.gradle.workers.max=2\n");
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);

        assertThat(failures).containsExactly("gradle.properties has no Gradle heap limit");
    }

    @Test
    void memoryUnitsAreReadAsComposeAndTheJvmWriteThem() {
        assertThat(TestBudget.INSTANCE.mib("1g")).isEqualTo(1024);
        assertThat(TestBudget.INSTANCE.mib("1G")).isEqualTo(1024);
        assertThat(TestBudget.INSTANCE.mib("512m")).isEqualTo(512);
        assertThat(TestBudget.INSTANCE.mib("512mb")).isEqualTo(512);
        assertThat(TestBudget.INSTANCE.mib("1.5g")).as("⚠️ NOT READ AS 1g").isEqualTo(1536);
        assertThat(TestBudget.INSTANCE.mib("2048k")).isEqualTo(2);
        assertThat(TestBudget.INSTANCE.mib("lots")).isNull();
        assertThat(TestBudget.INSTANCE.mib("")).isNull();
    }

    @Test
    void aReservationIsNotACap() {
        Map<String, String> expected = new java.util.HashMap<>();
        expected.put("soft", null);
        expected.put("capped", "1g");
        assertThat(TestBudget.INSTANCE.composeLimits("""
                services:
                  soft:
                    deploy:
                      resources:
                        reservations:
                          memory: 512m
                  capped:
                    deploy:
                      resources:
                        reservations:
                          memory: 256m
                        limits:
                          memory: 1g
                """))
                .as("⚠️ A RESERVATION IS A REQUEST (review P3): only limits: caps the container")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void aZeroLimitIsRefused() throws Exception {
        assertThat(check(ONE_SERVICE.replace("1g", "0"), 6, 2))
                .as("⚠️ 0 IS UNLIMITED to Docker (review P5)")
                .containsExactly("docker-compose.test.yml: service 'store' has a memory limit of 0,"
                        + " which Docker reads as none");
    }

    @Test
    void everyRootComposeFileIsRead() throws Exception {
        write(ONE_SERVICE, 6, 2);
        Files.writeString(root.resolve("docker-compose.extra.yaml"), "services:\n  bare:\n    image: x\n");
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);

        assertThat(failures).containsExactly(
                "docker-compose.extra.yaml: service 'bare' declares no memory limit");
    }

    @Test
    void theLargestConventionsTestHeapIsTheOneSummed() throws Exception {
        write(ONE_SERVICE, 6, 2);
        Files.writeString(root.resolve(CONVENTIONS), """
                options.forkOptions.memoryMaximumSize = "512m"
                tasks.withType<Test>().configureEach { maxHeapSize = "256m" }
                tasks.named<Test>("test") { maxHeapSize = "768m"; timeout.set(Duration.ofMinutes(10)) }
                """);
        List<String> failures = new ArrayList<>();

        assertThat(TestBudget.INSTANCE.parts(root, failures))
                .containsEntry("test JVMs (2 x 768 MiB)", 1536);
        assertThat(failures).isEmpty();
    }

    @Test
    void aMissingWorkerLimitOrTestTimeoutIsRefused() throws Exception {
        write(ONE_SERVICE, 6, 2);
        Files.writeString(root.resolve("gradle.properties"), "org.gradle.jvmargs=-Xmx1g\n");
        Files.writeString(root.resolve(CONVENTIONS), """
                options.forkOptions.memoryMaximumSize = "512m"
                tasks.withType<Test>().configureEach { maxHeapSize = "512m" }
                """);
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);

        assertThat(failures).containsExactly(
                "gradle.properties has no worker limit", "conventions plugin has no test timeout");
    }

    /**
     * ⚠️ A MODULE's OWN TEST HEAP (review P1): {@code plugin}'s clusterTest set
     * 2g where no gate read it. At most the conventions' heap, or a named
     * exception at its one value.
     */
    @Test
    void aModuleTestHeapAboveTheConventionsIsRefusedUnlessNamed() throws Exception {
        write(ONE_SERVICE, 6, 2);
        module("server", "tasks.named<Test>(\"test\") { maxHeapSize = \"1g\" }");
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);

        assertThat(failures).as("http's 256m and plugin's named 2g pass; server's 1g does not")
                .containsExactly("server/build.gradle.kts sets a test heap of 1024 MiB, above the"
                        + " conventions plugin's 512 MiB, and is not a named exception at that value");
    }

    @Test
    void theNamedExceptionIsPinnedAtItsValueAndMustStillBeSet() throws Exception {
        write(ONE_SERVICE, 6, 2);
        module("plugin", "tasks.named<Test>(\"clusterTest\") { maxHeapSize = \"3g\" }");
        List<String> raised = new ArrayList<>();
        TestBudget.INSTANCE.check(root, raised);
        assertThat(raised).containsExactly(
                "plugin/build.gradle.kts sets a test heap of 3072 MiB, above the conventions"
                        + " plugin's 512 MiB, and is not a named exception at that value",
                "plugin/build.gradle.kts is a named test-heap exception at 2048 MiB but no longer sets it");

        module("plugin", "tasks.named<Test>(\"clusterTest\") { }");
        List<String> gone = new ArrayList<>();
        TestBudget.INSTANCE.check(root, gone);
        assertThat(gone).as("⚠️ AN EXCEPTION CANNOT OUTLIVE ITS REASON").containsExactly(
                "plugin/build.gradle.kts is a named test-heap exception at 2048 MiB but no longer sets it");
    }

    private void module(String name, String script) throws Exception {
        Files.createDirectories(root.resolve(name));
        Files.writeString(root.resolve(name).resolve("build.gradle.kts"), script + "\n");
    }

    private List<String> check(String compose, int ceilingGib, int workers) throws Exception {
        write(compose, ceilingGib, workers);
        List<String> failures = new ArrayList<>();
        TestBudget.INSTANCE.check(root, failures);
        return failures;
    }

    /** A repository with the real tree's Gradle limits and module heaps, and this compose file and ceiling. */
    private void write(String compose, int ceilingGib, int workers) throws Exception {
        Files.writeString(root.resolve("gradle.properties"),
                "org.gradle.jvmargs=-Xmx1g -XX:MaxMetaspaceSize=512m\norg.gradle.workers.max="
                        + workers + "\n");
        Path conventions = root.resolve(CONVENTIONS);
        Files.createDirectories(conventions.getParent());
        Files.writeString(conventions, """
                options.forkOptions.memoryMaximumSize = "512m"
                tasks.withType<Test>().configureEach {
                    maxHeapSize = "512m"
                    timeout.set(Duration.ofMinutes(10))
                }
                """);
        Path buildMd = root.resolve("docs/internal/standards/build.md");
        Files.createDirectories(buildMd.getParent());
        Files.writeString(buildMd, "## Budget\n\n**Ceiling: " + ceilingGib
                + " GiB**, which the gate sums the configured limits against.\n");
        Files.writeString(root.resolve("docker-compose.test.yml"), compose);
        module("plugin", "tasks.named<Test>(\"clusterTest\") { maxHeapSize = \"2g\" }");
        module("http", "tasks.named<Test>(\"slowTest\") { maxHeapSize = \"256m\" }");
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** The checkout's root: the harness runs with {@code buildSrc} as its working directory. */
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }
}
