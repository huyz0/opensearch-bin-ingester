// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RepositoryGateChecksTest {

    @Test
    void stagedCsvAndSvgBlobsMatchWindowsCheckouts() {
        byte[] staged = "value,other\n1,2\n".getBytes(StandardCharsets.UTF_8);
        byte[] windows = "value,other\r\n1,2\r\n".getBytes(StandardCharsets.UTF_8);
        assertThat(RepositoryGatesTask.Companion.contentMatches("results.csv", staged, windows))
                .isTrue();
        assertThat(RepositoryGatesTask.Companion.contentMatches("curve.svg", staged, windows))
                .isTrue();
    }

    @Test
    void theGlossaryIsExemptFromTheTerminologyGateOnEitherPathSeparator() {
        assertThat(RepositoryGatesTask.Companion.terminologyExempt("docs/internal/standards/glossary.md")).isTrue();
        assertThat(RepositoryGatesTask.Companion.terminologyExempt("docs\\internal\\standards\\glossary.md")).isTrue();
        assertThat(RepositoryGatesTask.Companion.terminologyExempt("docs/internal/standards/testing.md")).isFalse();
        assertThat(RepositoryGatesTask.Companion.terminologyExempt("docs/glossary.md")).isFalse();
    }

    @Test
    void nativeHarnessKeepsTheMeasurementWorkflowWiringTestIncluded() throws Exception {
        String build = Files.readString(Path.of("build.gradle.kts"));
        assertThat(build).contains("includeTestsMatching(\"io.github.huyz0.os.biningester.GradleGateWiringTest\")");
    }

    @Test
    void moduleDriftRefusesASettingsEntryWithoutABuildFile() throws Exception {
        Path root = Files.createTempDirectory("gate-module-");
        Files.writeString(root.resolve("settings.gradle.kts"), "include(\"ghost\")\n");
        ArrayList<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.moduleDrift(root, failures);

        assertThat(failures).anyMatch(message -> message.contains("ghost"));
    }

    @Test
    void metricGateRefusesAnIndexLabel() throws Exception {
        Path root = Files.createTempDirectory("gate-metric-");
        Path source = root.resolve("src/main/java/Example.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "class Example { void record(String index) { tag(\"index\", index); } }\n");
        ArrayList<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.metricCardinality(root, java.util.List.of(source), failures);

        assertThat(failures).anyMatch(message -> message.contains("high-cardinality"));
    }

    @Test
    void ioGateRefusesUnseamedJavaIoAndZipReferences() throws Exception {
        Path root = Files.createTempDirectory("gate-io-");
        Path source = root.resolve("src/main/java/Example.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "class Example { java.io.File file; java.util.zip.ZipFile zip; }\n");
        ArrayList<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.ioSeam(root, java.util.List.of(source), failures);

        assertThat(failures).anyMatch(message -> message.contains("java.io"));
        assertThat(failures).anyMatch(message -> message.contains("java.util.zip"));
    }

    @Test
    void ioGateAllowsStreamingHttpOnlyInTheNamedCatchUpExchangeAdapter() throws Exception {
        Path root = Files.createTempDirectory("gate-streaming-http-");
        String adapterPath = "client/src/main/java/io/github/huyz0/os/biningester/client/HttpCatchUpExchange.java";
        String reference = "class HttpCatchUpExchange { java.net.http.HttpClient client; }\n";
        Path adapter = root.resolve(adapterPath);
        Files.createDirectories(adapter.getParent());
        Files.writeString(adapter, reference);
        ArrayList<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.ioSeam(root, java.util.List.of(adapter), failures);

        assertThat(failures).as("streaming network I/O is confined to the transport seam")
                .isEmpty();

        Path businessLogic = root.resolve(
                "client/src/main/java/io/github/huyz0/os/biningester/client/ConsumerClient.java");
        Files.writeString(businessLogic, reference);
        failures.clear();
        RepositoryGateChecks.INSTANCE.ioSeam(root, java.util.List.of(businessLogic), failures);

        assertThat(failures).singleElement().satisfies(failure -> assertThat(failure)
                .contains("I/O package java.net", "ConsumerClient.java"));
    }

    @Test
    void tddEvidenceRequiresARealByteBoundRedRecord() {
        String sha = "abc";
        assertThat(TddEvidenceTask.Companion.redRecordMatches(
                "{\"red\":{\"Example#test\":{\"sha256\":\"abc\",\"at\":1,\"source\":\"Example.java\"}}}",
                "Example#test", sha)).isTrue();
        assertThat(TddEvidenceTask.Companion.redRecordMatches(
                "{\"red\":{\"Example#test\":{\"sha256\":\"abc\"}}}",
                "Example#test", sha)).isFalse();
        assertThat(TddEvidenceTask.Companion.redRecordMatches(
                "{\"other\":{\"Example#test\":{\"sha256\":\"abc\",\"at\":1,\"source\":\"Example.java\"}}}",
                "Example#test", sha)).isFalse();
        String source = "package p; class Outer { String text = \"class Fake {\"; @Test void outerTest() {} class Inner { @Test void innerTest() {} } }";
        assertThat(TddEvidenceTask.Companion.testIdsFor(source, "Outer.java"))
                .contains("p.Outer#outerTest", "p.Outer$Inner#innerTest");
        String textBlock = "package p; class Outer { String text = \"\"\"class Fake { @Test void fake() {} }\"\"\"; @Test void realTest() {} }";
        assertThat(TddEvidenceTask.Companion.testIdsFor(textBlock, "Outer.java"))
                .containsExactly("p.Outer#realTest");
        String escapedBlock = "package p; class Outer { String text = " + "\"\"\"" + "escaped \\\"\"\" still text" + "\"\"\"" + "; @Test void realAfterEscapedTest() {} }";
        assertThat(TddEvidenceTask.Companion.testIdsFor(escapedBlock, "Outer.java"))
                .containsExactly("p.Outer#realAfterEscapedTest");
    }

    @Test
    void reviewEvidenceRequiresTheCurrentDiffAndStructuredFindings() throws Exception {
        assertThat(ReviewEvidenceTask.Companion.isValidVerdict(
                Map.of("task", "M0.14", "diff_sha256", "abc", "verdict", "pass", "findings", java.util.List.of()), "abc")).isTrue();
        assertThat(ReviewEvidenceTask.Companion.isValidVerdict(
                Map.of("task", "M0.14", "diff_sha256", "old", "verdict", "pass", "findings", java.util.List.of()), "abc")).isFalse();
        assertThat(ReviewEvidenceTask.Companion.isValidVerdict(
                Map.of("task", "M0.14", "diff_sha256", "abc", "verdict", "pass", "findings", java.util.List.of(Map.of("id", "F1"))), "abc")).isFalse();
        assertThat(ReviewEvidenceTask.Companion.parseBudget(null)).isEqualTo(3);
        org.junit.jupiter.api.Assertions.assertThrows(org.gradle.api.GradleException.class,
                () -> ReviewEvidenceTask.Companion.parseBudget("not-a-number"));
        assertThat(RepositoryGatesTask.Companion.sameContentForTest("a\nb\n".getBytes(), "a\r\nb\r\n".getBytes())).isTrue();
        assertThat(RepositoryGatesTask.Companion.sameContentForTest("a\nb\n".getBytes(), "changed\r\n".getBytes())).isFalse();
        assertThat(RepositoryGatesTask.Companion.contentMatches("gradle-wrapper.jar", new byte[]{0, 1, 2}, new byte[]{0, 1, 3})).isFalse();
        Path repo = Files.createTempDirectory("gate-staged-");
        Process init = new ProcessBuilder("git", "init").directory(repo.toFile()).start();
        assertThat(init.waitFor()).isZero();
        Files.writeString(repo.resolve("probe.txt"), "staged\n");
        Process add = new ProcessBuilder("git", "add", "probe.txt").directory(repo.toFile()).start();
        assertThat(add.waitFor()).isZero();
        assertThat(RepositoryGatesTask.Companion.stagedBlobMatches(repo, "probe.txt", "staged\r\n".getBytes(StandardCharsets.UTF_8))).isTrue();
        assertThat(RepositoryGatesTask.Companion.stagedBlobMatches(repo, "probe.txt", "changed\n".getBytes(StandardCharsets.UTF_8))).isFalse();
    }
}
