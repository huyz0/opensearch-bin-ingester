// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class L1TestCountTest {

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

    @Test
    void l1TestCountRejectsNoProductResultsAndAcceptsExecutedProductTests() throws Exception {
        Path fixture = ROOT.resolve("buildSrc/build/tmp/l1-test-count")
                .resolve(UUID.randomUUID().toString());
        Files.createDirectories(fixture);

        Run empty = check(fixture);
        assertThat(empty.exitCode()).isEqualTo(1);
        assertThat(empty.output()).contains("L1 reported zero product unit tests");

        Path junit = fixture.resolve("ingest/build/test-results/test/TEST-ingest.UnitTest.xml");
        Files.createDirectories(junit.getParent());
        Files.writeString(junit, "<testsuite tests=\"2\" skipped=\"2\"><testcase><skipped/></testcase><testcase><skipped/></testcase></testsuite>");
        Run skipped = check(fixture);
        assertThat(skipped.exitCode()).isEqualTo(1);
        assertThat(skipped.output()).contains("L1 reported zero product unit tests");

        Files.writeString(junit, "<testsuite tests=\"2\" failures=\"0\"><testcase/><testcase/></testsuite>");
        Run populated = check(fixture);
        assertThat(populated.exitCode()).isZero();
        assertThat(populated.output()).contains("L1 executed 2 product unit test(s)");
    }

    private static Run check(Path fixture) throws Exception {
        Process process = ProcessSupport.builder("python", "scripts/check-l1-tests.py", "--root",
                fixture.toString()).directory(ROOT.toFile()).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        return new Run(process.waitFor(), output);
    }

    private record Run(int exitCode, String output) {}
}
