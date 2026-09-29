// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * M11.25 (H17): a test that runs past its {@code @Timeout} prints every
 * thread's stack. {@code DefaultIngestTest} timed out once at its 30 s class
 * limit and the one occurrence left no stack trace, so its cause could only be
 * guessed. JUnit prints the dump when this configuration parameter is set; the
 * build passes it to every test task, and this pins that it arrives.
 */
class TestTimeoutThreadDumpTest {

    @Test
    void aTimedOutTestPrintsAThreadDump() {
        assertThat(System.getProperty("junit.jupiter.execution.timeout.threaddump.enabled"))
                .as("⚠️ WITHOUT IT a timeout says which test, never where it was stuck")
                .isEqualTo("true");
    }
}
