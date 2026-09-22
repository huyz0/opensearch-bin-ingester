// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

/** A kill of a paused process must not resume it on the way to SIGKILL (M8.71). */
class NodeProcessKillResumesPausedTest {

    @Test
    void aKILLLeavesAPausedNodePausedUntilTheKernelReapsIt() throws Exception {
        FakeProcess process = new FakeProcess();
        try (NodeProcess node = NodeProcess.forTest(process, true)) {
            assertThat(node.paused()).isTrue();
            assertThat(node.kill()).isEqualTo(137);
            assertThat(process.destroyed).isTrue();
            assertThat(node.resumeAttempts())
                    .as("kill must not attempt SIGCONT before SIGKILL")
                    .isZero();
        }
    }

    private static final class FakeProcess extends Process {
        private boolean destroyed;

        @Override public OutputStream getOutputStream() { return new ByteArrayOutputStream(); }
        @Override public InputStream getInputStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public InputStream getErrorStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public int waitFor() { return 137; }
        @Override public boolean waitFor(long timeout, java.util.concurrent.TimeUnit unit) { return true; }
        @Override public int exitValue() { return 137; }
        @Override public void destroy() { destroyed = true; }
        @Override public Process destroyForcibly() { destroyed = true; return this; }
        @Override public boolean isAlive() { return !destroyed; }
    }
}
