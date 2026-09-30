// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * M13.2 (M12.28), through the path every harness node takes: a real node whose
 * first probed port another process holds by the time it binds is started
 * again, and serves, on the next one.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NodeProcessLostPortIT {

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: the node needs its store");
    }

    @Test
    void aNodeWhoseProbedPortWasTakenServesOnTheNextOne() throws Exception {
        List<Integer> handedOut = new ArrayList<>();
        try (ServerSocket thief = new ServerSocket(0); ChaosBucket bucket = ChaosBucket.create()) {
            int taken = thief.getLocalPort();
            try (NodeProcess node = NodeProcess.start(dir, "pod0", bucket.nodeSettings(), () -> {
                int port = handedOut.isEmpty() ? taken : NodePorts.probeFreePort();
                handedOut.add(port);
                return port;
            })) {
                assertThat(handedOut).as("the lost port, then a fresh one").hasSize(2)
                        .startsWith(taken);
                assertThat(node.port()).isEqualTo(handedOut.get(1)).isNotEqualTo(taken);
                assertThat(node.alive()).isTrue();
            }
        }
    }
}
