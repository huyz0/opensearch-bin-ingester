// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * M13.2 (M12.28): a node that loses its probed port to another process between
 * the probe and the bind is started again on a fresh port, and only then.
 *
 * <p>⚠️ NOT PORT 0 IN THE CHILD: the lease advertises the endpoint, port
 * included, when the graph is assembled -- before the front door binds -- so a
 * child on port 0 would advertise a port nobody knows. The race is recovered
 * rather than removed, which is what M12.28 allowed ("or retry the bind").
 */
class NodeProcessPortTest {

    /** What `Main` prints when the front door refused its port (FrontDoor, M8.4). */
    private static final String REFUSED =
            "could not start: the front door did not bind port 50174 -- it is already in use";

    @Test
    void aNodeThatLostItsPortIsStartedAgainOnAFreshOne() throws Exception {
        AtomicInteger next = new AtomicInteger(50_000);
        List<Integer> tried = new ArrayList<>();

        String started = NodePorts.onAFreePort(3, next::incrementAndGet, port -> {
            tried.add(port);
            if (tried.size() == 1) {
                throw new NodePorts.PortLost(port, "taken");
            }
            return "serving on " + port;
        });

        assertThat(tried).containsExactly(50_001, 50_002);
        assertThat(started).isEqualTo("serving on 50002");
    }

    @Test
    void anyOtherStartFailureIsNotRetried() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> NodePorts.onAFreePort(3, () -> 50_000, port -> {
            attempts.incrementAndGet();
            throw new AssertionError("pod0 died before it started serving");
        })).isInstanceOf(AssertionError.class);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void aRuntimeFailureOtherThanALostPortIsNotRetriedEither() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> NodePorts.onAFreePort(3, () -> 50_000, port -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("could not start: direct needs a signing backend");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(attempts).hasValue(1);
    }

    /**
     * ⚠️ THE PATH A REAL NODE TAKES (M13.2 review T1): a dead child whose log
     * holds the front door's refusal of ITS port must leave the wait as a
     * {@link NodePorts.PortLost} -- at once, not after Awaitility's two
     * minutes -- or {@link NodePorts#onAFreePort} never sees it.
     */
    @Test
    void aDeadChildThatPrintedTheRefusalOfItsPortLeavesTheWaitAsAPortLossAtOnce()
            throws Exception {
        Path dir = Files.createDirectories(Path.of("build", "tmp", "node-process-port"));
        Path log = Files.writeString(dir.resolve(UUID.randomUUID() + ".log"), REFUSED + "\n");
        try {
            DeadProcess child = new DeadProcess();
            NodeProcess node = NodeProcess.forTest(child, 50174, log);
            long started = System.nanoTime();

            assertThatThrownBy(node::awaitServingOrClose)
                    .isInstanceOf(NodePorts.PortLost.class).hasMessageContaining("50174");
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .as("the wait ends when the child is seen dead, not at 120 s")
                    .isLessThan(Duration.ofSeconds(30));
            assertThat(child.destroyed).as("the node is closed before any retry").isTrue();
        } finally {
            Files.deleteIfExists(log);
        }
    }

    /**
     * ⚠️ THE NEGATIVE SIDE, WHERE A REAL NODE'S DEATH IS CLASSIFIED (M13.2
     * review R4, M13.2a): a child that died for its own reason is not a lost
     * port, however its log reads, or it would be restarted until it happened
     * to come up -- and it fails the wait at once, as a dead child cannot
     * recover.
     */
    @Test
    void aDeadChildThatDiedForAnotherReasonIsNotAPortLoss() throws Exception {
        assertDiedWithoutPortLoss("could not start: direct needs a signing backend");
    }

    @Test
    void aDeadChildThatRefusedAnotherPortIsNotAPortLoss() throws Exception {
        assertDiedWithoutPortLoss(REFUSED.replace("50174", "50175"));
    }

    private static void assertDiedWithoutPortLoss(String logText) throws Exception {
        Path dir = Files.createDirectories(Path.of("build", "tmp", "node-process-port"));
        Path log = Files.writeString(dir.resolve(UUID.randomUUID() + ".log"), logText);
        try {
            DeadProcess child = new DeadProcess();
            NodeProcess node = NodeProcess.forTest(child, 50174, log);
            long started = System.nanoTime();

            assertThatThrownBy(node::awaitServingOrClose)
                    .isNotInstanceOf(NodePorts.PortLost.class)
                    .hasMessageContaining("died before it started serving");
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .as("a dead child ends the wait at once, not at 120 s")
                    .isLessThan(Duration.ofSeconds(30));
            assertThat(child.destroyed).isTrue();
        } finally {
            Files.deleteIfExists(log);
        }
    }

    @Test
    void aPortLossIsFoundHoweverDeeplyItIsWrapped() {
        NodePorts.PortLost lost = new NodePorts.PortLost(50174, "taken");

        assertThat(NodePorts.portLostIn(new RuntimeException(new IllegalStateException(lost))))
                .isSameAs(lost);
        assertThat(NodePorts.portLostIn(new AssertionError("died"))).isNull();
    }

    /** A child that has already exited. */
    private static final class DeadProcess extends Process {
        private boolean destroyed;

        @Override public OutputStream getOutputStream() { return new ByteArrayOutputStream(); }
        @Override public InputStream getInputStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public InputStream getErrorStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public int waitFor() { return 4; }
        @Override public boolean waitFor(long timeout, java.util.concurrent.TimeUnit unit) { return true; }
        @Override public int exitValue() { return 4; }
        @Override public void destroy() { destroyed = true; }
        @Override public Process destroyForcibly() { destroyed = true; return this; }
        @Override public boolean isAlive() { return false; }
    }

    @Test
    void theRetriesAreBoundedAndTheLastLossIsReported() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> NodePorts.onAFreePort(3, () -> 50_000, port -> {
            attempts.incrementAndGet();
            throw new NodePorts.PortLost(port, "taken");
        })).isInstanceOf(NodePorts.PortLost.class).hasMessageContaining("50000");
        assertThat(attempts).hasValue(3);
    }

    @Test
    void aLostPortIsRecognisedOnlyFromTheFrontDoorsRefusalOfThatPort() {
        assertThat(NodePorts.lostItsPort(REFUSED, 50174)).isTrue();
        assertThat(NodePorts.lostItsPort(REFUSED, 50175)).as("another port").isFalse();
        assertThat(NodePorts.lostItsPort(REFUSED, 5017)).as("a prefix of the port").isFalse();
        assertThat(NodePorts.lostItsPort("could not start: direct needs a signing backend",
                50174)).as("another refusal").isFalse();
    }
}
