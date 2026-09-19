// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.backend.MinioFixture;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The harness does what its verbs say, observed at the store (M8.8).
 *
 * <p>⚠️ **EVERY CHAOS ROW RESTS ON THESE THREE.** A {@code kill()} that ran
 * the shutdown hook would make an RPO row pass for a reason that has nothing
 * to do with durability. A {@code pause()} that froze nothing would make a
 * gray-failure row pass because nothing was grey. Each verb is therefore
 * asserted by its effect on the lease, read out of the bucket by a client
 * that shares nothing with the process.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ChaosHarnessIT {

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    /** A node that holds the sequencer term, which it takes on its first commit. */
    private NodeProcess leader(ChaosBucket bucket) throws Exception {
        NodeProcess node = NodeProcess.start(dir, "pod1", bucket.nodeSettings());
        node.registerLogs(UUID.randomUUID());
        assertThat(node.write("first", 1)).as("the premise: a write that takes the term")
                .isEqualTo(202);
        assertThat(bucket.leaseExpiry()).as("the premise: the term is held").isPresent();
        assertThat(bucket.leaseExpiry().getAsLong()).isGreaterThan(System.currentTimeMillis());
        return node;
    }

    @Test
    void aKILLRunsNOHookAndLEAVESTheTermHeld() throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create(); NodeProcess node = leader(bucket)) {
            int code = node.kill();
            long after = System.currentTimeMillis();

            assertThat(code).as("⚠️ 137 IS SIGKILL: the kernel ended it, not the JVM")
                    .isEqualTo(137);
            assertThat(bucket.leaseExpiry().getAsLong())
                    .as("⚠️ THE TERM IS STILL HELD. A kill that ran the hook would have "
                            + "released it, and an RPO row would pass on a clean shutdown")
                    .isGreaterThan(after);
            assertThat(node.log()).as("and no drain ran").doesNotContain("shutdown event");
        }
    }

    @Test
    void aTERMINATERunsTheDRAINAndRELEASESTheTerm() throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create(); NodeProcess node = leader(bucket)) {
            node.terminate();

            assertThat(bucket.leaseExpiry().getAsLong())
                    .as("⚠️ RELEASED: the expiry is in the past")
                    .isLessThanOrEqualTo(System.currentTimeMillis());
            assertThat(node.log()).contains("graceful shutdown took");
        }
    }

    @Test
    void aPAUSEFreezesRENEWALAndRESUMEStartsItAgain() throws Exception {
        // ⚠️ ABOUT TIME, SO A BOUND WITH HEADROOM: the default renewal is
        // every 3 s and the pause is 7 s, more than two renewals' worth.
        try (ChaosBucket bucket = ChaosBucket.create(); NodeProcess node = leader(bucket)) {
            long first = bucket.leaseExpiry().getAsLong();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (bucket.leaseExpiry().getAsLong() == first) {
                assertThat(System.nanoTime()).as("the premise: the lease is being renewed")
                        .isLessThan(deadline);
                Thread.sleep(200);
            }

            node.pause();
            long frozen = bucket.leaseExpiry().getAsLong();
            Thread.sleep(7_000);
            long stillFrozen = bucket.leaseExpiry().getAsLong();
            assertThat(node.alive()).as("paused is not dead").isTrue();
            node.resume();

            assertThat(stillFrozen).as("⚠️ A STOPPED PROCESS RENEWS NOTHING").isEqualTo(frozen);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (bucket.leaseExpiry().getAsLong() == frozen) {
                assertThat(System.nanoTime()).as("⚠️ AND A RESUMED ONE RENEWS AGAIN")
                        .isLessThan(deadline);
                Thread.sleep(200);
            }
        }
    }
}
