// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.HealthTrackingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MinioFixture;
import io.github.huyz0.os.biningester.http.HealthService;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A pod partitioned from the store only: readiness fails, and the acks stop,
 * counted (M8.15, NFR-8, M8 criterion 12).
 *
 * <p>⚠️ **THE ACKS ARE COUNTED, NOT A LOG LINE READ.** A pod that logged the
 * failure and kept returning 202 is exactly the defect: records acked into a
 * buffer that cannot reach the bucket. So the producers count every 202, and
 * the count must not move while the store is out of reach.
 *
 * <p>⚠️ **THE CUT IS A BLACK HOLE** ({@link ChaosProxy}): a real partition
 * answers nothing, so the node learns of it only from calls that hang, which
 * is what its readiness must notice.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StorePartitionIT {

    /** Time for requests already inside the node to finish before counting. */
    private static final Duration SETTLE = Duration.ofSeconds(3);

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    private static int status(NodeProcess node, String path) {
        try (var response = node.client().get(path).request()) {
            return response.status().code();
        } catch (RuntimeException noAnswer) {
            return -1;
        }
    }

    @Test
    void aStoreONLYPartitionFAILSReadinessAndSTOPSTheAcks() throws Exception {
        URI minio = URI.create(MinioFixture.endpoint());
        AtomicLong acks = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create();
                ChaosProxy store = new ChaosProxy(minio.getHost(), minio.getPort())) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("store.endpoint", "http://localhost:" + store.port());
            settings.put("ingest.interval-floor", "PT0.05S");
            try (NodeProcess node = NodeProcess.start(dir, "pod0", settings)) {
                node.registerLogs(UUID.randomUUID());
                for (int p = 0; p < 4; p++) {
                    int producer = p;
                    producers.add(Thread.ofVirtual().start(() -> {
                        for (int seq = 0; !stop.get(); seq++) {
                            try {
                                if (node.write("p" + producer + "-" + seq, 5) == 202) {
                                    acks.incrementAndGet();
                                }
                            } catch (RuntimeException timedOut) {
                                // a producer's 2 s timeout: no ack
                            }
                        }
                    }));
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (acks.get() < 20) {
                    assertThat(System.nanoTime()).as("the premise: it acks").isLessThan(deadline);
                    Thread.sleep(50);
                }
                assertThat(status(node, HealthService.READY_PATH)).isEqualTo(200);

                store.cut();
                long cut = System.nanoTime();
                Thread.sleep(SETTLE.toMillis());
                long acksAtSettle = acks.get();
                long unreadyAfter = -1;
                long budget = HealthTrackingBinStore.DEFAULT_STALL.plusSeconds(10).toNanos();
                while (System.nanoTime() - cut < budget) {
                    if (status(node, HealthService.READY_PATH) == 503) {
                        unreadyAfter = System.nanoTime() - cut;
                        break;
                    }
                    Thread.sleep(200);
                }
                long acksDuringCut = acks.get() - acksAtSettle;
                int live = status(node, HealthService.LIVE_PATH);

                store.heal();
                long healed = System.nanoTime();
                long acksAtHeal = acks.get();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                while (status(node, HealthService.READY_PATH) != 200 || acks.get() == acksAtHeal) {
                    assertThat(System.nanoTime()).as("⚠️ AND IT RECOVERS WHEN THE STORE DOES")
                            .isLessThan(deadline);
                    Thread.sleep(200);
                }
                long recovered = System.nanoTime() - healed;
                stop.set(true);
                for (Thread producer : producers) {
                    producer.join(TimeUnit.SECONDS.toMillis(10));
                }

                System.out.println("M8.15 unready " + TimeUnit.NANOSECONDS.toMillis(unreadyAfter)
                        + " ms after the cut (stall bound "
                        + HealthTrackingBinStore.DEFAULT_STALL.toMillis() + " ms); acks during "
                        + "the cut after a " + SETTLE.toMillis() + " ms settle: " + acksDuringCut
                        + "; recovered " + TimeUnit.NANOSECONDS.toMillis(recovered)
                        + " ms after the heal");
                assertThat(acksDuringCut)
                        .as("⚠️ THE ACKS STOP, COUNTED: a pod that kept returning 202 would be "
                                + "acking records it cannot make durable")
                        .isZero();
                assertThat(unreadyAfter).as("⚠️ READINESS FAILS, within the stall bound + 10 s")
                        .isPositive();
                assertThat(live).as("and liveness does not: the kubelet must not kill a node "
                        + "that is only waiting for the store").isEqualTo(200);
            } finally {
                stop.set(true);
            }
        }
    }
}
