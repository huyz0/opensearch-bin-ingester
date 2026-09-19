// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.backend.MinioFixture;
import binjava.client.HttpSubscriptionTransport;
import binjava.client.SubscriptionTransport;
import binjava.format.RunKey;
import io.helidon.webclient.api.WebClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A rolling restart of every pod: no visibility gap over 1 s, and no
 * thundering herd, at 200 subscribers (M8.16, NFR-9, FR-16, M8 criterion 13).
 *
 * <p>⚠️ **THE TWO NUMBERS ARE WHAT MAKE IT FALSIFIABLE.** If a pod simply
 * closed, every subscriber would wait out the same timeout and reconnect in
 * ONE window, a shape no assertion about connection counts can see. So the
 * reconnects are bucketed into 100 ms windows, and none may carry more than
 * 20% of them.
 *
 * <p>⚠️ **SUBSCRIBERS AND THE PRODUCER GO THROUGH A SERVICE** ({@link
 * ServiceLb}): new connections reach only ready pods, and an open connection
 * stays where it is. The subscribers use the production retry defaults,
 * because the herd is decided by those.
 */
@Timeout(value = 900, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RollingRestartIT {

    private static final int PODS = 3;
    private static final int SUBSCRIBERS = 200;
    private static final int PRODUCERS = 4;
    private static final Duration WINDOW = Duration.ofMillis(100);
    private static final double HERD_BUDGET = 0.20;
    private static final Duration GAP_BUDGET = Duration.ofSeconds(1);

    /**
     * ⚠️ **KEPT, NOT A {@code @TempDir}** (M8.54): a stall this case saw once
     * could not be explained because the pods' logs were deleted with the run.
     * Under the build directory, so {@code ./gradlew clean} still removes them.
     */
    private final Path dir = Path.of(System.getProperty("binjava.repoRoot", "."),
            "server/build/chaos-logs/RollingRestartIT", UUID.randomUUID().toString());

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    private NodeProcess start(ChaosBucket bucket, ServiceLb lb, String pod, UUID index)
            throws Exception {
        Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
        settings.put("ingest.interval-floor", "PT0.05S");
        NodeProcess node = NodeProcess.start(Files.createDirectories(dir.resolve(pod)), pod,
                settings);
        node.registerLogs(index);
        lb.add(node.port());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try (var response = node.client().get("/ready").request()) {
                if (response.status().code() == 200) {
                    break;
                }
            } catch (RuntimeException notYet) {
                // starting
            }
            assertThat(System.nanoTime()).as("%s never became ready", pod).isLessThan(deadline);
            Thread.sleep(100);
        }
        // two probe periods, so the load balancer has seen it ready too
        Thread.sleep(ServiceLb.PROBE_PERIOD.multipliedBy(2).toMillis());
        return node;
    }

    @Test
    void aROLLINGRestartKeepsVISIBILITYAndSPREADSTheReconnects() throws Exception {
        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        ConcurrentLinkedQueue<Long> reconnects = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> acks = new ConcurrentLinkedQueue<>();
        AtomicBoolean restarting = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        List<AutoCloseable> subscriptions = new ArrayList<>();
        List<HttpSubscriptionTransport> transports = new ArrayList<>();
        List<NodeProcess> nodes = new ArrayList<>();
        // ⚠️ PRINTED FIRST, so a run that fails or times out says where its logs are
        System.out.println("M8.16 pod logs in " + dir);
        try (ChaosBucket bucket = ChaosBucket.create(); ServiceLb lb = new ServiceLb()) {
            for (int i = 0; i < PODS; i++) {
                nodes.add(start(bucket, lb, "pod" + i, index));
            }
            for (int s = 0; s < SUBSCRIBERS; s++) {
                AtomicLong connects = new AtomicLong();
                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(lb.endpoint(),
                        () -> {
                            // ⚠️ THE FIRST CONNECT IS NOT A RECONNECT.
                            if (connects.incrementAndGet() > 1 && restarting.get()) {
                                reconnects.add(System.nanoTime());
                            }
                        },
                        HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR,
                        HttpSubscriptionTransport.DEFAULT_RETRY_CEILING, Duration.ofSeconds(5));
                transports.add(transport);
                subscriptions.add(transport.subscribe(stream,
                        (SubscriptionTransport.Listener) delivery -> { }));
            }
            // ⚠️ SEVERAL PRODUCERS, because the gap is the FLEET's: a stretch
            // in which nothing new becomes visible. One serial producer turns
            // every slow request into a "gap" while the fleet commits others'
            // records; a handoff that stalls commits stops them all.
            List<Thread> writers = new ArrayList<>();
            for (int p = 0; p < PRODUCERS; p++) {
                int producerId = p;
                WebClient producer = WebClient.builder().baseUri(lb.endpoint())
                        .connectTimeout(Duration.ofSeconds(2)).readTimeout(Duration.ofSeconds(2))
                        .build();
                writers.add(Thread.ofVirtual().start(() -> {
                    for (int seq = 0; !stop.get(); seq++) {
                        try {
                            int status = producer.post("/logs/_bulk").queryParam("partition", "0")
                                    .submit("{\"index\":{\"_id\":\"w" + producerId + "-" + seq
                                            + "\",\"_version\":1}}\n{\"n\":1}\n")
                                    .status().code();
                            if (status == 202) {
                                acks.add(System.nanoTime());
                            }
                        } catch (RuntimeException retried) {
                            // the pod behind this connection went away: go again
                        }
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException interrupted) {
                            return;
                        }
                    }
                }));
            }
            Thread.sleep(3000);

            long began = System.nanoTime();
            restarting.set(true);
            for (int i = 0; i < PODS; i++) {
                NodeProcess old = nodes.get(i);
                old.terminate();
                lb.remove(old.port());
                nodes.set(i, start(bucket, lb, "podr" + i, index));
            }
            long restarted = System.nanoTime();
            Thread.sleep(3000);
            long ended = System.nanoTime();
            stop.set(true);
            for (Thread writer : writers) {
                writer.join(TimeUnit.SECONDS.toMillis(10));
            }

            // ⚠️ THE GAP RUNS TO `ended`, not to the last ack: a fleet that
            // stopped acking for good has one gap, from its last ack to the end.
            // And it runs FROM the last ack before `began`, so a gap straddling
            // the start counts whole.
            long worstGap = 0;
            long previous = began;
            for (long ack : acks.stream().sorted().toList()) {
                if (ack > ended) {
                    break;
                }
                if (ack >= began) {
                    worstGap = Math.max(worstGap, ack - previous);
                }
                previous = ack;
            }
            worstGap = Math.max(worstGap, ended - previous);
            long acksAfterRestart = acks.stream().filter(a -> a > restarted && a <= ended).count();
            TreeMap<Long, Integer> windows = new TreeMap<>();
            for (long at : reconnects) {
                windows.merge(at / WINDOW.toNanos(), 1, Integer::sum);
            }
            int busiest = windows.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            double share = reconnects.isEmpty() ? 0 : busiest / (double) reconnects.size();

            System.out.println("M8.16 worst visibility gap " + worstGap / 1_000_000 + " ms (budget "
                    + GAP_BUDGET.toMillis() + " ms); reconnects " + reconnects.size()
                    + " over " + windows.size() + " windows of " + WINDOW.toMillis()
                    + " ms, busiest " + busiest + " = " + Math.round(share * 1000) / 10.0
                    + "% (budget " + (int) (HERD_BUDGET * 100) + "%)");
            assertThat(reconnects.size())
                    .as("the premise: the restart really moved the subscribers")
                    .isGreaterThanOrEqualTo(SUBSCRIBERS / 2);
            assertThat(acksAfterRestart)
                    .as("the premise: the restarted fleet acks")
                    .isPositive();
            assertThat(Duration.ofNanos(worstGap))
                    .as("⚠️ NO VISIBILITY GAP OVER 1 s").isLessThanOrEqualTo(GAP_BUDGET);
            assertThat(share)
                    .as("⚠️ NO THUNDERING HERD: NO 100 ms WINDOW CARRIES OVER 20% OF THE "
                            + "RECONNECTS")
                    .isLessThanOrEqualTo(HERD_BUDGET);
        } finally {
            stop.set(true);
            for (AutoCloseable subscription : subscriptions) {
                subscription.close();
            }
            transports.forEach(HttpSubscriptionTransport::close);
            nodes.forEach(NodeProcess::close);
        }
    }
}
