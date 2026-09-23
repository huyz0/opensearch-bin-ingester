// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.OperatingSystemMXBean;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.FetchPolicy;
import io.github.huyz0.os.biningester.ingest.FetchPolicyConfig;
import io.github.huyz0.os.biningester.ingest.GrantIssuer;
import io.github.huyz0.os.biningester.ingest.SegmentCache;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentServing;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** M9.14: separate RustFS observations for cold proxy and signed-URL direct. */
@Timeout(300)
class DirectThresholdMeasurementIT {

    private static final String SEGMENT = "bins/cluster-a/data/direct-threshold";
    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000a9");
    private static final int MAX_FAN_OUT = 16;
    private static final int REPETITIONS = 3;
    private static final int CPU_DELIVERIES_PER_SAMPLE = 1_000;
    private static final int CPU_WARMUP_DELIVERIES = 1_000;
    private static final byte[] SEGMENT_BYTES = segmentBytes();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    void measuresProxyAndDirectSeparatelyAndPinsTheCostChosenDefault() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        var processBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        assumeTrue(processBean.getProcessCpuTime() >= 0,
                "process CPU time is not supported on this rig");

        try (ChaosBucket bucket = ChaosBucket.create()) {
            CountingBinStore store = new CountingBinStore(bucket.observer());
            store.put(SEGMENT, Body.ofBytes(SEGMENT_BYTES));
            var defaults = FetchPolicyConfig.defaultsFor(store.capabilities().costs(), true);
            assertThat(defaults.directFanOutThreshold())
                    .as("only fan-out one ties proxy's one GET with direct's GET count")
                    .isEqualTo(1);

            System.out.println("M9.14 TSV: mode\tfanout\tgets_per_segment\t"
                    + "ingester_bytes\tcpu_ns_median\tcpu_ns_min\tcpu_ns_max");
            for (int fanOut = 1; fanOut <= MAX_FAN_OUT; fanOut++) {
                Observation proxy = measure(store, fanOut,
                        new FetchPolicyConfig(1L, 1L, 0, true), FetchMode.PROXY,
                        processBean);
                Observation direct = measure(store, fanOut,
                        new FetchPolicyConfig(1L, 1L, fanOut, true), FetchMode.DIRECT,
                        processBean);
                FetchMode defaultMode = fanOut <= defaults.directFanOutThreshold()
                        ? FetchMode.DIRECT : FetchMode.PROXY;
                Observation selected = measure(store, fanOut, defaults, defaultMode,
                        processBean);
                assertObservation(proxy, fanOut, FetchMode.PROXY);
                assertObservation(direct, fanOut, FetchMode.DIRECT);
                assertObservation(selected, fanOut,
                        fanOut == 1 ? FetchMode.DIRECT : FetchMode.PROXY);
                print("proxy", fanOut, proxy);
                print("direct", fanOut, direct);
                print("default", fanOut, selected);
            }
        }
    }

    private static Observation measure(CountingBinStore store, int fanOut,
            FetchPolicyConfig policyConfig, FetchMode expectedMode,
            OperatingSystemMXBean processBean) throws Exception {
        List<Long> cpuNanos = new ArrayList<>(REPETITIONS);
        long gets = 0;
        long externalGets = 0;
        long ingesterBytes = 0;
        for (int repetition = 0; repetition < REPETITIONS; repetition++) {
            SubscriptionHub hub = new SubscriptionHub();
            List<SubscriptionHub.Push> pushes = new ArrayList<>(
                    fanOut * (CPU_DELIVERIES_PER_SAMPLE + 1));
            AtomicLong sinkBytes = new AtomicLong();
            List<AutoCloseable> subscriptions = new ArrayList<>(fanOut);
            List<RunCommit> runs = new ArrayList<>(fanOut);
            for (int consumer = 0; consumer < fanOut; consumer++) {
                RunKey key = new RunKey(INDEX, consumer);
                runs.add(new RunCommit(key, 1, consumer));
                subscriptions.add(hub.subscribe(key, published -> {
                    pushes.addAll(published);
                    return (buffer, offset, length) -> sinkBytes.addAndGet(length);
                }));
            }
            SegmentServing serving = new SegmentServing(
                    new FetchPolicy(policyConfig),
                    store.capabilities(),
                    new SegmentProxy(store, 1024,
                            new SegmentCache(SEGMENT_BYTES.length)),
                    new GrantIssuer(store));
            StoreCounts beforeWarmup = store.counts();
            hub.publish(new CommitDelta((long) repetition * CPU_DELIVERIES_PER_SAMPLE,
                            SEGMENT, runs),
                    "bins/cluster-a/data/another-segment", new byte[] {1}, serving);
            long coldGetDelta = store.counts().gets() - beforeWarmup.gets();
            assertThat(coldGetDelta)
                    .as("one cold proxy GET per delivery; direct serving has no ingester GET")
                    .isEqualTo(expectedMode == FetchMode.PROXY ? 1 : 0);
            gets += coldGetDelta;
            for (int delivery = 0; delivery < CPU_WARMUP_DELIVERIES; delivery++) {
                hub.publish(new CommitDelta(
                                (long) repetition * CPU_DELIVERIES_PER_SAMPLE
                                        + CPU_DELIVERIES_PER_SAMPLE + delivery + 1,
                                SEGMENT, runs),
                        "bins/cluster-a/data/another-segment", new byte[] {1}, serving);
            }
            assertThat(store.counts().gets() - beforeWarmup.gets())
                    .as("warmup deliveries reuse the measured segment without extra ingester GETs")
                    .isEqualTo(expectedMode == FetchMode.PROXY ? 1 : 0);
            sinkBytes.set(0);
            pushes.clear();
            StoreCounts beforeCpuBatch = store.counts();
            long cpuStart = processBean.getProcessCpuTime();
            for (int delivery = 0; delivery < CPU_DELIVERIES_PER_SAMPLE; delivery++) {
                hub.publish(new CommitDelta(
                                (long) repetition * CPU_DELIVERIES_PER_SAMPLE + delivery + 1,
                                SEGMENT, runs),
                        "bins/cluster-a/data/another-segment", new byte[] {1}, serving);
            }
            cpuNanos.add((processBean.getProcessCpuTime() - cpuStart)
                    / CPU_DELIVERIES_PER_SAMPLE);
            assertThat(store.counts().gets() - beforeCpuBatch.gets()).isZero();
            ingesterBytes += sinkBytes.get() / CPU_DELIVERIES_PER_SAMPLE;

            assertThat(pushes).hasSize(fanOut * CPU_DELIVERIES_PER_SAMPLE);
            assertThat(pushes.subList(pushes.size() - fanOut, pushes.size()))
                    .allSatisfy(push -> assertThat(push.via()).isEqualTo(expectedMode));
            if (expectedMode == FetchMode.DIRECT) {
                for (var push : pushes.subList(pushes.size() - fanOut, pushes.size())) {
                    assertThat(push.grant()).isNotNull();
                    HttpResponse<byte[]> response = HTTP.send(
                            HttpRequest.newBuilder(URI.create(push.grant().url()))
                                    .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.body()).containsExactly(SEGMENT_BYTES);
                    externalGets++;
                }
            }
            for (AutoCloseable subscription : subscriptions) {
                subscription.close();
            }
        }
        return new Observation((gets + externalGets) / REPETITIONS,
                ingesterBytes / REPETITIONS, median(cpuNanos),
                cpuNanos.stream().mapToLong(Long::longValue).min().orElseThrow(),
                cpuNanos.stream().mapToLong(Long::longValue).max().orElseThrow());
    }

    private static void assertObservation(Observation actual, int fanOut, FetchMode mode) {
        if (mode == FetchMode.PROXY) {
            assertThat(actual.getsPerSegment()).isEqualTo(1);
            assertThat(actual.ingesterBytesPerSegment())
                    .isEqualTo((long) fanOut * SEGMENT_BYTES.length);
        } else {
            // Signed-URL HTTP requests bypass CountingBinStore; each is counted
            // separately and its full body was checked above.
            assertThat(actual.getsPerSegment()).isEqualTo(fanOut);
            assertThat(actual.ingesterBytesPerSegment()).isZero();
        }
    }

    private static long median(List<Long> values) {
        return values.stream().sorted().skip(values.size() / 2).findFirst().orElseThrow();
    }

    private static void print(String mode, int fanOut, Observation o) {
        System.out.printf("M9.14 %s\t%d\t%d\t%d\t%d\t%d\t%d%n", mode,
                fanOut, o.getsPerSegment(), o.ingesterBytesPerSegment(), o.cpuMedianNanos(),
                o.cpuMinNanos(), o.cpuMaxNanos());
    }

    private static byte[] segmentBytes() {
        byte[] bytes = new byte[64 * 1024];
        byte[] marker = "m9.14-direct-threshold".getBytes(StandardCharsets.UTF_8);
        for (int offset = 0; offset < bytes.length; offset++) {
            bytes[offset] = marker[offset % marker.length];
        }
        return bytes;
    }

    private record Observation(long getsPerSegment, long ingesterBytesPerSegment,
            long cpuMedianNanos, long cpuMinNanos, long cpuMaxNanos) { }
}
