// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.LocalFsBinStore;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IngestConfig;
import binjava.ingest.SubscriptionHub;
import binjava.security.Principal;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * T12 / criterion 8: a 200 MB {@code _bulk} body is ingested with a 256 MB
 * heap and no OOM. Run ONLY via the {@code memoryBoundTest} Gradle task
 * (M1.18), which caps this JVM at {@code -Xmx256m} — under the default
 * {@code test} task's 512 MB, this would prove nothing about the 256 MB
 * budget SPEC criterion 8 actually names.
 *
 * <p>⚠️ Two separate proofs, not one, matching T12's own description: an
 * unbounded accumulate ends in a real {@code OutOfMemoryError} under the real
 * {@code -Xmx256m} this test runs under — that alone is already a genuine,
 * JVM-enforced ceiling, not a sampled estimate. The ratio and absolute
 * assertions below are a SECOND, more diagnostic proof, meant for a
 * regression that grew memory without quite tripping the JVM's own OOM
 * killer. ⚠️ Honestly: every mutation tried against this test (chunking fully
 * disabled, and at 50x/5x/2x the real chunk size) crashed with a hard OOM
 * before a response returned, so these two assertions have only ever been
 * exercised on the PASSING path — they are reasoned defense-in-depth for a
 * slower-growing regression class, not (yet) independently mutation-killed
 * the way the hard-OOM signal is.
 *
 * <p>⚠️ Both the request body and the backing store are STREAMED. The client
 * writes records straight to the request's own output stream (never a
 * {@code String}/{@code byte[]} holding the whole 200 MB), and
 * {@link LocalFsBinStore} writes segments to real files, not to a
 * {@code Map} in this JVM's heap — {@code MemoryBinStore} would retain every
 * segment forever and make THIS FIXTURE the thing that runs out of heap,
 * not the code under test.
 */
class MemoryFlatUnderTenXBodySizeTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    // ⚠️ 480s, not 300s: measured in a clean, isolated run at ~254s (review
    // round 2) -- 300s left only ~46s of margin, thin enough that ordinary
    // JIT/GC variance on a slower machine could turn a real pass into a
    // spurious JUnit timeout, which (per this test's own documented check-tdd
    // blind spot) surfaces as an uninformative <skipped/>, not a clear signal.
    @Test
    @Timeout(value = 480, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void memoryFlatUnderTenXBodySize() throws Exception {
        Path root = Files.createTempDirectory("binstore-memflat");
        LocalFsBinStore store = new LocalFsBinStore(root);
        SubscriptionHub hub = new SubscriptionHub();
        UUID logs = UUID.randomUUID();

        try (DefaultIngest ingest = new DefaultIngest(IngestConfig.defaults("cluster-a"),
                store, "bins/cluster-a", "pod1", hub, Clock.systemUTC(), index -> logs)) {
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            long targetBytes = 200L << 20;
            MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
            List<long[]> samples = new CopyOnWriteArrayList<>();
            AtomicLong written = new AtomicLong();
            AtomicBoolean sampling = new AtomicBoolean(true);

            // ⚠️ System.gc() BEFORE every sample, not a raw read every 20ms.
            // Raw "used" heap between GC cycles measures accumulated garbage,
            // not the live set -- it climbs for reasons unrelated to what this
            // test asserts and falls sharply the moment a collection happens,
            // which made an early draft of this test genuinely flaky (a
            // passing run's own rerun failed at a sample mid-transfer, with
            // nothing in the code different). A forced collection before each
            // read gives the LIVE set at that moment, which is what "does
            // memory grow with how much of the body has been consumed" is
            // actually asking. Sampling every second instead of every 20ms
            // keeps the added GC pauses a small fraction of the run.
            Thread sampler = new Thread(() -> {
                while (sampling.get()) {
                    System.gc();
                    samples.add(new long[] {written.get(), memBean.getHeapMemoryUsage().getUsed()});
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                // ⚠️ One final sample after the request completes: the last
                // in-loop sample can land well before the response arrives,
                // missing whatever the tail of the request actually cost.
                System.gc();
                samples.add(new long[] {written.get(), memBean.getHeapMemoryUsage().getUsed()});
            }, "mem-sampler");
            // ⚠️ Daemon: if the request under test ever hangs, JUnit's own
            // @Timeout fires on the test's thread, but this independent thread
            // would otherwise keep calling System.gc() once a second forever,
            // turning a prompt, specific timeout signal into a stall bounded
            // only by the Gradle task's own outer timeout.
            sampler.setDaemon(true);
            sampler.start();

            String pad = "x".repeat(180);
            var response = client.post("/logs/_bulk").queryParam("partition", "0")
                    .outputStream(out -> {
                        long total = 0;
                        int i = 0;
                        while (total < targetBytes) {
                            byte[] action = ("{\"index\":{\"_id\":\"d" + i
                                    + "\",\"_version\":1}}\n").getBytes(StandardCharsets.UTF_8);
                            byte[] doc = ("{\"p\":\"" + pad + "\"}\n")
                                    .getBytes(StandardCharsets.UTF_8);
                            out.write(action);
                            out.write(doc);
                            total += action.length + doc.length;
                            written.set(total);
                            i++;
                        }
                        out.close();
                    });

            sampling.set(false);
            sampler.join();

            assertThat(response.status().code()).isEqualTo(202);

            long peak = samples.stream().mapToLong(s -> s[1]).max().orElseThrow();
            assertThat(peak)
                    .as("peak heap used, sampled (GC-then-read) every second during a "
                            + "200 MB ingest run under -Xmx256m")
                    .isLessThan(220L << 20);

            // ⚠️ THE RATIO CHECK T12's own name promises: at every sample, heap
            // used is no more than 10x whatever fraction of the body had been
            // WRITTEN so far -- not the eventual 200 MB total. A FLOOR keeps
            // early samples (dominated by JVM/Helidon baseline overhead, not by
            // the few KB of body sent so far) from tripping this spuriously.
            // ⚠️ 128 MiB, not a tighter guess: measured baseline (JaCoCo
            // disabled, a few hundred KB into the stream) varies run to run in
            // the 60-70 MB range -- a 64 MiB floor failed once at 69 MB with
            // nothing wrong, on a rerun of the exact same code. 128 MiB gives
            // real headroom over that noise while staying far below the 220 MB
            // peak ceiling and further still below what the disabled-chunking
            // mutation actually produces (a real OutOfMemoryError, not a
            // near-miss number).
            long floor = 128L << 20;
            for (long[] s : samples) {
                long bound = Math.max(floor, 10 * s[0]);
                assertThat(s[1])
                        .as("heap used (%d) vs. 10x bytes written so far (%d) or the floor",
                                s[1], s[0])
                        .isLessThanOrEqualTo(bound);
            }
        }
    }
}
