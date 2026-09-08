// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.LocalFsBinStore;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IngestConfig;
import binjava.ingest.SubscriptionHub;
import binjava.security.Principal;
import binjava.sequencer.TestSequencers;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M3.5; M3 SPEC acceptance criterion 5 / T12, as amended by ADR-0026.
 * {@link MemoryFlatUnderTenXBodySizeTest} (M1.18) proves NFR-6 only at the
 * fixed 250 ms operating point. This sibling proves the same flatness with
 * the interval held at its 5 s ceiling, where the accumulator carries ~5.2 MiB
 * between flushes instead of ~223 KB.
 *
 * <p>Run ONLY via {@code memoryBoundCeilingTest} -- see
 * {@link MemoryFlatUnderTenXBodySizeTest}'s javadoc for why the ordinary
 * {@code test} task's 512 MB heap would prove nothing here either.
 *
 * <p>⚠️ CONCURRENT PRODUCERS, NOT ONE. ADR-0026, measured: {@code BulkService}
 * appends in BLOCKING 1000-record chunks, so ONE producer advances exactly one
 * chunk per flush and never holds more than ~223 KB in the accumulator -- at
 * the ceiling AND at the floor. A single-producer test therefore cannot reach
 * the state criterion 5 is about and would pass while exercising nothing. Only
 * concurrency puts real data in the accumulator, because N blocking producers
 * contribute N chunks to the same flush.
 *
 * <p>⚠️ THE MIDDLE BAND IS THE TARGET, and this is the subtle part. {@code
 * Accumulator.adaptInterval} only LEAVES the interval alone when {@code
 * fillRatio} is strictly between the two thresholds -- that is its {@code else}
 * branch. Above {@code fillRatioHighThreshold} the interval SHORTENS to the
 * floor immediately ({@code intervalShortenDelay} is zero), which would destroy
 * the state under test on the very first drain; below the low threshold is
 * where a lone producer already sits. So the producer count is chosen to land
 * mid-band: {@value #PRODUCERS} producers x ~223 KB is ~5.2 MiB against an
 * 8 MiB target, a ratio near 0.65 with real margin to both 0.4 and 0.9.
 *
 * <p>⚠️ The band is NOT asserted tightly, deliberately (round-2 review of
 * M3.6). The arithmetic above counts BODY bytes, while {@code fillRatio} is
 * measured on the SERIALISED SEGMENT, whose per-record cost differs (the action
 * line is parsed away; framing is added), so the true ratio is nearer 0.62 and
 * the safe band is ~16-34 producers. What this test pins is the OBSERVABLE
 * consequence -- flush cadence stays ceiling-governed -- not an internal ratio
 * it cannot see from {@code http} anyway.
 *
 * <p>⚠️ REACHING THE CEILING AT ALL REQUIRES A WARM-UP, and skipping it is a
 * trap: an {@link binjava.ingest.Accumulator} STARTS at the floor, and the
 * middle band leaves the interval UNTOUCHED. So concurrent producers alone
 * would sit at ~0.65 forever without ever lengthening -- the run would be
 * M1.18's 250 ms regime under a different name. {@link WarpableClock} jumps the
 * same clock {@code DefaultIngest} reads past {@code intervalLengthenDelay}
 * between two tiny low-{@code fillRatio} flushes, which is exactly the
 * sustained-low sequence {@code adaptInterval} needs, without waiting two real
 * minutes. After that the offset never moves again, so the clock tracks real
 * time and the 5 s ceiling governs real cadence for the rest of the run.
 */
class MemoryFlatAtIntervalCeilingTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    /** ~5.2 MiB per ceiling interval against an 8 MiB target -- see the class javadoc. */
    static final int PRODUCERS = 24;

    private static final long TARGET_BYTES = 200L << 20;

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Real wall-clock time, plus an offset this test can jump forward instantly. */
    private static final class WarpableClock extends Clock {
        private final AtomicLong offsetMillis = new AtomicLong();

        @Override
        public long millis() {
            return System.currentTimeMillis() + offsetMillis.get();
        }

        void jumpForward(Duration d) {
            offsetMillis.addAndGet(d.toMillis());
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static void sendTinyBulk(WebClient client) {
        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .outputStream(out -> {
                    out.write("{\"index\":{\"_id\":\"warmup\",\"_version\":1}}\n"
                            .getBytes(StandardCharsets.UTF_8));
                    out.write("{\"p\":\"x\"}\n".getBytes(StandardCharsets.UTF_8));
                    out.close();
                });
        assertThat(response.status().code()).as("warm-up flush accepted").isEqualTo(202);
    }

    /** ⚠️ 2 files per object (data + `.v`), 2 objects per flush (segment + delta). */
    private static long countFiles(Path root) throws Exception {
        try (var walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).count();
        }
    }

    // ⚠️ 900s. The ceiling path moves ~1 MiB/s aggregate against M1.18's
    // ~806 KB/s, and the per-second forced GC has ~5.2 MiB of live
    // accumulator to trace rather than ~223 KB. ADR-0026's own estimate is
    // ~192s of streaming; this leaves margin over that for warm-up, GC
    // variance and a slower machine without being so wide that a genuine
    // hang stops looking like one.
    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void memoryFlatWithTheIntervalAtItsCeiling() throws Exception {
        Path root = Files.createTempDirectory("binstore-memflat-ceiling");
        LocalFsBinStore store = new LocalFsBinStore(root);
        SubscriptionHub hub = new SubscriptionHub();
        UUID logs = UUID.randomUUID();
        WarpableClock clock = new WarpableClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");

        try (DefaultIngest ingest = new DefaultIngest(config, store, "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), hub, clock, index -> logs)) {
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            // ---- warm-up: two tiny (low-fillRatio) flushes, more than
            // intervalLengthenDelay apart BY THE CLOCK, which is the
            // sustained-low sequence adaptInterval needs to jump to the
            // ceiling. See the class javadoc for why this cannot be skipped.
            // ⚠️ NO sleep between these, deliberately (test-reviewer round 1).
            // An earlier draft slept 300ms "so the flusher has drained before
            // the clock jumps", which is false: `Ingest.append` BLOCKS until
            // its records are durable (FR-4), so the 202 already proves the
            // drain -- and therefore `adaptInterval` -- has run. testing.md
            // rule 15: a sleep whose justification is wrong is a sleep that
            // will be copied.
            sendTinyBulk(client);
            clock.jumpForward(config.intervalLengthenDelay().plusSeconds(5));
            sendTinyBulk(client);

            // ---- the ceiling is now in force. Prove it by OBSERVATION rather
            // than by trusting adaptInterval: one more tiny body must now take
            // about the ceiling to come back, where at the floor it took the
            // ~250ms the two warm-up calls above just did.
            long probeStart = System.currentTimeMillis();
            sendTinyBulk(client);
            long probeMillis = System.currentTimeMillis() - probeStart;
            assertThat(probeMillis)
                    .as("a tiny body's 202 now waits ~the 5s ceiling (%d ms), not the 250ms floor "
                            + "-- the warm-up actually lengthened the interval", probeMillis)
                    .isGreaterThan(2_000L);

            long loadStartMillis = System.currentTimeMillis();

            MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
            List<long[]> samples = new CopyOnWriteArrayList<>();
            AtomicLong written = new AtomicLong();
            AtomicBoolean sampling = new AtomicBoolean(true);
            List<Throwable> samplerFailures = new CopyOnWriteArrayList<>();

            // ⚠️ Same GC-then-read sampling as MemoryFlatUnderTenXBodySizeTest
            // -- see that class's comment for why a raw read between GCs would
            // measure accumulated garbage rather than the live set.
            // ⚠️ Each sample also carries a TIMESTAMP and the store's file
            // count, because the cadence assertion below has to be judged per
            // WINDOW, not averaged over the whole load -- see there.
            // ⚠️ Order matters: heap is read straight after the collection,
            // and countFiles' own walk allocates, so it runs LAST and its
            // garbage is cleared by the next cycle's gc rather than inflating
            // this cycle's reading.
            Runnable sample = () -> {
                System.gc();
                long heap = memBean.getHeapMemoryUsage().getUsed();
                    // ⚠️ RETRIED, and the failure is RECORDED rather than thrown
                // (both round-2 reviewers, each having MEASURED it): walking
                // the store races LocalFsBinStore's own `put*.tmp` -> final
                // rename, so `Files.walk` can throw
                // UncheckedIOException(NoSuchFileException) when an
                // enumerated entry vanishes before it is stat'd -- roughly 1
                // run in 200-400. A sampler that died there would take every
                // assertion below down with it SILENTLY: it is a daemon, so
                // `join()` returns at once and the cadence, peak and ratio
                // checks would all judge whatever prefix had been collected
                // and PASS. A test that can go green having measured nothing
                // is the one failure mode this file exists to prevent.
                long files = -1;
                for (int attempt = 0; attempt < 3 && files < 0; attempt++) {
                    try {
                        files = countFiles(root);
                    } catch (Exception e) {
                        if (attempt == 2) {
                            samplerFailures.add(e);
                            return;
                        }
                    }
                }
                samples.add(new long[] {written.get(), heap, System.currentTimeMillis(), files});
            };
            Thread sampler = new Thread(() -> {
                try {
                    while (sampling.get()) {
                        sample.run();
                        try {
                            Thread.sleep(1_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    sample.run();
                } catch (Throwable t) {
                    // ⚠️ Anything at all, for the same reason: an unrecorded
                    // death here is a green run that measured a prefix.
                    samplerFailures.add(t);
                }
            }, "mem-sampler");
            sampler.setDaemon(true);
            sampler.start();

            // ---- PRODUCERS concurrent streams, TARGET_BYTES between them.
            long perProducer = TARGET_BYTES / PRODUCERS;
            CountDownLatch ready = new CountDownLatch(PRODUCERS);
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> producers = new ArrayList<>();
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            for (int p = 0; p < PRODUCERS; p++) {
                int id = p;
                Thread t = new Thread(() -> {
                    try {
                        ready.countDown();
                        go.await();
                        String pad = "x".repeat(180);
                        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                                .outputStream(out -> {
                                    long total = 0;
                                    int i = 0;
                                    while (total < perProducer) {
                                        byte[] action = ("{\"index\":{\"_id\":\"p" + id + "d" + i
                                                + "\",\"_version\":1}}\n")
                                                .getBytes(StandardCharsets.UTF_8);
                                        byte[] doc = ("{\"p\":\"" + pad + "\"}\n")
                                                .getBytes(StandardCharsets.UTF_8);
                                        out.write(action);
                                        out.write(doc);
                                        total += action.length + doc.length;
                                        // ⚠️ Across ALL producers: what the 10x
                                        // ratio below is measured against.
                                        written.addAndGet(action.length + doc.length);
                                        i++;
                                    }
                                    out.close();
                                });
                        assertThat(response.status().code()).isEqualTo(202);
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                }, "producer-" + id);
                t.start();
                producers.add(t);
            }
            // ⚠️ Released together: producers that trickled in one at a time
            // would each be a LONE producer for their first flushes, which is
            // the ~223 KB regime this test exists not to be in.
            ready.await();
            go.countDown();
            for (Thread t : producers) {
                t.join();
            }

            sampling.set(false);
            sampler.join();

            assertThat(samplerFailures).as("the sampler survived -- otherwise every "
                    + "assertion below judges only the prefix it managed to collect").isEmpty();
            assertThat(samples.get(samples.size() - 1)[2])
                    .as("the last sample lands at the END of the load, not at a silent "
                            + "sampler death part-way through")
                    .isGreaterThanOrEqualTo(System.currentTimeMillis() - 5_000);
            assertThat(failures).as("no producer failed").isEmpty();
            assertThat(written.get()).as("the whole modeled body was streamed")
                    .isGreaterThanOrEqualTo(TARGET_BYTES - PRODUCERS * 512L);

            // ---- the ceiling governed the WHOLE load, not just its start.
            // ⚠️ 2.0 files/s, and the threshold is MEASURED, not derived from
            // the 250ms/5s ratio. The naive derivation says the floor gives
            // 16 files/s, but it does not here: at the floor the PRODUCERS
            // bound the flush rate, not the timer -- 24 blocking producers
            // can only supply chunks so fast -- and a run mutated to shorten
            // mid-load measured 4.33 files/s, not 16. Against that, a 4.0
            // threshold had only 8% margin. Measured on the clean path: 182
            // windows, max 0.792 files/s, median 0.790 (one 4-file flush per
            // 5s ceiling interval, as predicted). So 2.0 sits 2.5x above the
            // real ceiling maximum and 2.2x below the real floor regime.
            //
            // ⚠️ MEASURED PER WINDOW, NOT AVERAGED OVER THE LOAD, and this is
            // the whole point of the assertion (test-reviewer round 1, who
            // demonstrated the hole with a run rather than an argument). The
            // flush COUNT is interval-INVARIANT here: 24 blocking producers
            // contribute one chunk per flush whatever the interval, so the
            // same ~152 files are written either way and only the elapsed
            // time differs. An average over a wall clock the ceiling phase
            // dominates therefore stays under 4 files/s even when most of the
            // body went through at the floor -- a mutation shortening the
            // interval after 13 drains put ~77% of the 200 MB through the
            // 250ms regime and the averaged check passed it, green, in 66s.
            // A sliding window catches that. ⚠️ Not instantly, and the
            // arithmetic is worth stating: a window mixing ceiling and floor
            // needs ~3.4s of floor regime inside its 10s to clear 2.0 files/s,
            // so a floor stretch confined to roughly the last 5% of the body
            // would still slip through. It kills any SUSTAINED shortening,
            // which is the realistic failure and the one demonstrated.
            //
            // ⚠️ The window is 10s, not one sample: at the ceiling a single
            // 1s sample legitimately holds either 0 or 4 files (one flush),
            // and 4 files in ~1s IS 4 files/s -- a per-sample bound would sit
            // exactly on the threshold and flake. Over 10s the ceiling gives
            // ~8 files (0.8/s) against the floor regime's ~43 (4.33/s).
            // ⚠️ ~43, NOT the ~160 a 16-files/s derivation would predict --
            // see the measured note above. Sizing headroom from 16 would read
            // as 8x margin where there is 2.2x.
            List<long[]> loadSamples = samples.stream()
                    .filter(x -> x[2] >= loadStartMillis).toList();
            assertThat(loadSamples).as("enough samples to judge cadence").hasSizeGreaterThan(10);
            long windowMillis = 10_000;
            for (int i = 0; i < loadSamples.size(); i++) {
                for (int j = i + 1; j < loadSamples.size(); j++) {
                    long spanMillis = loadSamples.get(j)[2] - loadSamples.get(i)[2];
                    if (spanMillis < windowMillis) {
                        continue;
                    }
                    long filesInWindow = loadSamples.get(j)[3] - loadSamples.get(i)[3];
                    assertThat((double) filesInWindow / (spanMillis / 1000.0))
                            .as("files/s over a %dms window during the load (%d files) -- every "
                                    + "window must reflect the 5s ceiling, not the 250ms floor",
                                    spanMillis, filesInWindow)
                            .isLessThan(2.0);
                    break;  // one window per start point is enough
                }
            }

            long peak = samples.stream().mapToLong(s -> s[1]).max().orElseThrow();
            assertThat(peak)
                    .as("peak heap used, sampled (GC-then-read) every second during a 200 MB "
                            + "ingest at the interval's ceiling, under -Xmx256m")
                    .isLessThan(220L << 20);

            // ⚠️ Same ratio/floor check as MemoryFlatUnderTenXBodySizeTest --
            // see that class's comment for the 128 MiB floor's rationale.
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
