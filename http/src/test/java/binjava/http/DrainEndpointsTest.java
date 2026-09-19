// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.RunKey;
import binjava.ingest.AppendResult;
import binjava.ingest.Ingest;
import binjava.ingest.IndexCatalog;
import binjava.ingest.RetainedFloors;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import binjava.security.Principal;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The three routes a draining node answers differently (M8.7, research 08
 * §7 steps 1 to 3), over a real listener.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DrainEndpointsTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", java.util.Set.of("logs"));
    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    /** An ingest whose append waits until the test lets it finish. */
    private static final class HeldIngest implements Ingest {
        final CountDownLatch inside = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(1);

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws IOException {
            source.forEachRecord(record -> { });
            inside.countDown();
            try {
                finish.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, RecordSource source) throws IOException {
            return append(principal, indexOrAlias, 0, source);
        }

        @Override
        public void close() {
        }
    }

    private final DrainGate gate = new DrainGate();
    private final HeldIngest ingest = new HeldIngest();
    private WebServer server;
    private WebClient client;

    @AfterEach
    void stop() {
        ingest.finish.countDown();
        if (server != null) {
            server.stop();
        }
    }

    private void start() {
        WatermarkTable table = new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1),
                Duration.ofHours(2), Duration.ofMinutes(30));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new HealthService(gate))
                        .register(new BulkService(ingest, PRINCIPAL, gate))
                        .register(new SubscriptionService(new SubscriptionHub(),
                                new IndexCatalog(), table, Clock.systemUTC(),
                                RetainedFloors.unknown(), gate)))
                .build().start();
        client = WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    private int poll(int waitSeconds) {
        try (var response = client.get("/sub/" + STREAM.indexId() + "/" + STREAM.partitionId())
                .queryParam("wait", String.valueOf(waitSeconds)).queryParam("sub", "s1")
                .request()) {
            return response.status().code();
        }
    }

    private int bulk() {
        return client.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"d\",\"_version\":1}}\n{\"n\":1}\n")
                .status().code();
    }

    @Test
    void READINESSGoes503AndLIVENESSStays200() {
        start();
        assertThat(client.get(HealthService.READY_PATH).request().status().code())
                .isEqualTo(200);

        gate.failReadiness();

        assertThat(client.get(HealthService.READY_PATH).request().status().code())
                .as("⚠️ THE LOAD BALANCER STOPS SENDING WORK").isEqualTo(503);
        assertThat(client.get(HealthService.LIVE_PATH).request().status().code())
                .as("⚠️ AND THE KUBELET DOES NOT KILL IT MID-FLUSH").isEqualTo(200);
    }

    @Test
    void aWAITINGPollIsANSWERED503AtOnceNotAfterItsWAIT() throws Exception {
        start();
        CompletableFuture<Integer> answer = CompletableFuture.supplyAsync(() -> poll(30));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (gate.awaitNoPollers(Duration.ZERO) == 0) {
            assertThat(System.nanoTime()).as("the poll never arrived").isLessThan(deadline);
            Thread.onSpinWait();
        }

        long released = System.nanoTime();
        gate.releasePollers();
        int status = answer.get(20, TimeUnit.SECONDS);

        assertThat(status).as("⚠️ TOLD TO GO: a 503 the transport reconnects on")
                .isEqualTo(503);
        assertThat(Duration.ofNanos(System.nanoTime() - released))
                .as("⚠️ AT ONCE, not at the end of the 30 s wait it asked for, which is the "
                        + "stall §7 step 2 exists to prevent")
                .isLessThan(Duration.ofSeconds(5));
        assertThat(poll(1)).as("⚠️ AND A NEW POLL IS REFUSED").isEqualTo(503);
    }

    @Test
    void aBULKInsideTheDoorFINISHESWith202AndANewOneIs503() throws Exception {
        start();
        CompletableFuture<Integer> inside = CompletableFuture.supplyAsync(this::bulk);
        assertThat(ingest.inside.await(10, TimeUnit.SECONDS)).as("the premise: it is inside")
                .isTrue();

        gate.refuseBulk();

        assertThat(bulk()).as("⚠️ A NEW ONE IS REFUSED, WHICH A PRODUCER RETRIES")
                .isEqualTo(503);
        assertThat(gate.bulkInFlight()).as("the refused one never entered").isEqualTo(1);

        ingest.finish.countDown();
        assertThat(inside.get(20, TimeUnit.SECONDS))
                .as("⚠️ THE ONE INSIDE IS ANSWERED, NOT CUT OFF").isEqualTo(202);
        assertThat(gate.awaitNoBulk(Duration.ofSeconds(10))).isZero();
    }
}
