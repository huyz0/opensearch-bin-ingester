// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IngestConfig;
import binjava.ingest.SubscriptionHub;
import binjava.security.Principal;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Criterion 1, against the REAL stack rather than a fake.
 *
 * <p>⚠️ {@code BulkEndpointTest} can only use a recording fake, because when M1.7
 * was written no production {@code Ingest} existed at all (M1.6b). A fake cannot
 * demonstrate the property criterion 1 actually states — that the 202 arrives
 * only once the segment AND its commit delta are durable in the store — because
 * the fake has no store.
 *
 * <p>⚠️ This is NOT joined to the searchable half. architecture.md
 * rule 4 forbids anything depending on {@code http}, including the plugin's test
 * JVM, precisely so Helidon never enters the OpenSearch process. So the walking
 * skeleton is proven on either side of the {@code Ingest} seam — here from the
 * producer's HTTP request down to a decoded record, and in {@code SearchableIT}
 * from a segment up to a search hit — with a real implementation on both sides.
 */
// ⚠️ SEPARATE_THREAD: the default mode only checks the deadline once the test
// METHOD RETURNS, and the natural failure mode here is a HANG -- append parks
// in join(), the request thread never responds, and the WebClient has no read
// timeout. Without this the suite wedges instead of failing.
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkThroughTheRealStackTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-00000000ab01");
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    // ⚠️ THE ACK ORDERING IS NOT RE-PROVEN HERE, and that is deliberate.
    //
    // A version of this test held the commit open with a gated store, issued the
    // POST on another thread, and asserted the response future was not done. It
    // was FLAKY: it failed on the first three runs after compilation and passed
    // ~30 afterwards. `isNotDone()` is one instantaneous sample of an HTTP
    // client's future, so the evidence rested on timing rather than on
    // construction -- and a flaky test is worse than none here, because it is
    // the evidence for a durability claim.
    //
    // The property decomposes into two halves, and BOTH are already proven
    // deterministically, neither by sampling:
    //   (a) append returns only once the segment AND the commit delta are
    //       durable -- DefaultIngestTest.noAppendIsAckedBeforeItsCommitDeltaIsDurable,
    //       which holds the commit open and looks at the future while it is held
    //   (b) BulkService sends 202 only once append has RETURNED --
    //       BulkEndpointTest.theTwoOhTwoIsSentOnlyAfterAppendSucceeds, proved
    //       through the failure path: a handler that acked before delegating has
    //       already sent 202 when the exception arrives, so it cannot answer 503
    //
    // What this file adds over those is the composition: a real socket, a real
    // BulkService and a real DefaultIngest over a real store, with the bytes read
    // back out. Re-asserting (a) and (b) here bought a flake, not evidence.

    @Test
    void theBytesThatLandInTheStoreAreTheRequestsOwn() throws Exception {
        // ⚠️ Reads BACK, because a 202 plus a request count leaves the request's
        // specifics unconstrained: a handler passing a constant partition, or
        // only the first record, produces exactly the same two requests.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(new RunKey(LOGS, 3), pushed::add);
                DefaultIngest ingest = new DefaultIngest(
                        new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"),
                        store, "bins/cluster-a", "pod1", hub, Clock.systemUTC(),
                        index -> LOGS)) {

            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            StringBuilder body = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                body.append("{\"index\":{\"_id\":\"doc-").append(i).append("\",\"_version\":1}}\n")
                        .append("{\"n\":").append(i).append("}\n");
            }
            assertThat(client.post("/logs/_bulk").queryParam("partition", "3")
                    .submit(body.toString()).status().code()).isEqualTo(202);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (pushed.isEmpty()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("no push arrived");
                }
                Thread.onSpinWait();
            }

            // ⚠️ PARTITION 3 and all 100 records, read out of the object that was
            // actually committed -- not out of what the handler was handed.
            byte[] segment;
            try (InputStream in = store.get(pushed.get(0).segmentKey())) {
                segment = in.readAllBytes();
            }
            assertThat(SegmentReader.open(segment).directory())
                    .anyMatch(e -> e.key().equals(new RunKey(LOGS, 3)) && e.recordCount() == 100);
        }
    }

    // ⚠️ The consumer half of the skeleton is NOT tested here, and the reason is
    // a gap rather than a choice: there is no production SubscriptionTransport
    // bridging SubscriptionHub to ConsumerClient. NodeSubscriptions consumes one
    // and every test writes its own fake, so the ingester->consumer seam is
    // joined only inside test fixtures -- the same defect as M1.6b, found the
    // same way. Tracked as M1.11b; adding another fake here would have hidden it.
}
