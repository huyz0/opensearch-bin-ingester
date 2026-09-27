// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.ingest.PlacementRefusedException;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@code _bulk} is admitted through the lanes' fair share of the in-flight
 * budget (M10.8, ADR-0074 decision 6, M10 criterion 11): refused {@code 429}
 * -- never a 5xx -- when saturated, and the permit returned however the
 * request ends.
 *
 * <p>⚠️ THE BUDGET IS 2 OVER THE ONE LANE {@code {0}}, whose floor is 1, so
 * the pod admits at most two requests and two leaked permits are a refusal.
 * The release tests send their requests one after another, so a leak is seen
 * as a 429 on a LATER request -- never read from a counter the handler may not
 * have decremented yet when its answer arrives; and the spare permit is what
 * keeps a release still in flight from reading as a leak.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkServiceAdmissionTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final String BODY = "{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n";

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * An ingest whose n-th append does what {@code outcomes[n]} says, and
     * which schedules lanes 0 and 1 only.
     */
    private static final class ScriptedIngest implements Ingest {
        private final List<String> outcomes;
        private final LaneAdmission admission;
        final AtomicInteger appends = new AtomicInteger();
        final List<Integer> inFlightDuringAppend = new ArrayList<>();

        ScriptedIngest(LaneAdmission admission, String... outcomes) {
            this.admission = admission;
            this.outcomes = List.of(outcomes);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws IOException {
            int n = appends.getAndIncrement();
            inFlightDuringAppend.add(admission.inFlight());
            source.forEachRecord(r -> { });
            switch (outcomes.get(n)) {
                case "io" -> throw new IOException("the store is down");
                case "bug" -> throw new IllegalStateException("an invariant broke");
                case "refused" -> throw new PlacementRefusedException("no such partition");
                default -> {
                    return new AppendResult(1, 0L, 0L);
                }
            }
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource source) throws IOException {
            return append(principal, index, partition, source);
        }

        @Override
        public boolean acceptsLane(byte lane) {
            return lane == 0 || lane == 1;
        }

        @Override
        public void close() {
        }
    }

    private WebClient serve(Ingest ingest, LaneAdmission admission) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(
                        new BulkService(ingest, PRINCIPAL, new DrainGate(), admission)))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    private static int post(WebClient client) {
        try (HttpClientResponse answer = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(BODY)) {
            return answer.status().code();
        }
    }

    private static int post(WebClient client, int lane) {
        try (HttpClientResponse answer = client.post("/logs/_bulk")
                .queryParam("partition", "0").queryParam("lane", Integer.toString(lane))
                .submit(BODY)) {
            return answer.status().code();
        }
    }

    @Test
    void theRequestsOwnLaneIsAdmittedBelowItsFloorWhileAnotherSaturatesThePod() {
        // Budget 2 over {0, 1}: both floors are 1, and lane 0 fills the budget.
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0, (byte) 1));
        ScriptedIngest ingest = new ScriptedIngest(admission, "ok");
        WebClient client = serve(ingest, admission);
        admission.tryAcquire((byte) 0).orElseThrow();
        admission.tryAcquire((byte) 0).orElseThrow();

        assertThat(post(client, 1)).as("lane 1 holds nothing of its floor").isEqualTo(202);
        assertThat(post(client, 0)).as("lane 0 is past its floor").isEqualTo(429);
        assertThat(ingest.appends).hasValue(1);
    }

    @Test
    void anInactiveLaneIsItsPermanentBadRequestEvenOnASaturatedPod() {
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0, (byte) 1));
        ScriptedIngest ingest = new ScriptedIngest(admission);
        WebClient client = serve(ingest, admission);
        admission.tryAcquire((byte) 0).orElseThrow();
        admission.tryAcquire((byte) 0).orElseThrow();

        assertThat(post(client, 2))
                .as("a 429 would be retried for a lane that will never be accepted")
                .isEqualTo(400);
        assertThat(admission.inFlight()).as("and it spent no permit").isEqualTo(2);
        assertThat(ingest.appends).hasValue(0);
    }

    @Test
    void aSaturatedPodAnswersTooManyRequestsAndAppendsNothing() {
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0));
        ScriptedIngest ingest = new ScriptedIngest(admission, "ok");
        WebClient client = serve(ingest, admission);
        LaneAdmission.Permit held = admission.tryAcquire((byte) 0).orElseThrow();
        admission.tryAcquire((byte) 0).orElseThrow();
        assertThat(admission.tryAcquire((byte) 0)).as("the pod is saturated").isEmpty();

        try (HttpClientResponse answer = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(BODY)) {
            assertThat(answer.status().code()).isEqualTo(429);
            assertThat(answer.as(String.class)).contains("retry");
        }
        assertThat(ingest.appends).as("a refused request is never appended").hasValue(0);

        held.release();
        assertThat(post(client)).as("a returned permit readmits").isEqualTo(202);
    }

    @Test
    void thePermitIsHeldWhileAppendingAndReturnedAfterEachSuccess() {
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0));
        ScriptedIngest ingest = new ScriptedIngest(admission, "ok", "ok", "ok", "ok");
        WebClient client = serve(ingest, admission);

        for (int i = 0; i < 4; i++) {
            assertThat(post(client)).as("request %d", i).isEqualTo(202);
        }
        assertThat(ingest.inFlightDuringAppend).as("each request held a permit while appending")
                .hasSize(4).allSatisfy(n -> assertThat(n).isPositive());
    }

    @Test
    void thePermitIsReturnedAfterEachFailureWhateverItsStatus() {
        LaneAdmission admission = new LaneAdmission(2, LaneSet.of((byte) 0));
        ScriptedIngest ingest = new ScriptedIngest(admission, "io", "bug", "bug", "refused", "ok");
        WebClient client = serve(ingest, admission);

        assertThat(post(client)).isEqualTo(503);
        // ⚠️ TWICE: a defect escaping the handler is the one failure no catch
        // answers, and one leak alone still fits the spare permit.
        assertThat(post(client)).isEqualTo(500);
        assertThat(post(client)).isEqualTo(500);
        assertThat(post(client)).isEqualTo(400);
        try (HttpClientResponse unparseable = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit("not a bulk body\n")) {
            assertThat(unparseable.status().code()).isEqualTo(400);
        }
        assertThat(post(client)).as("six requests, and none leaked a permit").isEqualTo(202);
        assertThat(ingest.inFlightDuringAppend).hasSize(5)
                .allSatisfy(n -> assertThat(n).isPositive());
    }
}
