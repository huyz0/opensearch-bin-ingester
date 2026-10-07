// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.ServerResponse;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A bulk request gives back everything it holds BEFORE its answer is sent
 * (M13.58): a producer that reads its answer and sends again at once is never
 * refused by the permit of the request it just finished -- and a test that
 * reads {@code inFlight()} after the answer reads what the answer promised.
 *
 * <p>⚠️ RECORDED AT THE SEND, NOT AFTER THE ANSWER ARRIVES: the response
 * handed to the handler records {@code inFlight()} as {@code send} is called,
 * so the order is pinned without a race.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AnswerAfterReleaseTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-0000000000aa");

    private WebServer server;
    private DefaultIngest ingest;
    /** At each send: the lane permits in flight, and whether the index's one slot is free. */
    private final List<String> heldAtSend = new CopyOnWriteArrayList<>();

    /**
     * One request in flight per index, at a rate never reached: a held slot
     * refuses a second admission. ⚠️ NOT UNLIMITED, which tracks no slot.
     */
    private final IndexQuotas quotas = new IndexQuotas(
            new IndexQuotas.Config(new IndexQuotas.Limit(1L << 40, 1L << 40), Map.of(), 1),
            Clock.systemUTC(), java.util.Optional::of, index -> List.of());

    @AfterEach
    void stop() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (ingest != null) {
            ingest.close();
        }
    }

    private ServerResponse recording(ServerResponse real, LaneAdmission admission) {
        ServerResponse[] self = new ServerResponse[1];
        self[0] = (ServerResponse) Proxy.newProxyInstance(ServerResponse.class.getClassLoader(),
                new Class<?>[] {ServerResponse.class}, (proxy, method, args) -> {
                    if (method.getName().equals("send")) {
                        IndexQuotas.Admission probe = quotas.admit("logs");
                        probe.ticket().ifPresent(IndexQuotas.Ticket::release);
                        heldAtSend.add("permits " + admission.inFlight() + ", slot "
                                + (probe.ticket().isPresent() ? "free" : "held"));
                    }
                    Object result;
                    try {
                        result = method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    // the fluent calls keep the recorder in the chain
                    return result == real ? self[0] : result;
                });
        return self[0];
    }

    private WebClient serve(LaneAdmission admission) throws Exception {
        ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(20), 8L << 20, "cluster-a"),
                new MemoryBinStore(), "bins/cluster-a", "pod1",
                TestSequencers.leased(new MemoryBinStore(), "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { },
                new IndexCostLedger());
        BulkService service = new BulkService(ingest, PRINCIPAL, new DrainGate(), admission,
                quotas);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post("/{index}/_bulk",
                        (req, res) -> service.bulk(req, recording(res, admission))))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    @Test
    void aREFUSEDBodyIsAnsweredWithNothingInFlight() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        WebClient client = serve(admission);

        try (HttpClientResponse empty = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit("")) {
            assertThat(empty.status().code()).as("the premise: refused").isEqualTo(400);
        }

        assertThat(heldAtSend).as("⚠️ THE PERMIT AND THE SLOT WENT BACK BEFORE THE 400 WAS SENT")
                .containsExactly("permits 0, slot free");
    }

    @Test
    void anACCEPTEDBodyIsAnsweredWithNothingInFlight() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        WebClient client = serve(admission);

        try (HttpClientResponse accepted = client.post("/logs/_bulk")
                .queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"doc-0\",\"_version\":1}}\n{\"n\":0}\n")) {
            assertThat(accepted.status().code()).as("the premise: accepted").isEqualTo(202);
        }

        assertThat(heldAtSend).as("⚠️ THE SLOT WENT BACK BEFORE THE 202 WAS SENT -- the "
                + "permit already did, as the chunk buffered (M11.7)")
                .containsExactly("permits 0, slot free");
    }
}
