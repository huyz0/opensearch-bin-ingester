// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import binjava.ingest.AppendResult;
import binjava.ingest.Ingest;
import binjava.security.Principal;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * T6b: the adapter owns no decision. It parses, delegates, and reports — it does
 * not recompute the partition, rewrite an {@code _id}, or acknowledge before the
 * records are durable.
 */
class BulkEndpointTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", java.util.Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Records exactly what the adapter handed down, and when. */
    private static final class RecordingIngest implements Ingest {
        final List<String> indices = new ArrayList<>();
        final List<Integer> partitions = new ArrayList<>();
        final List<SegmentRecord> records = new ArrayList<>();
        final AtomicBoolean durable = new AtomicBoolean();
        volatile RuntimeException reject;
        volatile IOException unavailable;

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                List<SegmentRecord> batch) throws IOException {
            if (reject != null) {
                throw reject;
            }
            if (unavailable != null) {
                throw unavailable;
            }
            indices.add(index);
            partitions.add(partition);
            records.addAll(batch);
            durable.set(true);
            return new AppendResult(batch.size(), 0L, batch.size() - 1L);
        }

        @Override
        public void close() {
        }
    }

    private WebClient start(Ingest ingest) {
        server = WebServer.builder()
                .port(0)
                .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                .build()
                .start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    @Test
    void httpAdapterPassesTheRequestThroughUnaltered() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        String body = """
                {"index":{"_id":"doc-1","_version":7}}
                {"n":1}
                {"delete":{"_id":"gone","_version":8}}
                {"create":{"_id":"fresh"}}
                {"n":2}
                """;
        var response = client.post("/logs/_bulk").queryParam("partition", "3")
                .submit(body);

        assertThat(response.status().code()).isEqualTo(202);

        // ⚠️ The index and partition are the REQUEST's, not recomputed. A handler
        // that hashed the _id to choose a partition would put 3 somewhere else.
        assertThat(ingest.indices).containsExactly("logs");
        assertThat(ingest.partitions).containsExactly(3);

        // ⚠️ Ids, op types and versions arrive unaltered and in order.
        assertThat(ingest.records).hasSize(3);
        assertThat(ingest.records.stream().map(SegmentRecord::id))
                .containsExactly("doc-1", "gone", "fresh");
        assertThat(ingest.records.stream().map(SegmentRecord::opType))
                .containsExactly(OpType.INDEX, OpType.DELETE, OpType.CREATE);
        assertThat(ingest.records.stream().map(SegmentRecord::version))
                .containsExactly(OptionalLong.of(7), OptionalLong.of(8), OptionalLong.empty());
        assertThat(new String(ingest.records.get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":1}");
        assertThat(ingest.records.get(1).payload()).isEmpty();
    }

    @Test
    void theTwoOhTwoIsSentOnlyAfterAppendSucceeds() {
        // ⚠️ Criterion 1: 202 means DURABLE, not accepted-into-a-buffer. Proved
        // through the FAILURE path, with no clock: if append fails, the producer
        // must not see 202. A handler that acked before delegating has already
        // sent 202 by the time the exception arrives, so it cannot produce 503 --
        // which makes this a deterministic ordering proof. An earlier version
        // used a 150 ms sleep and a flag; that worked, but testing.md rule 15
        // forbids the sleep and this is strictly stronger.
        RecordingIngest ingest = new RecordingIngest();
        ingest.unavailable = new IOException("the store is unavailable");
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n");

        // ⚠️ 503, not 500: the store is unavailable and the producer should retry
        // the same batch, which an external version makes safe. A 500 tells it
        // the request was bad and must not be retried.
        assertThat(response.status().code()).isEqualTo(503);
        assertThat(ingest.durable).isFalse();
    }

    @Test
    void anIndexOutsideThePrincipalsAllowListIsForbidden() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        // ⚠️ PRINCIPAL allows "logs" only. The 403 comes from the allow-list,
        // not from the fake throwing -- so this still passes when append() is
        // never reached, which is the point: nothing must be appended.
        var response = client.post("/audit/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n");

        assertThat(response.status().code()).isEqualTo(403);
        assertThat(ingest.records).as("nothing is appended for a forbidden index").isEmpty();
        // ⚠️ And NO body at all -- not an empty string, no entity. Naming the
        // index tells an unauthorised caller which indices exist (security.md),
        // so the 403 is bare on purpose.
        assertThat(response.entity().hasEntity())
                .as("a 403 body would disclose which indices exist")
                .isFalse();
    }

    @Test
    void aMalformedBodyIsFourHundredAndNothingIsAppended() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"update\":{\"_id\":\"a\"}}\n{\"doc\":{}}\n");

        assertThat(response.status().code()).isEqualTo(400);
        assertThat(ingest.records).isEmpty();
    }

    @Test
    void aBodyOverTheCapIsFourThirteenRatherThanAnOutOfMemoryError() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        // ⚠️ Just over the cap. An unbounded accumulate ends in an
        // OutOfMemoryError, which is an Error: it reaches neither the 400 nor
        // the 503 path and takes the connection down with it. A 413 is a
        // refusal the producer can see and act on.
        StringBuilder body = new StringBuilder();
        String pad = "z".repeat(1000);
        for (int i = 0; body.length() <= BulkService.MAX_BODY_BYTES; i++) {
            body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n")
                    .append("{\"p\":\"").append(pad).append("\"}\n");
        }
        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString());

        assertThat(response.status().code()).isEqualTo(413);
        assertThat(ingest.records).isEmpty();
    }

    @Test
    void aRequestOverTheRecordCeilingIsFourThirteenEvenWhenItIsSmallInBytes() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        // ⚠️ The adversarial shape the BYTE cap does not catch. The smallest
        // legal record is 24 bytes, so a body far under 32 MiB still retains
        // ~108 B per record and can outweigh the heap. This body is ~5 MB and
        // carries well over MAX_RECORDS.
        StringBuilder body = new StringBuilder();
        for (int i = 0; i <= BulkService.MAX_RECORDS; i++) {
            body.append("{\"index\":{\"_id\":\"a\"}}\n1\n");
        }
        assertThat(body.length())
                .as("well inside the byte cap, so only the record ceiling can refuse it")
                .isLessThan((int) BulkService.MAX_BODY_BYTES);

        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit(body.toString());

        assertThat(response.status().code()).isEqualTo(413);
        assertThat(ingest.records).isEmpty();
    }

    @Test
    void anInternalInvariantFailureIsNotReportedAsForbidden() {
        // ⚠️ AppendResult and CommitLog both throw IllegalArgumentException for
        // their own invariant failures. Mapping IAE to 403 would tell the
        // producer its write was permanently refused, so it drops the batch and
        // the data is lost -- an implementation bug reported as a permissions
        // problem. Authorization is decided from the Principal instead.
        RecordingIngest ingest = new RecordingIngest();
        ingest.reject = new IllegalArgumentException("an internal invariant failed");
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n");

        assertThat(response.status().code())
                .as("an internal failure is a server error, never a 403 the producer will not retry")
                .isNotEqualTo(403)
                .isGreaterThanOrEqualTo(500);
    }

    @Test
    void aMissingPartitionIsFourHundredRatherThanADefaultOfZero() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        // ⚠️ Defaulting would silently funnel every producer that forgot the
        // parameter into partition 0 -- a correctness bug that looks like
        // success. M1 makes the partition EXPLICIT (aliases are ADR-0015, M6).
        var response = client.post("/logs/_bulk")
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n");

        assertThat(response.status().code()).isEqualTo(400);
        assertThat(ingest.records).isEmpty();
    }
}
