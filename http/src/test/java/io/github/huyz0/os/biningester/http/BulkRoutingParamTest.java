// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.ForwardingIngest;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * How a producer says where its records go (M6.6, FR-13, FR-19).
 *
 * <p>⚠️ EXACTLY ONE OF {@code partition} AND {@code routing}, AND NEITHER HAS A
 * DEFAULT. Defaulting the partition to 0 funnels every producer that forgot the
 * parameter into one shard while returning 202 — a data shape nobody can undo
 * afterwards — and accepting both makes a producer's stated intent depend on
 * which one the implementation happens to read first.
 *
 * <p>⚠️ AND AN UNREGISTERED INDEX IS NOT ONE OF THESE CASES. It is accepted and
 * waits (M6.5), because ADR-0015's Consequences require that "a producer that
 * starts before the plugin connects is not punished for a race it cannot see" —
 * a 400 there is a 400 storm on every plugin reconnect. {@link #aROUTINGValueReachesTheINGESTERUnaltered()} is
 * **the only case in the tree that would red against a `400 UNKNOWN_INDEX`
 * implementation**: the adapter holds no catalog, so "logs" is an index it
 * cannot resolve, and the pool's own cases are T0 over the pool object and
 * never cross the HTTP path.
 */
class BulkRoutingParamTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Records which seam method the adapter chose, and with what. */
    private static final class RecordingIngest extends ForwardingIngest {
        final List<Integer> partitions = new ArrayList<>();
        final List<String> routings = new ArrayList<>();
        final List<String> indices = new ArrayList<>();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws IOException {
            indices.add(index);
            partitions.add(partition);
            return drain(source);
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, RecordSource source) throws IOException {
            indices.add(indexOrAlias);
            routings.add(routing);
            return drain(source);
        }

        @Override
        public void close() {
        }

        private AppendResult drain(RecordSource source) throws IOException {
            int[] count = {0};
            source.forEachRecord(r -> count[0]++);
            return new AppendResult(count[0], 0L, count[0] - 1L);
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

    private static final String BODY = """
            {"index":{"_id":"doc-1","_version":7}}
            {"n":1}
            """;

    @Test
    void aROUTINGValueReachesTheINGESTERUnaltered() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("routing", "tenant-42")
                .submit(BODY);

        assertThat(response.status().code()).isEqualTo(202);
        assertThat(ingest.routings)
                .as("the adapter passes the value down and computes NOTHING -- which "
                        + "partition it means needs the registered shard count, and an "
                        + "adapter that resolved it here would be a second implementation of "
                        + "the one thing M6 exists to get exactly right (ADR-0019)")
                .containsExactly("tenant-42");
        assertThat(ingest.partitions)
                .as("and the explicit path is not taken")
                .isEmpty();
    }

    @Test
    void anEXPLICITPartitionStillReachesTheINGESTER() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("partition", "3").submit(BODY);

        assertThat(response.status().code()).isEqualTo(202);
        assertThat(ingest.partitions)
                .as("the mode a producer that knows its partition already used keeps working")
                .containsExactly(3);
        assertThat(ingest.routings).isEmpty();
    }

    @Test
    void NEITHERParameterIsFOURHUNDRED() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").submit(BODY);

        assertThat(response.status().code())
                .as("there is no default: partition 0 would funnel every producer that forgot "
                        + "the parameter into one shard while returning 202")
                .isEqualTo(400);
        assertThat(ingest.indices).as("and nothing was appended").isEmpty();
    }

    @Test
    void BOTHParametersAreFOURHUNDRED() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk")
                .queryParam("partition", "3")
                .queryParam("routing", "tenant-42")
                .submit(BODY);

        assertThat(response.status().code())
                .as("they are alternatives -- one says where the records go, the other asks "
                        + "the ingester to work it out -- and preferring one silently makes "
                        + "the producer's intent depend on the implementation's order")
                .isEqualTo(400);
        assertThat(ingest.indices).isEmpty();
    }

    @Test
    void anEMPTYRoutingValueIsFOURHUNDRED() {
        RecordingIngest ingest = new RecordingIngest();
        WebClient client = start(ingest);

        var response = client.post("/logs/_bulk").queryParam("routing", "").submit(BODY);

        assertThat(response.status().code())
                .as("an empty string hashes to a partition like any other, so accepting it "
                        + "sends every producer that built its query string wrong to one "
                        + "shard -- indistinguishable from a working deployment until that "
                        + "shard is hot")
                .isEqualTo(400);
    }

    /**
     * FR-13's refusal reaches the producer as 400, not 500 (M6.6 round 2).
     *
     * <p>⚠️ MEASURED BEFORE THIS CASE EXISTED: {@code RoutedIngest.append}
     * refused {@code ?partition=99} on a 3-shard index correctly, and the
     * producer got a 500 — which reads as transient, so it retries a batch that
     * can never be accepted, forever. The criterion says 400 and nothing at the
     * HTTP layer served it, because every other case here wires an ingester
     * that accepts anything.
     */
    @Test
    void aPARTITIONTheIndexDoesNotHaveIsFOURHUNDREDAndNotFIVEHUNDRED() {
        WebClient client = start(refusing(new io.github.huyz0.os.biningester.ingest.PlacementRefusedException(
                "partition 99 does not exist in logs-000001, which has 3 shards")));

        var response = client.post("/logs/_bulk").queryParam("partition", "99").submit(BODY);

        assertThat(response.status().code())
                .as("permanently the producer's: a 5xx here is retried forever for a batch "
                        + "that lands in a stream no shard polls")
                .isEqualTo(400);
        assertThat(response.as(String.class))
                .as("and the body says which partition and how many shards, because the "
                        + "producer's next question is what to send instead")
                .contains("3 shards");
    }

    /**
     * An index whose registration never arrived is 503, not 400 (M6.6 round 2).
     */
    @Test
    void anINDEXWhoseRegistrationNeverArrivedIsFIVEHUNDREDANDTHREE() {
        WebClient client = start(refusing(new io.github.huyz0.os.biningester.ingest.RegistrationTimeoutException(
                "index logs was still not registered after PT5S")));

        var response = client.post("/logs/_bulk").queryParam("routing", "tenant-42")
                .submit(BODY);

        assertThat(response.status().code())
                .as("retryable, and the producer should retry: the plugin's push ends this "
                        + "state on its own, and a 400 would make the producer drop records "
                        + "over a race ADR-0015 says it must not be punished for")
                .isEqualTo(503);
    }

    /** An ingester that refuses every write with one exception. */
    private static Ingest refusing(RuntimeException refusal) {
        return new ForwardingIngest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource source) {
                throw refusal;
            }

            @Override
            public AppendResult appendRouted(Principal principal, String indexOrAlias,
                    String routing, RecordSource source) {
                throw refusal;
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * A placement is exactly one of the two, by the TYPE (M6.6 round 3).
     *
     * <p>⚠️ {@code Placement(null, null)} was constructible and unboxed into an
     * NPE inside the append path — a 500 that reads to a producer as a server
     * fault worth retrying, for a request that named nothing.
     */
    @Test
    void aPLACEMENTOfNEITHEROrBOTHCannotBeBUILT() {
        assertThatThrownBy(() -> new BulkService.Placement(null, null))
                .as("neither is not a placement: the 400 above is the request-level rule, "
                        + "and this is the same rule where no caller can get past it")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BulkService.Placement(3, "tenant-42"))
                .as("and both is not a placement either")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
