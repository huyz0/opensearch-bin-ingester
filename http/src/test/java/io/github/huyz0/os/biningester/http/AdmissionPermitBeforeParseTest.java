// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A later chunk's permit is taken BEFORE the chunk's records are parsed
 * (M11.7 review P2): a request waiting for its permit holds one parsed record,
 * not a chunk, so what the pod holds while saturated is bounded by permits.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdmissionPermitBeforeParseTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Buffers at once; after its FIRST chunk, takes the only permit for the test. */
    private static final class TakesThePermit implements Ingest {
        private final LaneAdmission admission;
        final AtomicInteger records = new AtomicInteger();
        volatile LaneAdmission.Permit elsewhere;

        TakesThePermit(LaneAdmission admission) {
            this.admission = admission;
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws java.io.IOException {
            return append(principal, index, partition, (byte) 0, source, () -> { });
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource source, Runnable buffered) throws java.io.IOException {
            source.forEachRecord(r -> records.incrementAndGet());
            buffered.run();
            if (elsewhere == null) {
                elsewhere = admission.tryAcquire((byte) 0).orElseThrow();
            }
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public void close() {
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
                byte lane, RecordSource records, Runnable buffered) throws IOException {
            try { // the removed default's behaviour: buffered once the append returns (M12.2)
                return appendRouted(principal, indexOrAlias, routing, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public String concreteIndex(String indexOrAlias) {
            return indexOrAlias; // no catalog in this double (M12.2)
        }
    }

    /** Records {@code from} to {@code to} as ONE chunk of a chunked request body, sent now. */
    private static void write(OutputStream out, int from, int to) throws java.io.IOException {
        StringBuilder records = new StringBuilder();
        for (int i = from; i < to; i++) {
            records.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
        }
        byte[] bytes = records.toString().getBytes(StandardCharsets.UTF_8);
        out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(bytes);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    @Test
    void aRequestWaitingForItsNextChunksPermitHasParsedOneRecordOfIt() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        TakesThePermit ingest = new TakesThePermit(admission);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL,
                        new DrainGate(), admission)))
                .build().start();
        int chunk = BulkService.APPEND_CHUNK_RECORDS;

        // ⚠️ A RAW SOCKET, so the body STALLS on the wire exactly where the
        // test says: a client library may buffer what it was given.
        String status;
        int parkedWith;
        try (Socket socket = new Socket("localhost", server.port())) {
            OutputStream out = socket.getOutputStream();
            out.write(("POST /logs/_bulk?partition=0 HTTP/1.1\r\nHost: localhost\r\n"
                    + "Content-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n"
                    + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            // One whole chunk and ONE record of the next, and then the body
            // stalls: only a parser that takes the permit on the chunk's first
            // record is parked in acquire now.
            write(out, 0, chunk + 1);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (admission.waiting() < 1 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            parkedWith = admission.waiting();
            assertThat(ingest.elsewhere).as("the premise: the first chunk was appended")
                    .isNotNull();
            ingest.elsewhere.release();
            write(out, chunk + 1, chunk + 500);
            out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            status = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                    StandardCharsets.US_ASCII)).readLine();
        }
        assertThat(status).startsWith("HTTP/1.1 202");

        assertThat(parkedWith)
                .as("⚠️ PARKED FOR THE PERMIT WITH ONE RECORD OF THE CHUNK PARSED, not waiting "
                        + "to parse the rest of the chunk before asking")
                .isEqualTo(1);
        assertThat(ingest.records).hasValue(chunk + 500);
        assertThat(admission.inFlight()).isZero();
    }
}
