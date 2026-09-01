// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.ingest.AppendResult;
import binjava.ingest.Ingest;
import binjava.security.Principal;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M1.7b: {@code BulkService} streams records straight into {@link Ingest},
 * never building a {@code List} of the whole request first.
 *
 * <p>Drives {@link BulkService#appendBulkBody} directly, without the real HTTP
 * stack — the same reason {@code BulkParserTest} tests {@code BulkParser}
 * against a raw {@code InputStream} rather than a running server: it isolates
 * the claim from whatever Helidon itself does with a request body.
 */
class BulkServiceTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    /**
     * T6/M1.7b. The mutation this exists to kill: collect {@code sink} into a
     * {@code List} inside {@code appendBulkBody} before handing it to
     * {@link Ingest#append} (i.e. re-materialise behind a shape that still
     * type-checks as a {@code RecordSource}) — that mutation makes the first
     * record arrive only after {@code BulkParser} has already read the WHOLE
     * body, and this test's load-bearing assertion catches exactly that.
     */
    @Test
    void bulkServiceNeverBuffersTheWholeBodyBeforeAppending() throws Exception {
        int records = 20_000;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < records; i++) {
            sb.append("{\"index\":{\"_id\":\"doc-").append(i).append("\"}}\n")
                    .append("{\"n\":").append(i).append(",\"pad\":\"")
                    .append("x".repeat(200)).append("\"}\n");
        }
        byte[] raw = sb.toString().getBytes(StandardCharsets.UTF_8);
        assertThat(raw.length).as("a body far larger than any sane buffer").isGreaterThan(4 << 20);

        CountingStream in = new CountingStream(new ByteArrayInputStream(raw));
        long[] readWhenFirstEmitted = {-1};
        int[] seen = {0};
        Ingest fake = new Ingest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource source) throws IOException {
                source.forEachRecord(r -> {
                    if (seen[0]++ == 0) {
                        readWhenFirstEmitted[0] = in.count;
                    }
                });
                return new AppendResult(seen[0], 0L, seen[0] - 1L);
            }

            @Override
            public void close() {
            }
        };

        new BulkService(fake, PRINCIPAL).appendBulkBody(in, "logs", 0);

        assertThat(seen[0]).isEqualTo(records);
        // ⚠️ The load-bearing assertion: the FIRST record reaches Ingest.append
        // after only a small prefix of a 4+ MiB body has been consumed. A
        // handler that collects into a List first — or any handler that reads
        // every byte before appending any of it — reads the whole body first
        // and fails here.
        assertThat(readWhenFirstEmitted[0])
                .as("bytes consumed before the first record reached Ingest.append")
                .isGreaterThan(0)
                .isLessThan(raw.length / 2);
    }

    /** Reads bytes without ever holding more than one buffer's worth (BulkParserTest). */
    private static final class CountingStream extends FilterInputStream {
        volatile long count;

        CountingStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b >= 0) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }
    }
}
