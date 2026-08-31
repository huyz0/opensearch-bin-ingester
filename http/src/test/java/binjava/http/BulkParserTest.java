// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class BulkParserTest {

    private static InputStream body(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    private static List<SegmentRecord> parseAll(String s) throws IOException {
        List<SegmentRecord> out = new ArrayList<>();
        BulkParser.parse(body(s), out::add);
        return out;
    }

    @Test
    void anIndexActionCarriesIdVersionAndTheDocumentBytes() throws Exception {
        List<SegmentRecord> r = parseAll("""
                {"index":{"_id":"doc-1","_version":7}}
                {"n":1}
                """);
        assertThat(r).hasSize(1);
        assertThat(r.get(0).id()).isEqualTo("doc-1");
        assertThat(r.get(0).opType()).isEqualTo(OpType.INDEX);
        assertThat(r.get(0).version()).isEqualTo(OptionalLong.of(7));
        // ⚠️ Byte for byte. The ingester never parses a document (ADR-0020), so
        // the adapter must not normalise, re-serialise or pretty-print it.
        assertThat(new String(r.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo("{\"n\":1}");
    }

    @Test
    void aDeleteHasNoDocumentLine() throws Exception {
        List<SegmentRecord> r = parseAll("""
                {"delete":{"_id":"gone","_version":8}}
                {"index":{"_id":"after","_version":9}}
                {"n":2}
                """);
        // ⚠️ The delete consumes NO following line. Reading one would swallow the
        // next action and silently drop every remaining record in the batch.
        assertThat(r).hasSize(2);
        assertThat(r.get(0).opType()).isEqualTo(OpType.DELETE);
        assertThat(r.get(0).payload()).isEmpty();
        assertThat(r.get(1).id()).isEqualTo("after");
    }

    @Test
    void aCreateIsItsOwnOpTypeAndAnAbsentVersionStaysAbsent() throws Exception {
        List<SegmentRecord> r = parseAll("""
                {"create":{"_id":"fresh"}}
                {"n":3}
                """);
        assertThat(r.get(0).opType()).isEqualTo(OpType.CREATE);
        // ⚠️ Absent, not 0. A zero would be a real external version and would
        // lose to every later write under external versioning.
        assertThat(r.get(0).version()).isEmpty();
    }

    @Test
    void aTrailingNewlineIsOptionalAndABlankLineIsSkipped() throws Exception {
        // ⚠️ Assert the PAYLOAD, not just the count. This is the only input that
        // reaches LineReader's end-of-input partial-line return, and a count
        // assertion leaves an off-by-one there invisible: the last document
        // silently becomes `{"n":1` and is framed and stored corrupted.
        List<SegmentRecord> noNewline = parseAll("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}");
        assertThat(noNewline).hasSize(1);
        assertThat(new String(noNewline.get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":1}");

        List<SegmentRecord> blanks = parseAll("\n{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n\n");
        assertThat(blanks).hasSize(1);
        assertThat(new String(blanks.get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":1}");
    }

    /** ⚠️ CRLF was documented as load-bearing and constrained by nothing. */
    @Test
    void carriageReturnsAreStrippedFromBothActionAndDocumentLines() throws Exception {
        List<SegmentRecord> r = parseAll(
                "{\"index\":{\"_id\":\"a\",\"_version\":3}}\r\n{\"n\":1}\r\n");
        assertThat(r).hasSize(1);
        assertThat(r.get(0).version()).isEqualTo(OptionalLong.of(3));
        // ⚠️ Without the strip the action line ends `}\r` and Json refuses it as
        // trailing content -- a 400 for a request that is correct over CRLF.
        assertThat(new String(r.get(0).payload(), StandardCharsets.UTF_8))
                .as("a stray CR would be framed into the stored payload byte for byte")
                .isEqualTo("{\"n\":1}");
    }

    /**
     * ⚠️ The CR and the LF land in DIFFERENT reads, so trimCr must run on the
     * reassembled line rather than on a buffer slice. Sized to put the split
     * exactly on the 8192-byte internal boundary.
     */
    @Test
    void aCarriageReturnSplitAcrossTheBufferBoundaryIsStillStripped() throws Exception {
        String head = "{\"index\":{\"_id\":\"a\"}}\r\n{\"n\":\"";
        int padTo = 8192 - head.length() - 2; // leave "\"}" then CR at byte 8191
        String doc = head + "x".repeat(padTo) + "\"}";
        List<SegmentRecord> r = parseAll(doc + "\r\n");
        assertThat(r).hasSize(1);
        String payload = new String(r.get(0).payload(), StandardCharsets.UTF_8);
        assertThat(payload).doesNotContain("\r").endsWith("\"}");
    }

    /** ⚠️ A line longer than the 8192 buffer takes LineReader's slow path. */
    @Test
    void aDocumentLargerThanTheInternalBufferSurvivesByteForByte() throws Exception {
        String big = "{\"n\":\"" + "y".repeat(40_000) + "\"}";
        List<SegmentRecord> r = parseAll("{\"index\":{\"_id\":\"a\"}}\n" + big + "\n");
        assertThat(r).hasSize(1);
        assertThat(new String(r.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo(big);
    }

    @Test
    void anIndexActionWithNoDocumentLineIsRejected() {
        assertThatThrownBy(() -> parseAll("{\"index\":{\"_id\":\"a\"}}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("document line");
    }

    @Test
    void anUnknownActionIsRejectedRatherThanSkipped() {
        // ⚠️ Skipping would silently drop the producer's write while returning 202.
        assertThatThrownBy(() -> parseAll("{\"update\":{\"_id\":\"a\"}}\n{\"doc\":{}}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("update");
    }

    @Test
    void aNonIntegerVersionIsRejectedRatherThanTreatedAsAbsent() {
        // ⚠️ Treating it as absent would switch external versioning OFF behind a
        // 202 -- the same silent-drop class as skipping an unknown action, and
        // invisible until a stale replay overwrites a newer document.
        assertThatThrownBy(() -> parseAll("{\"index\":{\"_id\":\"a\",\"_version\":\"7\"}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("_version");
        assertThatThrownBy(() -> parseAll("{\"index\":{\"_id\":\"a\",\"_version\":1.5}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("_version");
    }

    @Test
    void anActionLineCarryingTwoActionsIsRejected() {
        // ⚠️ `outer.size() != 1` relaxed to `< 1` accepts this and silently drops
        // the delete behind a 202.
        assertThatThrownBy(() -> parseAll(
                "{\"index\":{\"_id\":\"a\"},\"delete\":{\"_id\":\"b\"}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("exactly one action key");
    }

    @Test
    void anEmptyDocumentLineIsRejectedRatherThanFramedAsAnEmptyPayload() {
        // ⚠️ Self-found, not review-found. An empty payload reaches the consumer
        // as `"_source":` followed by nothing -- malformed JSON at the OpenSearch
        // mapper, whose only symptom is "0 documents indexed". That is the exact
        // failure M1.15 spent a session diagnosing.
        assertThatThrownBy(() -> parseAll("{\"index\":{\"_id\":\"a\"}}\n\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("document line");
    }

    @Test
    void aLineWithoutANewlineIsCappedRatherThanBufferedUntilTheHeapIsGone() throws Exception {
        // ⚠️ The DoS guard: no volume required, just a missing newline. Feeds an
        // endless stream rather than allocating 64 MiB up front.
        InputStream endless = new InputStream() {
            @Override public int read() {
                return 'x';
            }

            @Override public int read(byte[] b, int off, int len) {
                java.util.Arrays.fill(b, off, off + len, (byte) 'x');
                return len;
            }
        };
        assertThatThrownBy(() -> BulkParser.parse(endless, r -> { }))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("exceeds");
    }

    @Test
    void anUnknownActionMetadataKeyIsRejectedRatherThanIgnored() {
        // ⚠️ `_index` is the STANDARD per-action form. Ignoring it writes the
        // record to the URL's index and returns 202 -- the producer's document
        // silently in the wrong index, which is worse than a 400.
        assertThatThrownBy(() -> parseAll(
                "{\"index\":{\"_index\":\"metrics\",\"_id\":\"a\"}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("_index");
        assertThatThrownBy(() -> parseAll(
                "{\"index\":{\"_id\":\"a\",\"routing\":\"r\"}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("routing");
    }

    @Test
    void anUntrustedKeyIsTruncatedBeforeItIsEchoedIntoTheMessage() {
        String huge = "k".repeat(5_000);
        assertThatThrownBy(() -> parseAll(
                "{\"index\":{\"" + huge + "\":1,\"_id\":\"a\"}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .satisfies(e -> assertThat(e.getMessage().length()).isLessThan(120));
    }

    @Test
    void anActionWithoutAnIdIsRejected() {
        assertThatThrownBy(() -> parseAll("{\"index\":{}}\n{\"n\":1}\n"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("_id");
    }

    /**
     * T6. The mutation this exists to kill is "replace the streaming read with
     * {@code readAllBytes}".
     */
    @Test
    void bulkBodyIsNeverFullyBuffered() throws Exception {
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
        BulkParser.parse(in, r -> {
            if (seen[0]++ == 0) {
                readWhenFirstEmitted[0] = in.count;
            }
        });

        assertThat(seen[0]).isEqualTo(records);
        // ⚠️ The load-bearing assertion: the FIRST record is emitted after only a
        // small prefix of a 4 MiB body has been consumed. readAllBytes -- or any
        // parse that collects records into a list before delivering them --
        // reads every byte first and fails here.
        assertThat(readWhenFirstEmitted[0])
                .as("bytes consumed before the first record was delivered")
                .isGreaterThan(0)
                // ⚠️ 4 buffers, not 64 KiB. The true value is one 8192-byte read;
                // a looser bound tolerates a parse that batches ~250 records
                // before draining, which is the very thing being refused.
                .isLessThan(4 * 8192);
    }

    /** Counts bytes actually pulled from the underlying stream. */
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
