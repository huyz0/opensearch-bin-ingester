// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * Parses an OpenSearch {@code _bulk} body as a STREAM.
 *
 * <p>⚠️ The whole point is that the body is never fully buffered (SPEC T6). A
 * bulk request is the largest thing a producer sends — criterion 8 ingests
 * 200 MB under a 256 MB heap — so records are handed to the sink as they are
 * read, and only one line is in memory at a time.
 *
 * <p>⚠️ The document line is copied as BYTES and never parsed. ADR-0020: the
 * ingester frames bytes it does not read. Only the small action lines go through
 * {@link Json}.
 */
public final class BulkParser {

    /**
     * ⚠️ A cap, because the body is untrusted. Without it a producer sending one
     * unterminated line makes the ingester buffer until the heap is gone — a
     * denial of service that needs no volume, just a missing newline.
     */
    static final int MAX_LINE_BYTES = 64 << 20;

    private BulkParser() {
    }

    /**
     * ⚠️ Truncates untrusted input before it is echoed into a 400 body. The key
     * is neither an {@code _id} nor a document, so quoting it is not a
     * security.md breach, but reflecting an arbitrary-length attacker string
     * back is still worth refusing.
     */
    private static String summarise(String untrusted) {
        String oneLine = untrusted.replaceAll("[^\\p{Print}]", "?");
        return oneLine.length() <= 40 ? oneLine : oneLine.substring(0, 40) + "...";
    }

    /** Reads {@code in} to the end, handing each record to {@code sink} as it is parsed. */
    public static void parse(InputStream in, Consumer<SegmentRecord> sink) throws IOException {
        LineReader lines = new LineReader(in);
        byte[] action;
        while ((action = lines.next()) != null) {
            if (action.length == 0) {
                continue;
            }
            emit(action, lines, sink);
        }
    }

    private static void emit(byte[] action, LineReader lines, Consumer<SegmentRecord> sink)
            throws IOException {
        Object parsed = Json.parse(new String(action, StandardCharsets.UTF_8));
        if (!(parsed instanceof Map<?, ?> outer) || outer.size() != 1) {
            throw new BulkParseException(
                    "a bulk action line is one JSON object with exactly one action key");
        }
        String verb = outer.keySet().iterator().next().toString();
        OpType opType = switch (verb) {
            case "index" -> OpType.INDEX;
            case "create" -> OpType.CREATE;
            case "delete" -> OpType.DELETE;
            // ⚠️ Rejected, not skipped. `update` needs a read-modify-write the
            // ingester cannot do without reading documents, and skipping it
            // would drop the producer's write behind a 202.
            default -> throw new BulkParseException(
                    "unsupported bulk action: " + summarise(verb));
        };
        if (!(outer.values().iterator().next() instanceof Map<?, ?> meta)) {
            throw new BulkParseException("the value of '" + verb + "' is a JSON object");
        }

        // ⚠️ An UNKNOWN metadata key is a 400, not a silent drop. `_index` is
        // the standard per-action form, so `{"index":{"_index":"metrics",...}}`
        // POSTed to /logs/_bulk would otherwise be written to `logs` and acked
        // 202 -- the producer's document silently in the wrong index. Same class
        // as the `update` action refused above; routing and _type likewise
        // change meaning and are refused rather than ignored.
        for (Object k : meta.keySet()) {
            if (!"_id".equals(k) && !"_version".equals(k)) {
                throw new BulkParseException(
                        "unsupported bulk action metadata key: " + summarise(String.valueOf(k)));
            }
        }

        Object rawId = meta.get("_id");
        if (!(rawId instanceof String id) || id.isEmpty()) {
            // ⚠️ M1 REQUIRES an _id. OpenSearch would auto-generate one, but an
            // auto-generated id is not idempotent: the same record replayed
            // after a crash becomes a second document. ADR-0001's dedup story
            // rests on the producer's id.
            throw new BulkParseException("every bulk action needs a non-empty _id");
        }

        Object rawVersion = meta.get("_version");
        OptionalLong version;
        if (rawVersion == null) {
            version = OptionalLong.empty();
        } else if (rawVersion instanceof Long v) {
            version = OptionalLong.of(v);
        } else {
            throw new BulkParseException("_version is a JSON integer");
        }

        byte[] payload;
        if (opType == OpType.DELETE) {
            // ⚠️ A delete consumes NO following line. Reading one would swallow
            // the next action and silently drop the rest of the batch.
            payload = new byte[0];
        } else {
            byte[] doc = lines.next();
            if (doc == null || doc.length == 0) {
                // ⚠️ An EMPTY document line is refused, not framed as an empty
                // payload. The consumer assembles `"_source":` + the payload
                // (ADR-0020), so an empty one produces malformed JSON whose only
                // symptom at the OpenSearch mapper is "0 documents indexed".
                throw new BulkParseException(
                        "a '" + verb + "' action must be followed by a non-empty document line");
            }
            payload = doc;
        }
        sink.accept(new SegmentRecord(id, opType, version, payload));
    }

    /** Reads newline-delimited lines without ever holding more than one. */
    private static final class LineReader {

        private final InputStream in;
        private final byte[] buf = new byte[8192];
        private int len;
        private int pos;
        private boolean eof;

        LineReader(InputStream in) {
            this.in = in;
        }

        /** The next line without its newline, or null at end of input. */
        byte[] next() throws IOException {
            ByteArrayOutputStream line = null;
            while (true) {
                if (pos == len) {
                    if (eof) {
                        break;
                    }
                    len = in.read(buf);
                    pos = 0;
                    if (len < 0) {
                        len = 0;
                        eof = true;
                        break;
                    }
                }
                int nl = -1;
                for (int k = pos; k < len; k++) {
                    if (buf[k] == '\n') {
                        nl = k;
                        break;
                    }
                }
                int end = nl < 0 ? len : nl;
                if (line == null && nl >= 0) {
                    byte[] out = trimCr(buf, pos, end);
                    pos = nl + 1;
                    return out;
                }
                if (line == null) {
                    line = new ByteArrayOutputStream();
                }
                line.write(buf, pos, end - pos);
                if (line.size() > MAX_LINE_BYTES) {
                    throw new BulkParseException("a bulk line exceeds " + MAX_LINE_BYTES + " bytes");
                }
                pos = nl < 0 ? len : nl + 1;
                if (nl >= 0) {
                    byte[] raw = line.toByteArray();
                    return trimCr(raw, 0, raw.length);
                }
            }
            if (line == null) {
                return null;
            }
            byte[] raw = line.toByteArray();
            return trimCr(raw, 0, raw.length);
        }

        /** ⚠️ CRLF: a producer on Windows, or any HTTP client that normalises. */
        private static byte[] trimCr(byte[] src, int from, int to) {
            int end = to;
            if (end > from && src[end - 1] == '\r') {
                end--;
            }
            byte[] out = new byte[end - from];
            System.arraycopy(src, from, out, 0, end - from);
            return out;
        }
    }
}
