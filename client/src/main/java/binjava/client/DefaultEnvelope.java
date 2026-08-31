// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Wraps a record into the JSON shape OpenSearch's {@code DEFAULT} mapper expects
 * (ADR-0020).
 *
 * <p>WARNING: ASSEMBLED AT READ TIME, and it is a BYTE CONCATENATION around an
 * opaque payload -- never a parse of the document. The ingester never looked
 * inside the body and neither does this: a parse here would put a JSON reader on
 * the hot path of every record, and would reject documents the producer
 * considers valid.
 *
 * <p>Why DEFAULT and not the alternatives: {@code RAW_PAYLOAD} auto-generates
 * {@code _id} and forces {@code index}, so it cannot express a delete;
 * {@code FIELD_MAPPING} lifts the fields from INSIDE the document, and the
 * producer sends them in the bulk action line, not the body. MapperType is a
 * closed enum, so those were the only three options.
 */
public final class DefaultEnvelope {

    private DefaultEnvelope() {}

    /** The bytes {@code Message.getPayload()} must return. */
    public static byte[] assemble(SegmentRecord record) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, "{\"_id\":");
        writeJsonString(out, record.id());
        write(out, ",\"_op_type\":\"");
        write(out, opTypeName(record.opType()));
        write(out, "\"");
        if (record.version().isPresent()) {
            // WARNING: OMITTED when absent, not written as 0. Zero is a version
            // OpenSearch compares against, so "the producer sent none" and "the
            // producer sent 0" must not become the same document.
            // WARNING: a JSON STRING, not a number. OpenSearch's DEFAULT
            // mapper casts this field to String, so a numeric literal fails the
            // whole batch with
            // "ClassCastException: Integer cannot be cast to String" -- which
            // surfaces only as "0 documents indexed".
            write(out, ",\"_version\":\"");
            write(out, Long.toString(record.version().getAsLong()));
            write(out, "\"");
        }
        if (record.opType() != OpType.DELETE) {
            // WARNING: a delete carries no _source. Emitting an empty object
            // would make it an index of {} -- the silent resurrection that
            // acceptance criterion 0 exists to refuse.
            write(out, ",\"_source\":");
            out.writeBytes(record.payload());
        }
        write(out, "}");
        return out.toByteArray();
    }

    private static String opTypeName(OpType op) {
        // WARNING: the wire names OpenSearch uses, not the enum's. The enum is
        // uppercase; renaming a constant here would silently change the action
        // of every document.
        return switch (op) {
            case INDEX -> "index";
            case CREATE -> "create";
            case DELETE -> "delete";
        };
    }

    private static void write(ByteArrayOutputStream out, String s) {
        out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /** WARNING: the id is producer-supplied, so it is escaped rather than trusted. */
    private static void writeJsonString(ByteArrayOutputStream out, String value) {
        StringBuilder b = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        write(out, b.append('"').toString());
    }
}
