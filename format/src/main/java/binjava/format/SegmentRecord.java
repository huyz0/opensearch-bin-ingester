// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * One mutation, as the segment frames it (ADR-0020).
 *
 * <p>⚠️ Named {@code SegmentRecord}, not {@code Record}: the latter collides with
 * {@code java.lang.Record} under a wildcard import, so every module that imports
 * this package — ingest, client, plugin — would have had to disambiguate or
 * silently pick the wrong one.
 *
 * <p>⚠️ The envelope carries {@code _id}, {@code _op_type} and {@code _version}
 * NATIVELY rather than inside the payload. Producers send a monotonic external
 * version in the bulk ACTION line, not in the document, so a mapper that lifts
 * those fields from the body cannot see them — which is why `FIELD_MAPPING` was
 * disqualified and the consumer assembles the `DEFAULT` envelope at read time.
 *
 * <p>⚠️ The payload is OPAQUE. The ingester never parses a document; it frames
 * bytes it does not read.
 *
 * @param id the document id, never empty
 * @param opType index, create or delete
 * @param version the source's external version, or empty when it sent none
 * @param payload the document bytes; empty for a delete
 */
public record SegmentRecord(String id, OpType opType, OptionalLong version, byte[] payload) {

    public SegmentRecord {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(opType, "opType");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(payload, "payload");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("a record id is never empty");
        }
        if (opType == OpType.DELETE && payload.length != 0) {
            // ⚠️ A delete with a body would be silently indexed by a reader that
            // trusts the payload length instead of the op type.
            throw new IllegalArgumentException("a delete carries no payload");
        }
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
