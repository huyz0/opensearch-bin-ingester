// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import java.util.Objects;
import org.opensearch.index.Message;

/**
 * One record, as OpenSearch's ingestion engine consumes it.
 *
 * <p>WARNING: {@code Message<byte[]>} -- the payload is an OPAQUE byte array all
 * the way through. The ingester never parsed the document, the segment framed it
 * without reading it, and the consumer only concatenated an envelope around it.
 * Parsing here would put a JSON reader on the hot path of every record and would
 * reject documents the producer considers valid.
 *
 * <p>WARNING: the payload is already in the {@code DEFAULT} mapper's shape
 * (ADR-0020). MapperType is a closed enum, so a plugin cannot register its own;
 * whatever reaches {@code getPayload()} must already match one of the three
 * built-in shapes.
 */
public record BinStoreMessage(byte[] payload, Long timestamp) implements Message<byte[]> {

    public BinStoreMessage {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(timestamp, "timestamp");
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    /**
     * WARNING: NEVER NULL. OpenSearch reads this on every record and a null is
     * an unboxing hazard in the ingestion processor -- and under
     * {@code error_strategy: BLOCK} a single failure there stops the shard, which
     * presents as "zero documents indexed" with no other symptom.
     *
     * <p>It is the SEGMENT's creation time -- when the producer's oldest record
     * in it arrived -- never this node's clock: an ingestion-time stamp would
     * make a replay produce different documents than the original run.
     */
    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}
