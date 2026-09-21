// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * Which stream a run belongs to: one index, one partition.
 *
 * <p>⚠️ Runs are sorted by {@code (indexId, partitionId)} so that ONE consumer's
 * runs are contiguous in the segment — its fetch is then one coalesced range
 * rather than hundreds of range GETs. That ordering is cost rules R4 and R5, not
 * a tidiness preference.
 */
public record RunKey(UUID indexId, int partitionId) implements Comparable<RunKey> {

    public RunKey {
        Objects.requireNonNull(indexId, "indexId");
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId is never negative: " + partitionId);
        }
    }

    /**
     * The stream an OpenSearch index uuid names (M7.2).
     *
     * <p>⚠️ AN OPENSEARCH INDEX UUID IS BASE64URL, NOT THE HYPHENATED FORM.
     * {@code UUID.fromString} throws on every real one — "Invalid UUID string:
     * nVzgup36TLqWp7VBBREj1w" — and no in-process test caught that, because
     * they all built a {@link UUID} directly. Only a booting node hands over a
     * real one.
     *
     * <p>⚠️ IT LIVES HERE SO THERE IS ONE RULE RATHER THAN TWO. The plugin
     * derives a stream from the index uuid to subscribe; the ingester derives
     * the same stream from the uuid a {@link ConsumerProgress} frame carries,
     * to key a watermark. Two implementations that disagreed would not throw —
     * they would produce two streams for one index, and the watermark for the
     * stream nobody reports on would never move while the one being reported on
     * belonged to nothing. That is a deletion, not an exception.
     */
    public static RunKey ofIndexUuid(String openSearchIndexUuid, int partitionId) {
        Objects.requireNonNull(openSearchIndexUuid, "openSearchIndexUuid");
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(openSearchIndexUuid);
        } catch (IllegalArgumentException notBase64) {
            throw new IllegalArgumentException("not an index uuid: " + openSearchIndexUuid,
                    notBase64);
        }
        if (raw.length != 16) {
            // ⚠️ EXACTLY 16, because a truncation maps two indices onto one
            // stream and the collision is silent on both sides.
            throw new IllegalArgumentException("an index uuid decodes to 16 bytes, not "
                    + raw.length + ": " + openSearchIndexUuid);
        }
        ByteBuffer b = ByteBuffer.wrap(raw);
        return new RunKey(new UUID(b.getLong(), b.getLong()), partitionId);
    }

    @Override
    public int compareTo(RunKey other) {
        int byIndex = indexId.compareTo(other.indexId);
        return byIndex != 0 ? byIndex : Integer.compare(partitionId, other.partitionId);
    }
}
