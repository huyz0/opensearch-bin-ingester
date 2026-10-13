// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;

/**
 * A fast batch's key, for a pushed entry and a proxied read before its upload
 * (ADR-0082 §6; M13.30j):
 * {@code <prefix>/fast/<epoch %016x>/<assignedAfter %08x>/<indexUuid>/<partition>/<firstOffset %016x>},
 * where {@code indexUuid} is the OpenSearch index UUID as
 * {@link RunKey#ofIndexUuid} reads it (22 base64url characters, unpadded) and
 * {@code partition} is decimal.
 *
 * <p>⚠️ IT NAMES NO OBJECT: {@code /seg} serves it from a pushed copy or the
 * leader's journal (M13.30f), and the {@code /fast/} segment can never equal a
 * {@link SegmentKey}'s {@code /data/}, so no cache keyed by segment key
 * confuses the two.
 */
public final class FastKey {

    /** The object-key bound every backend shares, as {@link SegmentKey}'s. */
    public static final int MAX_KEY_BYTES = 1024;

    private FastKey() {
    }

    /** A fast key's fields, as {@link #of} wrote them. */
    public record Parsed(String prefix, long epoch, long assignedAfter, RunKey key,
            long firstOffset) {
    }

    /**
     * {@code key}'s fields if it is a canonical fast key (M13.30l): the key
     * {@link #of} writes from them, and nothing else -- empty for any other
     * string, a data segment's key included.
     */
    public static java.util.Optional<Parsed> parse(String key) {
        if (key == null) {
            return java.util.Optional.empty();
        }
        String[] parts = key.split("/", -1);
        int n = parts.length;
        if (n < 7 || !"fast".equals(parts[n - 6]) || parts[n - 5].length() != 16
                || parts[n - 4].length() != 8 || parts[n - 1].length() != 16) {
            return java.util.Optional.empty();
        }
        try {
            String prefix = String.join("/", java.util.Arrays.copyOfRange(parts, 0, n - 6));
            long epoch = Long.parseUnsignedLong(parts[n - 5], 16);
            long assignedAfter = Long.parseLong(parts[n - 4], 16);
            RunKey run = RunKey.ofIndexUuid(parts[n - 3], Integer.parseInt(parts[n - 2]));
            long firstOffset = Long.parseUnsignedLong(parts[n - 1], 16);
            // ⚠️ CANONICAL ONLY: a key that does not round-trip names no
            // entry this pod could hold under it.
            return key.equals(of(prefix, epoch, assignedAfter, run, firstOffset))
                    ? java.util.Optional.of(new Parsed(prefix, epoch, assignedAfter, run,
                            firstOffset))
                    : java.util.Optional.empty();
        } catch (IllegalArgumentException notAFastKey) {
            return java.util.Optional.empty();
        }
    }

    /** The key of the entry of {@code key} at {@code firstOffset}, assigned in term {@code epoch}. */
    public static String of(String prefix, long epoch, long assignedAfter, RunKey key,
            long firstOffset) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(key, "key");
        if (epoch < 0 || firstOffset < 0) {
            throw new IllegalArgumentException("an epoch and an offset are never negative: "
                    + epoch + ", " + firstOffset);
        }
        if (assignedAfter < 0 || assignedAfter > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException("assignedAfter is a u32: " + assignedAfter);
        }
        byte[] uuid = ByteBuffer.allocate(16).putLong(key.indexId().getMostSignificantBits())
                .putLong(key.indexId().getLeastSignificantBits()).array();
        String fast = String.format(Locale.ROOT, "%s/fast/%016x/%08x/%s/%d/%016x", prefix, epoch,
                assignedAfter, Base64.getUrlEncoder().withoutPadding().encodeToString(uuid),
                key.partitionId(), firstOffset);
        if (fast.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("a fast key over " + MAX_KEY_BYTES
                    + " bytes: the prefix is too long");
        }
        return fast;
    }
}
