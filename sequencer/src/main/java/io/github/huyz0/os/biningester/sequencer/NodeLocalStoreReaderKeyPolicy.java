// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.SegmentKey;
import java.util.Objects;

/** Canonical key and operation allowlist for automatic node-local recovery. */
public final class NodeLocalStoreReaderKeyPolicy {
    public enum Operation { GET, STAT }

    public enum KeyKind { CHECKPOINT_POINTER, COMMIT_DELTA, SEGMENT }

    private final String bucket;
    private final String prefix;

    public NodeLocalStoreReaderKeyPolicy(String bucket, String prefix) {
        this.bucket = nonBlank(bucket, "bucket");
        this.prefix = nonBlank(prefix, "prefix");
        if (prefix.endsWith("/")) {
            throw new IllegalArgumentException("prefix must not end with '/'");
        }
    }

    public KeyKind authorize(String bucket, String prefix, String key, Operation operation) {
        Objects.requireNonNull(operation, "operation");
        if (!this.bucket.equals(bucket) || !this.prefix.equals(prefix) || key == null) {
            throw refused();
        }
        KeyKind kind = logKey(key);
        if (kind == null && key.startsWith(this.prefix + "/data/")) {
            try {
                SegmentKey parsed = SegmentKey.parse(key);
                if (this.prefix.equals(parsed.prefix())) {
                    kind = KeyKind.SEGMENT;
                }
            } catch (IllegalArgumentException malformed) {
                throw refused();
            }
        }
        if (kind == null || operation == Operation.STAT && kind != KeyKind.CHECKPOINT_POINTER) {
            throw refused();
        }
        return kind;
    }

    private KeyKind logKey(String key) {
        String root = prefix + "/ctl/log/0/";
        if (!key.startsWith(root) || key.length() < root.length() + 17) {
            return null;
        }
        String epochText = key.substring(root.length(), root.length() + 16);
        long epoch;
        try {
            epoch = Long.parseUnsignedLong(epochText, 16);
        } catch (NumberFormatException malformed) {
            return null;
        }
        LogKeys log = new LogKeys(prefix, epoch);
        if (key.equals(log.latestCheckpointKey())) {
            return KeyKind.CHECKPOINT_POINTER;
        }
        return log.isEntryKey(key) ? KeyKind.COMMIT_DELTA : null;
    }

    private static String nonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is blank");
        }
        return value;
    }

    private static IllegalArgumentException refused() {
        return new IllegalArgumentException("automatic recovery key or namespace refused");
    }
}
