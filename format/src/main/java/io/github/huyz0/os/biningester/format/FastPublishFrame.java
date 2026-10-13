// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Publication's frames (ADR-0082 §2 kinds 7 and 8; ADR-0081 §6; M13.30c),
 * landing before anything sends them, so a reader ships first:
 *
 * <ul>
 *   <li>PUBLISH, leader to an interested pod: exposed entries, of one or
 *       more batches -- a frame per exposure step per pod, never per stream;
 *       laid out as REPLICA's body is: {@code u32} count, each entry its
 *       journal encoding behind a {@code u32} length;
 *   <li>INTEREST, pod to leader: the streams it adds and removes --
 *       {@code u32} count and run keys added, then {@code u32} count and run
 *       keys removed.
 * </ul>
 *
 * <p>⚠️ BOTH ARE DATA FRAMES, outside ADR-0081 §12's control list: a PUBLISH
 * carries records, and an INTEREST is sent per subscriber change and leader
 * change, a few bytes each.
 */
public final class FastPublishFrame {

    public static final int KIND_PUBLISH = 7;
    public static final int KIND_INTEREST = 8;

    private static final int RUN_KEY_BYTES = 20;

    private FastPublishFrame() {
    }

    /** PUBLISH: exposed entries the addressed pod is interested in. */
    public record Publish(List<FastJournalRecord.Entry> entries) implements FastFrame.Body {
        public Publish {
            entries = List.copyOf(entries);
            if (entries.isEmpty()) {
                throw new IllegalArgumentException("a PUBLISH carries at least one entry");
            }
        }

        @Override
        public int kind() {
            return KIND_PUBLISH;
        }
    }

    /** INTEREST: the streams a pod adds to its interest, and removes from it. */
    public record Interest(List<RunKey> added, List<RunKey> removed) implements FastFrame.Body {
        public Interest {
            added = List.copyOf(added);
            removed = List.copyOf(removed);
            if (added.isEmpty() && removed.isEmpty()) {
                throw new IllegalArgumentException("an INTEREST changes something");
            }
            Set<RunKey> seen = new HashSet<>();
            for (RunKey k : added) {
                if (!seen.add(k)) {
                    throw new IllegalArgumentException("a stream added twice: " + k);
                }
            }
            for (RunKey k : removed) {
                if (!seen.add(k)) {
                    throw new IllegalArgumentException("a stream named twice: " + k);
                }
            }
        }

        @Override
        public int kind() {
            return KIND_INTEREST;
        }
    }

    static void encode(ByteArrayOutputStream out, FastFrame.Body body) {
        switch (body) {
            case Publish p -> {
                FastFrame.u32(out, p.entries().size());
                for (FastJournalRecord.Entry e : p.entries()) {
                    byte[] bytes = e.encode();
                    FastFrame.u32(out, bytes.length);
                    out.writeBytes(bytes);
                }
            }
            case Interest i -> {
                FastFrame.u32(out, i.added().size());
                i.added().forEach(k -> FastFrame.runKey(out, k));
                FastFrame.u32(out, i.removed().size());
                i.removed().forEach(k -> FastFrame.runKey(out, k));
            }
            default -> throw new IllegalArgumentException("not a publication frame: "
                    + body.kind());
        }
    }

    static FastFrame.Body decode(int kind, Cursor c) throws IOException {
        return switch (kind) {
            case KIND_PUBLISH -> {
                long n = FastFrame.count(c, 4 + FastJournalRecord.HEADER_BYTES);
                List<FastJournalRecord.Entry> entries = new ArrayList<>();
                for (long i = 0; i < n; i++) {
                    long length = FastFrame.u32(c);
                    if (length > c.remaining()) {
                        throw new IOException("a PUBLISH entry overruns the frame");
                    }
                    entries.add(FastWriteFrame.entry(c.bytes((int) length)));
                }
                yield new Publish(entries);
            }
            case KIND_INTEREST -> new Interest(keys(c), keys(c));
            default -> throw new IOException("fast frame kind " + kind
                    + " is not a publication frame");
        };
    }

    private static List<RunKey> keys(Cursor c) throws IOException {
        long n = FastFrame.count(c, RUN_KEY_BYTES);
        List<RunKey> keys = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            keys.add(FastFrame.runKey(c));
        }
        return keys;
    }
}
